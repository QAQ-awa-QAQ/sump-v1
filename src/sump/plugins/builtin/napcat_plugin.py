"""NapCat (QQ) 适配插件：正向 WebSocket 接入 OneBot 11。

NapCat 作为 WS 服务端，本插件作为客户端连接其正向 WS 端口：
- 收到 OneBot `message` 事件 → 驱动 Agent 处理
- 订阅 `agent.reply` 钩子 → 把回复发回 QQ

依赖 `websockets`（uvicorn[standard] 已附带）。
"""

import asyncio
import base64
import json
import logging
import re
from pathlib import Path
from typing import Any

from sump.agent import Agent
from sump.config import Config
from sump.event import AgentEvents, get_event_bus
from sump.tools.builtin.qq_image import SendQQImageTool

logger = logging.getLogger("sump.napcat")

# 单图上限（DeepSeek 文档：base64 图片最大 32 MiB）
_MAX_IMAGE_BYTES = 32 * 1024 * 1024


class NapCatPlugin:
    """NapCat QQ 机器人适配插件。"""

    def __init__(self, config: Config | None = None) -> None:
        self._config = config or Config()
        self._enabled = bool(self._config.get("napcat.enabled", False))
        self._ws_url = str(self._config.get("napcat.ws_url", "ws://127.0.0.1:3001"))
        self._access_token = str(self._config.get("napcat.access_token", ""))
        self._owner_id = str(self._config.get("napcat.owner_id", "") or "").strip()
        self._name = str(self._config.get("napcat.name", "星宝") or "星宝")
        self._bus = get_event_bus()
        self._agents: dict[str, Agent] = {}
        self._pending_approval: dict[str, list[dict[str, str]]] = {}  # session_id -> 挂起队列（FIFO）
        self._pending_source: dict[str, str] = {}  # session_id -> 审批来源标注
        self._locks: dict[str, asyncio.Lock] = {}  # session_id -> 处理锁（防并发）
        self._ws: Any = None
        self._task: asyncio.Task[None] | None = None
        # 文件接收（QQ 文件消息 → 下载保存 + 文本预览）
        self._file_dir = str(self._config.get("napcat.file_dir", "data/napcat_files"))
        self._file_max_bytes = max(1, int(self._config.get("napcat.file_max_mb", 50))) * 1024 * 1024
        # OneBot action 请求-响应（echo → future）
        self._pending_actions: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._action_seq = 0
        # 消息处理后台任务锁：保证消息串行处理，同时不阻塞 action 响应分发
        self._message_lock = asyncio.Lock()
        # 钩子：Agent 回复完成 → 发回 QQ；审批挂起/超时 → 推送主人
        self._bus.on(AgentEvents.REPLY, self._on_reply, consumer="napcat")
        self._bus.on(AgentEvents.APPROVAL_PENDING, self._on_approval_pending, consumer="napcat")
        self._bus.on(AgentEvents.APPROVAL_EXPIRED, self._on_approval_expired, consumer="napcat")

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动后台连接循环（幂等）。"""
        if not self._enabled:
            logger.info("napcat 插件未启用（napcat.enabled=false）")
            return
        try:
            import websockets  # noqa: F401
        except ImportError:
            logger.error("缺少 websockets 依赖，请执行：pip install websockets")
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        """停止连接循环。"""
        if self._task is not None:
            self._task.cancel()
            self._task = None
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # 连接循环
    # ------------------------------------------------------------------

    async def _run_forever(self) -> None:
        """连接循环：正常断开 1 秒后重连；异常时指数退避（上限 30 秒），防止紧密重连风暴。"""
        backoff = 1.0
        while True:
            try:
                await self._connect_and_serve()
                wait = 1.0
                backoff = 1.0  # 对端正常关闭：固定短延迟并重置退避
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                wait = backoff
                logger.error("napcat 连接异常，%.0f 秒后重连：%s", wait, exc)
                backoff = min(backoff * 2, 30.0)
            await asyncio.sleep(wait)

    async def _connect_and_serve(self) -> None:
        import websockets

        url = self._ws_url
        if self._access_token:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}access_token={self._access_token}"
        async with websockets.connect(url) as ws:
            self._ws = ws
            logger.info("napcat 已连接：%s", self._ws_url)
            async for raw in ws:
                await self._handle_raw(str(raw))
        self._ws = None

    # ------------------------------------------------------------------
    # 事件处理
    # ------------------------------------------------------------------

    async def _handle_raw(self, raw: str) -> None:
        """处理一条 WS 原始帧（先匹配 action 响应，再处理消息事件）。"""
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        echo = str(data.get("echo", "") or "")
        if echo and echo in self._pending_actions:
            fut = self._pending_actions.pop(echo)
            if not fut.done():
                fut.set_result(data)
            return
        if data.get("post_type") == "message":
            # 后台处理：长耗时消息（如文件下载 + action 往返）不能阻塞 WS 接收循环，
            # 否则 action 响应无法分发 → 自等死锁
            asyncio.create_task(self._handle_message_safe(data))

    async def _handle_message_safe(self, data: dict[str, Any]) -> None:
        """后台处理一条消息事件（锁保证串行，异常只记日志）。"""
        async with self._message_lock:
            try:
                await self._handle_message(data)
            except Exception as exc:  # noqa: BLE001
                logger.error("消息处理失败：%s", exc)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        """把 OneBot 消息事件转成 Agent 会话并驱动回复。"""
        message_type = data.get("message_type", "private")
        user_id = str(data.get("user_id", ""))
        text = self._extract_text(data.get("message"))
        images: list[str] = []
        for u in self._extract_image_urls(data.get("message")):
            data_url = await self._download_image_data_url(u)
            if data_url:
                images.append(data_url)
            else:
                # 下载失败降级：文本占位，模型仍知道这条消息带过图片
                text = f"{text}\n[图片]" if text else "[图片]"
        # 文件消息：下载保存（PDF 等自动附文本预览），以说明文本注入上下文
        for info in self._extract_files(data.get("message")):
            note = await self._save_incoming_file(info, data)
            if note:
                text = f"{text}\n{note}" if text else note
        if (not text and not images) or not user_id:
            return

        if message_type == "group":
            group_id = str(data.get("group_id", ""))
            session_id = f"group_{group_id}"
            reply_ctx: dict[str, Any] = {"message_type": "group", "group_id": group_id}
        else:
            session_id = f"private_{user_id}"
            reply_ctx = {"message_type": "private", "user_id": user_id}

        # 私聊零信任：非主人私聊一律拒绝
        if message_type == "private" and not self._is_owner(user_id):
            logger.warning("拒绝非主人私聊：user_id=%s", user_id)
            await self._send(reply_ctx, "抱歉，你不是授权用户，已拒绝执行。")
            return

        # 审批响应（仅主人私聊）：1=同意 / 2=拒绝，按挂起顺序 FIFO（同会话可能排队多个）
        if (
            text in ("1", "2")
            and message_type == "private"
            and self._is_owner(user_id)
            and self._pending_approval
        ):
            sid = next(iter(self._pending_approval))
            queue = self._pending_approval[sid]
            pending = queue.pop(0)
            if not queue:
                self._pending_approval.pop(sid, None)
            async with self._get_lock(sid):
                await self._get_agent(sid).approve_and_continue(
                    pending["call_id"], text == "1"
                )
            return

        if message_type == "group":
            await self._handle_group_message(data, session_id, text, user_id, images)
        else:
            await self._handle_private_message(session_id, text, images)

    async def _handle_private_message(
        self, session_id: str, text: str, images: list[str]
    ) -> None:
        """私聊：记录并直接回复（1v1，逐条回应）。"""
        async with self._get_lock(session_id):
            self._pending_source[session_id] = "私聊"
            await self._bus.emit(
                AgentEvents.MESSAGE_RECEIVED, session_id=session_id, content=text, source="napcat"
            )
            agent = self._get_agent(session_id)
            try:
                async for _ in agent.run_stream(text, images=images):
                    pass  # 回复通过 agent.reply 钩子发回
            except Exception as exc:  # noqa: BLE001
                logger.error("Agent 处理失败：%s", exc)

    async def _handle_group_message(
        self, data: dict[str, Any], session_id: str, text: str, user_id: str, images: list[str]
    ) -> None:
        """群聊：记录所有消息，智能体自主决定是否说话（@ 必回，星宝加权）。"""
        async with self._get_lock(session_id):
            agent = self._get_agent(session_id)
            nickname = str((data.get("sender") or {}).get("nickname", user_id))
            group_id = str(data.get("group_id") or session_id[len("group_"):])
            self._pending_source[session_id] = f"群聊 {group_id} · {nickname}"

            # 记录会话：主人消息带标记（供记忆提炼只针对主人）
            owner_marker = str(self._config.get("memory.owner_marker", "·主人"))
            if self._is_owner(user_id):
                label = f"[{nickname}{owner_marker}]"
            else:
                label = f"[{nickname}]"
            agent.ctx.add_user_message(f"{label} {text}", images=images)

            # 2. 决定是否说话
            at_me = self._is_at_me(data)
            mentioned = self._name in text
            if not await self._should_speak(agent, text, mentioned, at_me):
                return

            # 3. 回复（基于完整群聊上下文）
            try:
                async for _ in agent.run_core():
                    pass  # 回复通过 agent.reply 钩子发回
            except Exception as exc:  # noqa: BLE001
                logger.error("Agent 处理失败：%s", exc)

    async def _should_speak(
        self, agent: Any, text: str, mentioned: bool, at_me: bool
    ) -> bool:
        """决定是否插话：@ 必回；星宝加权；其余交给 flash 判断。"""
        if at_me:
            return True
        prompt = (
            f"你是 QQ 群里的智能体「{self._name}」。群里刚有人发了一条消息，"
            "你需要决定是否回复。\n"
            f"消息：{text}\n"
            + (f"注意：这条消息提到了你的名字「{self._name}」，倾向于回复。\n" if mentioned else "")
            + "只有当消息在向你提问、求助、明确提到你、或值得你插话时才回复；"
            "普通闲聊、无指向性的群聊不要插话。只回答 yes 或 no。"
        )
        try:
            result = await agent.llm.chat_flash(prompt, max_tokens=8, temperature=0.3)
            return "yes" in result.lower()
        except Exception:  # noqa: BLE001
            return mentioned  # flash 失败：提到名字才回

    def _is_at_me(self, data: dict[str, Any]) -> bool:
        """判断消息是否 @ 了本机器人（at 段的 qq 等于 self_id）。"""
        self_id = str(data.get("self_id", ""))
        message = data.get("message")
        if not self_id or not isinstance(message, list):
            return False
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "at":
                if str(seg.get("data", {}).get("qq", "")) == self_id:
                    return True
        return False

    async def _on_reply(self, session_id: str, content: str, **kwargs: Any) -> None:
        """钩子：Agent 回复完成 → 发回 QQ。"""
        if not content:
            return
        ctx = self._reply_ctx(session_id)
        if ctx is None:
            return
        await self._send(ctx, content)

    async def _on_approval_pending(
        self,
        session_id: str,
        call_id: str,
        command: str,
        summary: str,
        danger: str,
        **kwargs: Any,
    ) -> None:
        """钩子：审批挂起 → 推送主人私聊（标注来源群聊 + 发起人）。"""
        source = self._pending_source.get(session_id, "未知")
        self._pending_approval.setdefault(session_id, []).append(
            {"call_id": call_id, "source": source}
        )
        ctx = self._owner_ctx()
        if ctx is None:
            logger.warning("未配置主人 QQ 号（napcat.owner_id），审批无法推送")
            return
        msg = (
            "⚠️ 待审批命令\n"
            f"来源：{source}\n"
            f"命令：{command}\n"
            f"意图：{summary or '未知'}\n"
            f"危险等级：{danger or '未知'}\n"
            "回复 1 同意，2 拒绝"
        )
        await self._send(ctx, msg)

    async def _on_approval_expired(
        self, session_id: str, call_id: str, **kwargs: Any
    ) -> None:
        """钩子：审批超时 → 通知主人私聊并继续执行。"""
        queue = self._pending_approval.get(session_id)
        if queue is not None:
            queue[:] = [p for p in queue if p["call_id"] != call_id]
            if not queue:
                self._pending_approval.pop(session_id, None)
        ctx = self._owner_ctx()
        if ctx is not None:
            await self._send(ctx, "审批超时，已自动拒绝。")
        agent = self._get_agent(session_id)
        try:
            async with self._get_lock(session_id):
                async for _ in agent.run_core():
                    pass  # 回复通过 agent.reply 钩子发回
        except Exception as exc:  # noqa: BLE001
            logger.error("审批超时后继续执行失败：%s", exc)

    def _owner_ctx(self) -> dict[str, Any] | None:
        """主人私聊回复上下文（审批推送目标）。"""
        if not self._owner_id:
            return None
        return {"message_type": "private", "user_id": self._owner_id}

    def _reply_ctx(self, session_id: str) -> dict[str, Any] | None:
        """把 session_id 转成 OneBot 回复上下文（群/私聊）。"""
        if session_id.startswith("group_"):
            return {"message_type": "group", "group_id": session_id[len("group_"):]}
        if session_id.startswith("private_"):
            return {"message_type": "private", "user_id": session_id[len("private_"):]}
        return None

    async def _send(self, ctx: dict[str, Any], content: str) -> None:
        """通过 OneBot `send_msg` 动作发送消息。"""
        if self._ws is None:
            logger.warning("napcat 未连接，丢弃回复")
            return
        try:
            await self._ws.send(json.dumps(
                {"action": "send_msg", "params": {**ctx, "message": content}},
                ensure_ascii=False,
            ))
        except Exception as exc:  # noqa: BLE001
            logger.error("发送 QQ 消息失败：%s", exc)

    async def send_image_to(self, session_id: str, image_path: str, text: str = "") -> str | None:
        """向指定会话发送本地图片（供 send_qq_image 工具调用）；失败返回错误信息。"""
        ctx = self._reply_ctx(session_id)
        if ctx is None:
            return f"未知会话：{session_id}"
        if self._ws is None:
            return "QQ 未连接"
        segments: list[dict[str, Any]] = [
            {"type": "image", "data": {"file": Path(image_path).resolve().as_uri()}}
        ]
        if text:
            segments.append({"type": "text", "data": {"text": text}})
        try:
            await self._ws.send(json.dumps(
                {"action": "send_msg", "params": {**ctx, "message": segments}},
                ensure_ascii=False,
            ))
        except Exception as exc:  # noqa: BLE001
            logger.error("发送 QQ 图片失败：%s", exc)
            return str(exc)
        return None

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------

    def _get_agent(self, session_id: str) -> Agent:
        """每个 QQ 群/私聊一个独立 Agent 会话，避免记忆串扰。"""
        if session_id not in self._agents:
            agent = Agent(self._config)
            agent.switch_session(session_id)
            # QQ 专属能力：发送图片（表情包等）到当前会话
            agent.tools.register(SendQQImageTool(self, session_id))
            self._agents[session_id] = agent
        return self._agents[session_id]

    def apply_settings(self, settings: dict[str, Any]) -> None:
        """设置中心变更后：热更新本插件已创建的 Agent 实例。"""
        for agent in self._agents.values():
            agent.apply_settings(settings)

    def _get_lock(self, session_id: str) -> asyncio.Lock:
        """每个会话一把处理锁，串行化该会话的消息处理。"""
        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    def _is_owner(self, user_id: str) -> bool:
        """零信任：仅主人 QQ 号可执行；未配置主人则拒绝所有人。"""
        return bool(self._owner_id) and user_id == self._owner_id

    @staticmethod
    def _extract_text(message: Any) -> str:
        """从 OneBot message 段提取纯文本。"""
        if isinstance(message, str):
            return message.strip()
        if isinstance(message, list):
            parts: list[str] = []
            for seg in message:
                if isinstance(seg, dict) and seg.get("type") == "text":
                    parts.append(str(seg.get("data", {}).get("text", "")))
            return "".join(parts).strip()
        return ""

    @staticmethod
    def _extract_image_urls(message: Any) -> list[str]:
        """从 OneBot message 段提取图片 URL。"""
        if not isinstance(message, list):
            return []
        urls: list[str] = []
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "image":
                url = str(seg.get("data", {}).get("url", ""))
                if url:
                    urls.append(url)
        return urls

    async def _download_image_data_url(self, url: str) -> str | None:
        """下载 QQ 图片并转 base64 data URL（内网 URL 云端不可达，直发模型）；失败返回 None。"""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.content
        except Exception as exc:  # noqa: BLE001
            logger.warning("下载 QQ 图片失败：%s %s", url, exc)
            return None

        if len(data) > _MAX_IMAGE_BYTES:
            logger.warning("QQ 图片超过 32MiB 上限，跳过：%s", url)
            return None
        b64 = base64.b64encode(data).decode("utf-8")
        return f"data:{_guess_image_mime(data)};base64,{b64}"

    # ------------------------------------------------------------------
    # 文件消息（接收 / 保存 / 文本预览）
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_files(message: Any) -> list[dict[str, str]]:
        """从 OneBot message 段提取文件信息（file_id / 文件名 / url / 大小）。"""
        files: list[dict[str, str]] = []
        if isinstance(message, list):
            for seg in message:
                if isinstance(seg, dict) and seg.get("type") == "file":
                    info = seg.get("data") or {}
                    files.append({
                        "file_id": str(info.get("file_id", "") or info.get("file", "")),
                        "name": str(info.get("file", "") or info.get("name", "")),
                        "url": str(info.get("url", "") or ""),
                        "size": str(info.get("file_size", "") or ""),
                    })
        return files

    async def _save_incoming_file(self, info: dict[str, str], envelope: dict[str, Any]) -> str | None:
        """下载并保存用户发来的文件；返回注入上下文的说明文本（含文本预览）。"""
        import httpx

        name = _sanitize_filename(info["name"]) or "unnamed.bin"
        url = info["url"] or await self._resolve_file_url(info, envelope)
        if not url:
            logger.warning("QQ 文件无下载链接：%s", name)
            return f"[用户发来文件：{name}（无法获取下载链接，未保存）]"
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.content
        except Exception as exc:  # noqa: BLE001
            logger.warning("下载 QQ 文件失败：%s %s", name, exc)
            return f"[用户发来文件：{name}（下载失败，未保存）]"

        max_mb = self._file_max_bytes // (1024 * 1024)
        if len(data) > self._file_max_bytes:
            logger.warning("QQ 文件超过 %sMB 上限，跳过：%s", max_mb, name)
            return f"[用户发来文件：{name}（超过 {max_mb}MB 上限，未保存）]"

        path = self._unique_path(name)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        except OSError as exc:
            logger.error("保存 QQ 文件失败：%s %s", path, exc)
            return f"[用户发来文件：{name}（保存失败）]"

        logger.info("QQ 文件已保存：%s（%dKB）", path, max(1, len(data) // 1024))
        note = f"[用户发来文件：{name}，已保存至 {path}]"
        preview = await asyncio.to_thread(_extract_text_preview, path)
        if preview:
            note += f"\n[文件内容预览]\n{preview}"
        return note

    def _unique_path(self, name: str) -> Path:
        """生成不冲突的保存路径（重名自动加序号）。"""
        base = Path(self._file_dir) / name
        if not base.exists():
            return base
        stem, suffix = base.stem, base.suffix
        for i in range(1, 1000):
            candidate = base.with_name(f"{stem}_{i}{suffix}")
            if not candidate.exists():
                return candidate
        return base

    async def _resolve_file_url(self, info: dict[str, str], envelope: dict[str, Any]) -> str:
        """file 段无 url 时，用 NapCat action 换取下载链接（私聊/群文件）。"""
        file_id = info["file_id"]
        if not file_id:
            return ""
        if envelope.get("message_type") == "group":
            action = "get_group_file_url"
            params: dict[str, Any] = {
                "group_id": str(envelope.get("group_id", "")),
                "file_id": file_id,
            }
        else:
            action = "get_private_file_url"
            params = {"file_id": file_id}
        resp = await self._call_action(action, params)
        data = (resp or {}).get("data") or {}
        return str(data.get("url", "") or "")

    async def _call_action(
        self, action: str, params: dict[str, Any], timeout: float = 15.0
    ) -> dict[str, Any] | None:
        """发送 OneBot action 并等待 echo 匹配的响应；超时/失败返回 None。"""
        if self._ws is None:
            return None
        self._action_seq += 1
        echo = f"sump-{self._action_seq}"
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_actions[echo] = fut
        try:
            await self._ws.send(json.dumps(
                {"action": action, "params": params, "echo": echo},
                ensure_ascii=False,
            ))
            return await asyncio.wait_for(fut, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning("NapCat action 失败：%s %s", action, exc)
            return None
        finally:
            self._pending_actions.pop(echo, None)


def _sanitize_filename(name: str) -> str:
    """清洗文件名：取 basename、替换危险字符、限长（防路径穿越）。"""
    cleaned = Path(str(name).replace("\\", "/")).name.strip()
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", cleaned)
    return cleaned[:120]


def _extract_text_preview(path: Path, max_chars: int = 4000) -> str:
    """提取文本类文件的内容预览（PDF / 纯文本）；不支持或失败返回空串。"""
    suffix = path.suffix.lower()
    try:
        if suffix == ".pdf":
            from pypdf import PdfReader  # 延迟导入：仅 PDF 场景需要

            reader = PdfReader(str(path))
            parts: list[str] = []
            total = 0
            for page in reader.pages[:30]:
                chunk = page.extract_text() or ""
                parts.append(chunk)
                total += len(chunk)
                if total >= max_chars * 2:
                    break
            text = "\n".join(parts).strip()
        elif suffix in {".txt", ".md", ".csv", ".json", ".log", ".yaml", ".yml"}:
            text = path.read_text(encoding="utf-8", errors="replace").strip()
        else:
            return ""
    except Exception as exc:  # noqa: BLE001
        logger.warning("提取文件文本失败：%s %s", path.name, exc)
        return ""
    if not text:
        return ""
    if len(text) > max_chars:
        return text[:max_chars] + "…（已截断）"
    return text


def _guess_image_mime(data: bytes) -> str:
    """按文件实际内容判断图片 MIME 类型（与 DeepSeek 支持格式对齐）。"""
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"GIF8"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"
