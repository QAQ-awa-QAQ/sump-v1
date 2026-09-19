"""NapCat 插件测试：OneBot 解析、会话路由、钩子闭环"""

import asyncio
import json
from types import SimpleNamespace

import pytest

from sump.event import AgentEvents
from sump.plugins.builtin.napcat_plugin import NapCatPlugin


class _FakeGroupAgentWithReply:
    """模拟群聊 Agent：记录消息 + run_core 时通过钩子 emit 回复。"""

    def __init__(self, bus, session_id: str) -> None:
        self._bus = bus
        self._session_id = session_id
        self.recorded: list[str] = []
        self.ctx = SimpleNamespace(
            add_user_message=lambda text, images=None: self.recorded.append(text)
        )

    async def run_core(self):
        await self._bus.emit(
            AgentEvents.REPLY, session_id=self._session_id, content="星宝在"
        )
        if False:  # 使其成为 async generator
            yield


class TestExtractText:
    def test_string(self):
        assert NapCatPlugin._extract_text("你好") == "你好"

    def test_segments(self):
        assert NapCatPlugin._extract_text([
            {"type": "text", "data": {"text": "你好"}},
            {"type": "face", "data": {"id": "1"}},
            {"type": "text", "data": {"text": "世界"}},
        ]) == "你好世界"

    def test_non_text_and_none(self):
        assert NapCatPlugin._extract_text([{"type": "at", "data": {"qq": "123"}}]) == ""
        assert NapCatPlugin._extract_text(None) == ""


class TestHandleMessage:
    @pytest.mark.asyncio
    async def test_private_message(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: True)
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)

        calls: list[str] = []
        streams: list[tuple[str, list[str]]] = []

        async def fake_run_stream(text, images=None):
            streams.append((text, images))
            if False:
                yield

        fake_agent = SimpleNamespace(run_stream=fake_run_stream)

        def fake_get_agent(sid):
            calls.append(sid)
            return fake_agent

        monkeypatch.setattr(plugin, "_get_agent", fake_get_agent)

        received: list[dict] = []
        plugin._bus.on(
            AgentEvents.MESSAGE_RECEIVED, lambda **kw: received.append(kw), consumer="t"
        )

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": [{"type": "text", "data": {"text": "你好"}}],
        })

        assert calls == ["private_111"]
        assert streams == [("你好", [])]
        assert received[0]["content"] == "你好"
        assert received[0]["source"] == "napcat"
        assert sent == []  # 无回复钩子，不发回

    @pytest.mark.asyncio
    async def test_group_records_message(self, config, monkeypatch):
        """群聊：记录消息；判断不插话则不回复。"""
        plugin = NapCatPlugin(config)
        recorded: list[tuple[str, list[str]]] = []
        run_core_calls: list[int] = []
        agent = SimpleNamespace(
            ctx=SimpleNamespace(
                add_user_message=lambda text, images=None: recorded.append((text, images))
            )
        )

        async def fake_run_core():
            run_core_calls.append(1)
            if False:
                yield

        agent.run_core = fake_run_core
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)

        async def fake_should_speak(agent, text, mentioned, at_me):
            return False

        monkeypatch.setattr(plugin, "_should_speak", fake_should_speak)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "group",
            "user_id": 111,
            "group_id": 222,
            "sender": {"nickname": "小明"},
            "message": "今天天气不错",
        })

        assert recorded == [("[小明] 今天天气不错", [])]
        assert run_core_calls == []

    @pytest.mark.asyncio
    async def test_group_at_me_speaks(self, config, monkeypatch):
        """群聊：被 @ 必回。"""
        plugin = NapCatPlugin(config)
        recorded: list[tuple[str, list[str]]] = []
        run_core_calls: list[int] = []
        agent = SimpleNamespace(
            ctx=SimpleNamespace(
                add_user_message=lambda text, images=None: recorded.append((text, images))
            )
        )

        async def fake_run_core():
            run_core_calls.append(1)
            if False:
                yield

        agent.run_core = fake_run_core
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)
        monkeypatch.setattr(plugin, "_is_at_me", lambda data: True)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "group",
            "user_id": 111,
            "group_id": 222,
            "sender": {"nickname": "小明"},
            "message": [{"type": "at", "data": {"qq": "10001"}}, {"type": "text", "data": {"text": "星宝在吗"}}],
        })

        assert recorded == [("[小明] 星宝在吗", [])]
        assert run_core_calls == [1]


class TestOnReply:
    @pytest.mark.asyncio
    async def test_routes_by_session_prefix(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)

        await plugin._on_reply("group_123", "群回复")
        await plugin._on_reply("private_456", "私聊回复")
        await plugin._on_reply("other_1", "忽略")  # 未知前缀不发送

        assert sent == [
            ({"message_type": "group", "group_id": "123"}, "群回复"),
            ({"message_type": "private", "user_id": "456"}, "私聊回复"),
        ]


class TestRoundTrip:
    @pytest.mark.asyncio
    async def test_group_roundtrip(self, config, monkeypatch):
        """群聊 @ → 记录 + run_core → REPLY 钩子 → 发回 QQ 的闭环。"""
        plugin = NapCatPlugin(config)
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)
        monkeypatch.setattr(plugin, "_is_at_me", lambda data: True)

        agent = _FakeGroupAgentWithReply(plugin._bus, "group_123")
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "group",
            "user_id": 111,
            "group_id": 123,
            "sender": {"nickname": "小明"},
            "message": "星宝在吗",
        })

        assert sent == [({"message_type": "group", "group_id": "123"}, "星宝在")]


class TestOwnerRestriction:
    @pytest.mark.asyncio
    async def test_non_owner_rejected(self, config, monkeypatch):
        """非主人消息：拒绝执行，不驱动 Agent。"""
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_owner_id", "999")
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)
        called: list[str] = []
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: called.append(sid) or None)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": "执行 rm -rf /",
        })

        assert called == []  # Agent 未被调用
        assert sent and "拒绝" in sent[0][1]

    @pytest.mark.asyncio
    async def test_owner_allowed(self, config, monkeypatch):
        """主人消息：正常驱动 Agent。"""
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_owner_id", "999")
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)
        called: list[str] = []

        async def fake_run_stream(text, images=None):
            if False:
                yield

        fake_agent = SimpleNamespace(run_stream=fake_run_stream)
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: called.append(sid) or fake_agent)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 999,
            "message": "你好",
        })

        assert called == ["private_999"]
        assert sent == []  # fake agent 不 emit 回复

    def test_is_owner(self, config):
        plugin = NapCatPlugin(config)
        plugin._owner_id = "999"
        assert plugin._is_owner("999") is True
        assert plugin._is_owner("111") is False
        # 未配置主人 → 拒绝所有人
        plugin._owner_id = ""
        assert plugin._is_owner("999") is False


class TestShouldSpeak:
    @pytest.mark.asyncio
    async def test_at_me_always_speaks(self, config):
        plugin = NapCatPlugin(config)
        agent = SimpleNamespace(llm=None)  # @ 必回，不调 flash
        assert await plugin._should_speak(agent, "在吗", False, True) is True

    @pytest.mark.asyncio
    async def test_mention_with_flash_yes(self, config):
        plugin = NapCatPlugin(config)

        async def fake_flash(prompt, *, max_tokens=256, temperature=0.3):
            return "yes"

        agent = SimpleNamespace(llm=SimpleNamespace(chat_flash=fake_flash))
        assert await plugin._should_speak(agent, "星宝帮我", True, False) is True

    @pytest.mark.asyncio
    async def test_plain_flash_no(self, config):
        plugin = NapCatPlugin(config)

        async def fake_flash(prompt, *, max_tokens=256, temperature=0.3):
            return "no"

        agent = SimpleNamespace(llm=SimpleNamespace(chat_flash=fake_flash))
        assert await plugin._should_speak(agent, "随便聊聊", False, False) is False

    @pytest.mark.asyncio
    async def test_flash_error_falls_back_to_mention(self, config):
        plugin = NapCatPlugin(config)

        async def boom(prompt, *, max_tokens=256, temperature=0.3):
            raise RuntimeError("x")

        agent = SimpleNamespace(llm=SimpleNamespace(chat_flash=boom))
        assert await plugin._should_speak(agent, "星宝", True, False) is True
        assert await plugin._should_speak(agent, "随便", False, False) is False


class TestIsAtMe:
    def test_is_at_me(self, config):
        plugin = NapCatPlugin(config)
        assert plugin._is_at_me({
            "self_id": "10001",
            "message": [
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "text", "data": {"text": "在吗"}},
            ],
        }) is True
        assert plugin._is_at_me({
            "self_id": "10001",
            "message": [{"type": "text", "data": {"text": "在吗"}}],
        }) is False
        assert plugin._is_at_me({
            "self_id": "10001",
            "message": [{"type": "at", "data": {"qq": "999"}}],
        }) is False
        assert plugin._is_at_me({"self_id": "", "message": "在吗"}) is False


class TestImageExtraction:
    def test_extract_image_urls(self, config):
        assert NapCatPlugin._extract_image_urls([
            {"type": "image", "data": {"url": "http://x/a.jpg"}},
            {"type": "text", "data": {"text": "看这个"}},
        ]) == ["http://x/a.jpg"]
        assert NapCatPlugin._extract_image_urls("text") == []
        assert NapCatPlugin._extract_image_urls([{"type": "text", "data": {"text": "x"}}]) == []

    @pytest.mark.asyncio
    async def test_image_message_recorded(self, config, monkeypatch):
        """纯图片消息：下载转 base64 后随消息传给 Agent，不因无文本被忽略。"""
        plugin = NapCatPlugin(config)
        recorded: list[tuple[str, list[str]]] = []
        agent = SimpleNamespace(
            ctx=SimpleNamespace(
                add_user_message=lambda text, images=None: recorded.append((text, images))
            )
        )
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)

        async def fake_download(url):
            return "data:image/jpeg;base64,AAAA"

        monkeypatch.setattr(plugin, "_download_image_data_url", fake_download)

        async def fake_should_speak(agent, text, mentioned, at_me):
            return False

        monkeypatch.setattr(plugin, "_should_speak", fake_should_speak)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "group",
            "user_id": 111,
            "group_id": 222,
            "sender": {"nickname": "小明"},
            "message": [{"type": "image", "data": {"url": "http://x/pic.jpg"}}],
        })

        assert recorded == [("[小明] ", ["data:image/jpeg;base64,AAAA"])]

    def test_guess_image_mime(self):
        from sump.plugins.builtin.napcat_plugin import _guess_image_mime

        assert _guess_image_mime(b"\xff\xd8\xffxx") == "image/jpeg"
        assert _guess_image_mime(b"\x89PNG\r\n\x1a\nxx") == "image/png"
        assert _guess_image_mime(b"GIF8xx") == "image/gif"
        assert _guess_image_mime(b"RIFF....WEBPxx") == "image/webp"
        assert _guess_image_mime(b"unknown") == "image/jpeg"

    @pytest.mark.asyncio
    async def test_private_image_passthrough(self, config, monkeypatch):
        """私聊图片：下载转 data URL 后随消息传给 Agent。"""
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: True)

        async def fake_download(url):
            return "data:image/png;base64,AAAA"

        monkeypatch.setattr(plugin, "_download_image_data_url", fake_download)

        streams: list[tuple[str, list[str]]] = []

        async def fake_run_stream(text, images=None):
            streams.append((text, images))
            if False:
                yield

        fake_agent = SimpleNamespace(run_stream=fake_run_stream)
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: fake_agent)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": [
                {"type": "image", "data": {"url": "http://x/pic.png"}},
                {"type": "text", "data": {"text": "看看这图"}},
            ],
        })

        assert streams == [("看看这图", ["data:image/png;base64,AAAA"])]

    @pytest.mark.asyncio
    async def test_image_download_failure_placeholder(self, config, monkeypatch):
        """图片下载失败：文本降级为 [图片] 占位，不携带图片。"""
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: True)

        async def fake_download(url):
            return None

        monkeypatch.setattr(plugin, "_download_image_data_url", fake_download)

        streams: list[tuple[str, list[str]]] = []

        async def fake_run_stream(text, images=None):
            streams.append((text, images))
            if False:
                yield

        fake_agent = SimpleNamespace(run_stream=fake_run_stream)
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: fake_agent)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": [{"type": "image", "data": {"url": "http://x/pic.png"}}],
        })

        assert streams == [("[图片]", [])]


class TestNapCatApproval:
    @pytest.mark.asyncio
    async def test_approval_pending_pushes_to_owner(self, config, monkeypatch):
        config._data.setdefault("napcat", {})["owner_id"] = "111"
        plugin = NapCatPlugin(config)
        sent: list[tuple] = []

        async def fake_send(ctx, content):
            sent.append((ctx, content))

        monkeypatch.setattr(plugin, "_send", fake_send)

        await plugin._on_approval_pending(
            session_id="private_111",
            call_id="c1",
            command="rm -rf /",
            summary="删除文件",
            danger="high",
        )

        assert plugin._pending_approval["private_111"] == {"call_id": "c1", "source": "未知"}
        assert sent and "待审批" in sent[0][1]
        assert "rm -rf /" in sent[0][1]
        # 审批发到主人私聊
        assert sent[0][0] == {"message_type": "private", "user_id": "111"}

    @pytest.mark.asyncio
    async def test_approval_response_1_approves(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: True)
        plugin._pending_approval["private_111"] = {"call_id": "c1", "source": "x"}

        calls: list[tuple] = []

        async def fake_approve_and_continue(call_id, approved):
            calls.append((call_id, approved))

        agent = SimpleNamespace(approve_and_continue=fake_approve_and_continue)
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)
        monkeypatch.setattr(plugin, "_send", lambda ctx, content: None)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": "1",
        })

        assert calls == [("c1", True)]

    @pytest.mark.asyncio
    async def test_approval_response_2_rejects(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: True)
        plugin._pending_approval["group_222"] = {"call_id": "c2", "source": "群聊 222 · 张三"}

        calls: list[tuple] = []

        async def fake_approve_and_continue(call_id, approved):
            calls.append((call_id, approved))

        agent = SimpleNamespace(approve_and_continue=fake_approve_and_continue)
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)
        monkeypatch.setattr(plugin, "_send", lambda ctx, content: None)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "private",
            "user_id": 111,
            "message": "2",
        })

        assert calls == [("c2", False)]


class TestOwnerMarker:
    @pytest.mark.asyncio
    async def test_owner_message_marked(self, config, monkeypatch):
        """主人消息带 owner_marker 标记，非主人不带。"""
        config._data.setdefault("memory", {})["owner_marker"] = "·主人"
        plugin = NapCatPlugin(config)
        monkeypatch.setattr(plugin, "_is_owner", lambda uid: uid == "999")
        recorded: list[str] = []
        agent = SimpleNamespace(
            ctx=SimpleNamespace(
                add_user_message=lambda text, images=None: recorded.append(text)
            )
        )
        monkeypatch.setattr(plugin, "_get_agent", lambda sid: agent)

        async def fake_should_speak(agent, text, mentioned, at_me):
            return False

        monkeypatch.setattr(plugin, "_should_speak", fake_should_speak)

        await plugin._handle_message({
            "post_type": "message",
            "message_type": "group",
            "user_id": 999,
            "group_id": 222,
            "sender": {"nickname": "小明"},
            "message": "我喜欢 Python",
        })

        assert recorded == ["[小明·主人] 我喜欢 Python"]


class TestSendImageTo:
    @pytest.mark.asyncio
    async def test_send_image_segments(self, config, tmp_path):
        """send_image_to：OneBot 消息段数组，file 为 file:// URI，附言为 text 段。"""
        plugin = NapCatPlugin(config)
        sent_raw: list[str] = []

        class _FakeWS:
            async def send(self, raw: str) -> None:
                sent_raw.append(raw)

        plugin._ws = _FakeWS()
        pic = tmp_path / "cat.png"
        pic.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x")

        error = await plugin.send_image_to("group_123", str(pic), "看这个")
        assert error is None
        msg = json.loads(sent_raw[0])
        assert msg["action"] == "send_msg"
        assert msg["params"]["group_id"] == "123"
        segments = msg["params"]["message"]
        assert segments[0]["type"] == "image"
        assert segments[0]["data"]["file"].startswith("file:///")
        assert segments[1] == {"type": "text", "data": {"text": "看这个"}}

    @pytest.mark.asyncio
    async def test_not_connected(self, config, tmp_path):
        plugin = NapCatPlugin(config)
        pic = tmp_path / "cat.png"
        pic.write_bytes(b"x")
        assert await plugin.send_image_to("group_123", str(pic)) == "QQ 未连接"

    @pytest.mark.asyncio
    async def test_unknown_session(self, config):
        plugin = NapCatPlugin(config)
        assert await plugin.send_image_to("weird_1", "x.png") == "未知会话：weird_1"

    def test_qq_agent_has_image_tool(self, config):
        """QQ 场景的 Agent 注册了 send_qq_image 与资产工具。"""
        plugin = NapCatPlugin(config)
        agent = plugin._get_agent("group_tool_test")
        assert agent.tools.get("send_qq_image") is not None
        assert agent.tools.get("asset_save") is not None
        assert agent.tools.get("asset_search") is not None


class TestReconnectBackoff:
    """连接循环反风暴：正常断开与异常都必须等待后再重连。"""

    @pytest.mark.asyncio
    async def test_normal_disconnect_waits_fixed_delay(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        calls: list[int] = []
        sleeps: list[float] = []

        async def fake_connect():
            calls.append(1)
            if len(calls) >= 3:
                raise asyncio.CancelledError

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr(plugin, "_connect_and_serve", fake_connect)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await plugin._run_forever()
        assert sleeps == [1.0, 1.0]

    @pytest.mark.asyncio
    async def test_errors_use_exponential_backoff(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        calls: list[int] = []
        sleeps: list[float] = []

        async def fake_connect():
            calls.append(1)
            if len(calls) >= 4:
                raise asyncio.CancelledError
            raise RuntimeError("boom")

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr(plugin, "_connect_and_serve", fake_connect)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await plugin._run_forever()
        assert sleeps == [1.0, 2.0, 4.0]


def _make_pdf_with_text(text: str) -> bytes:
    """构造带文本的最小 PDF（含正确 xref），用于解析测试。"""
    objs = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n",
    ]
    stream = f"BT /F1 24 Tf 72 700 Td ({text}) Tj ET".encode()
    objs.append(
        b"4 0 obj\n<< /Length %d >>\nstream\n%s\nendstream\nendobj\n" % (len(stream), stream)
    )
    objs.append(b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n")
    out = b"%PDF-1.4\n"
    offsets: list[int] = []
    for obj in objs:
        offsets.append(len(out))
        out += obj
    xref_pos = len(out)
    out += b"xref\n0 %d\n" % (len(objs) + 1)
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (
        len(objs) + 1,
        xref_pos,
    )
    return out


class TestIncomingFile:
    """QQ 文件接收：解析 / 消毒 / 下载保存 / 文本预览."""

    def test_extract_files(self):
        message = [
            {"type": "text", "data": {"text": "看这个"}},
            {
                "type": "file",
                "data": {
                    "file": "课表 (2).pdf",
                    "file_id": "abc-123",
                    "file_size": "12345",
                    "url": "https://example.com/dl",
                },
            },
        ]
        assert NapCatPlugin._extract_files(message) == [
            {
                "file_id": "abc-123",
                "name": "课表 (2).pdf",
                "url": "https://example.com/dl",
                "size": "12345",
            }
        ]

    def test_extract_files_ignores_others(self):
        assert NapCatPlugin._extract_files("not a list") == []
        assert NapCatPlugin._extract_files(None) == []
        assert NapCatPlugin._extract_files([{"type": "image", "data": {}}]) == []

    def test_sanitize_filename(self):
        from sump.plugins.builtin.napcat_plugin import _sanitize_filename

        assert _sanitize_filename("课表 (2).pdf") == "课表 (2).pdf"
        assert _sanitize_filename("../../etc/passwd") == "passwd"
        assert _sanitize_filename("..\\..\\win.ini") == "win.ini"
        assert _sanitize_filename("") == ""

    def test_text_preview_txt(self, tmp_path):
        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "a.txt"
        p.write_text("hello world", encoding="utf-8")
        assert _extract_text_preview(p) == "hello world"

    def test_text_preview_truncates(self, tmp_path):
        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "b.txt"
        p.write_text("x" * 5000, encoding="utf-8")
        out = _extract_text_preview(p)
        assert out.endswith("…（已截断）")
        assert len(out) < 5000

    def test_text_preview_unsupported_binary(self, tmp_path):
        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "c.bin"
        p.write_bytes(b"\x00\x01")
        assert _extract_text_preview(p) == ""

    def test_text_preview_pdf_blank(self, tmp_path):
        """可解析但无文本的 PDF → 空预览（不报错）。"""
        from pypdf import PdfWriter

        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "blank.pdf"
        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        with open(p, "wb") as f:
            writer.write(f)
        assert _extract_text_preview(p) == ""

    def test_text_preview_pdf_corrupt(self, tmp_path):
        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "bad.pdf"
        p.write_bytes(b"%PDF-1.4 broken")
        assert _extract_text_preview(p) == ""

    def test_text_preview_pdf_extracts_text(self, tmp_path):
        """pypdf 能从真实 PDF 提取文本。"""
        from sump.plugins.builtin.napcat_plugin import _extract_text_preview

        p = tmp_path / "hello.pdf"
        p.write_bytes(_make_pdf_with_text("Hello PDF World"))
        out = _extract_text_preview(p)
        assert "Hello PDF World" in out

    @pytest.mark.asyncio
    async def test_call_action_echo_roundtrip(self, config):
        plugin = NapCatPlugin(config)

        class _FakeWS:
            async def send(self, payload):
                data = json.loads(payload)
                await plugin._handle_raw(json.dumps({
                    "status": "ok",
                    "echo": data["echo"],
                    "data": {"url": "https://cdn/x"},
                }))

        plugin._ws = _FakeWS()
        resp = await plugin._call_action("get_private_file_url", {"file_id": "x"}, timeout=2)
        assert resp is not None
        assert resp["data"]["url"] == "https://cdn/x"
        assert plugin._pending_actions == {}

    @pytest.mark.asyncio
    async def test_call_action_timeout_cleans_pending(self, config):
        plugin = NapCatPlugin(config)

        class _FakeWS:
            async def send(self, payload):
                pass  # 永不响应

        plugin._ws = _FakeWS()
        resp = await plugin._call_action("get_private_file_url", {"file_id": "x"}, timeout=0.05)
        assert resp is None
        assert plugin._pending_actions == {}

    @pytest.mark.asyncio
    async def test_save_incoming_file_with_url(self, config, tmp_path, monkeypatch):
        """file 段自带 url：直接下载保存 + txt 预览注入。"""
        import httpx

        plugin = NapCatPlugin(config)
        plugin._file_dir = str(tmp_path)

        class _FakeResp:
            content = b"timetable data"

            def raise_for_status(self):
                pass

        class _FakeClient:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url):
                assert url == "https://x/dl"
                return _FakeResp()

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
        info = {"file_id": "f1", "name": "课表.txt", "url": "https://x/dl", "size": "10"}
        note = await plugin._save_incoming_file(info, {"message_type": "private"})
        assert "课表.txt" in note
        assert "已保存至" in note
        assert "timetable data" in note
        assert (tmp_path / "课表.txt").read_bytes() == b"timetable data"

    @pytest.mark.asyncio
    async def test_save_file_without_url_uses_private_action(self, config, tmp_path, monkeypatch):
        """无 url：走 get_private_file_url 换取链接。"""
        import httpx

        plugin = NapCatPlugin(config)
        plugin._file_dir = str(tmp_path)
        calls = []

        async def fake_action(action, params, timeout=15.0):
            calls.append((action, params))
            return {"status": "ok", "data": {"url": "https://cdn/file.bin"}}

        monkeypatch.setattr(plugin, "_call_action", fake_action)

        class _FakeResp:
            content = b"binary-data"

            def raise_for_status(self):
                pass

        class _FakeClient:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url):
                assert url == "https://cdn/file.bin"
                return _FakeResp()

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
        info = {"file_id": "f2", "name": "数据.bin", "url": "", "size": "10"}
        note = await plugin._save_incoming_file(info, {"message_type": "private"})
        assert calls == [("get_private_file_url", {"file_id": "f2"})]
        assert "已保存至" in note
        assert (tmp_path / "数据.bin").exists()

    @pytest.mark.asyncio
    async def test_save_file_group_uses_group_action(self, config, monkeypatch):
        """群聊文件走 get_group_file_url；拿不到链接时给出占位说明。"""
        plugin = NapCatPlugin(config)
        calls = []

        async def fake_action(action, params, timeout=15.0):
            calls.append((action, params))
            return {"status": "ok", "data": {"url": ""}}

        monkeypatch.setattr(plugin, "_call_action", fake_action)
        info = {"file_id": "f3", "name": "a.pdf", "url": "", "size": "1"}
        note = await plugin._save_incoming_file(info, {"message_type": "group", "group_id": "123"})
        assert calls == [("get_group_file_url", {"group_id": "123", "file_id": "f3"})]
        assert "无法获取下载链接" in note

    @pytest.mark.asyncio
    async def test_save_file_over_limit_skips(self, config, tmp_path, monkeypatch):
        """超过大小上限：不保存。"""
        import httpx

        plugin = NapCatPlugin(config)
        plugin._file_dir = str(tmp_path)
        plugin._file_max_bytes = 10  # 10 字节上限

        class _FakeResp:
            content = b"x" * 100

            def raise_for_status(self):
                pass

        class _FakeClient:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url):
                return _FakeResp()

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
        info = {"file_id": "f4", "name": "big.bin", "url": "https://x/dl", "size": "100"}
        note = await plugin._save_incoming_file(info, {"message_type": "private"})
        assert "上限" in note
        assert not (tmp_path / "big.bin").exists()

    @pytest.mark.asyncio
    async def test_handle_message_injects_file_note(self, config, tmp_path, monkeypatch):
        """file 段走通 _handle_message：说明文本（含路径与预览）注入 Agent。"""
        import httpx

        plugin = NapCatPlugin(config)
        plugin._owner_id = "2271917353"
        plugin._file_dir = str(tmp_path)

        class _FakeResp:
            content = b"timetable data"

            def raise_for_status(self):
                pass

        class _FakeClient:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url):
                return _FakeResp()

        monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

        captured = {}

        class _FakeAgent:
            async def run_stream(self, text, images=None):
                captured["text"] = text
                if False:
                    yield

        plugin._agents["private_2271917353"] = _FakeAgent()
        data = {
            "post_type": "message",
            "message_type": "private",
            "user_id": "2271917353",
            "message": [
                {
                    "type": "file",
                    "data": {"file": "课表.txt", "file_id": "f9", "url": "https://x/dl"},
                },
            ],
        }
        await plugin._handle_message(data)
        assert "课表.txt" in captured["text"]
        assert "已保存至" in captured["text"]
        assert "timetable data" in captured["text"]


class TestMessageDispatch:
    """消息事件的后台派发：不阻塞 action 响应，且消息间保持串行。"""

    @pytest.mark.asyncio
    async def test_handle_raw_spawns_background_task(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        handled: list[dict] = []

        async def fake_safe(data):
            handled.append(data)

        monkeypatch.setattr(plugin, "_handle_message_safe", fake_safe)
        await plugin._handle_raw(json.dumps({"post_type": "message", "user_id": "1"}))
        await asyncio.sleep(0)  # 让后台任务得到调度
        assert len(handled) == 1
        assert handled[0]["user_id"] == "1"

    @pytest.mark.asyncio
    async def test_messages_processed_serially(self, config, monkeypatch):
        plugin = NapCatPlugin(config)
        order: list[str] = []

        async def fake_handle(data):
            order.append(f"start-{data['n']}")
            await asyncio.sleep(0.01)
            order.append(f"end-{data['n']}")

        monkeypatch.setattr(plugin, "_handle_message", fake_handle)
        await plugin._handle_raw(json.dumps({"post_type": "message", "n": 1}))
        await plugin._handle_raw(json.dumps({"post_type": "message", "n": 2}))
        await asyncio.sleep(0.05)
        assert order == ["start-1", "end-1", "start-2", "end-2"]

    @pytest.mark.asyncio
    async def test_action_response_not_blocked_by_message(self, config, monkeypatch):
        """消息处理阻塞时，action 响应仍能被立刻分发（防自等死锁）。"""
        plugin = NapCatPlugin(config)
        release = asyncio.Event()

        async def slow_handle(data):
            await release.wait()

        monkeypatch.setattr(plugin, "_handle_message", slow_handle)
        await plugin._handle_raw(json.dumps({"post_type": "message", "n": 1}))
        await asyncio.sleep(0)  # 让慢任务先占住锁

        fut = asyncio.get_running_loop().create_future()
        plugin._pending_actions["sump-1"] = fut
        await plugin._handle_raw(
            json.dumps({"status": "ok", "echo": "sump-1", "data": {"url": "u"}})
        )
        assert fut.done()
        assert fut.result()["data"]["url"] == "u"

        release.set()
        await asyncio.sleep(0.02)
