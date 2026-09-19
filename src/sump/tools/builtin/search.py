"""搜索工具（直连 SearXNG JSON API，无需 Node.js / MCP）"""

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from sump.tools.base import Tool


class SearchTool(Tool):
    """SearXNG 元搜索（聚合 Google/Bing/百度等，自托管实例）。"""

    name = "search"
    description = (
        "搜索互联网信息（SearXNG 元搜索引擎，聚合 Google/Bing/百度等）。"
        "返回结果标题、链接与摘要。"
    )
    parameters = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "搜索关键词"},
            "max_results": {
                "type": "integer",
                "description": "返回结果条数（1~10，默认 5）",
            },
        },
        "required": ["query"],
    }

    _MAX_RESULTS = 10
    _MAX_CONTENT = 300  # 单条摘要截断长度

    def __init__(
        self, searxng_url: str = "http://localhost:1010", timeout: float = 10.0
    ) -> None:
        self._searxng_url = str(searxng_url).rstrip("/")
        self._timeout = max(1.0, float(timeout))

    async def execute(self, **kwargs: Any) -> str:
        query = str(kwargs.get("query", "") or "").strip()
        if not query:
            return "错误：搜索关键词不能为空"
        try:
            max_results = int(kwargs.get("max_results", 5))
        except (TypeError, ValueError):
            max_results = 5
        max_results = max(1, min(max_results, self._MAX_RESULTS))

        # urllib 是阻塞 IO，放线程池避免卡事件循环
        try:
            results = await asyncio.to_thread(self._fetch, query, max_results)
        except TimeoutError:
            return f"搜索超时（{self._timeout:g}s）：{self._searxng_url}"
        except urllib.error.URLError as exc:
            return f"搜索服务不可用（{self._searxng_url}）：{exc.reason}"
        except Exception as exc:  # noqa: BLE001
            return f"搜索失败：{exc}"

        if not results:
            return f"未找到与“{query}”相关的结果"

        lines = [f"“{query}”的搜索结果："]
        for i, item in enumerate(results, 1):
            title = str(item.get("title", "")).strip() or "(无标题)"
            url = str(item.get("url", "")).strip()
            content = str(item.get("content", "")).strip()
            if len(content) > self._MAX_CONTENT:
                content = content[: self._MAX_CONTENT] + "…"
            lines.append(f"{i}. {title}\n{url}\n{content}")
        return "\n".join(lines)

    def _fetch(self, query: str, max_results: int) -> list[dict[str, Any]]:
        """在线程中执行：请求 SearXNG JSON 接口并返回截断后的结果列表。"""
        params = urllib.parse.urlencode({"q": query, "format": "json"})
        req = urllib.request.Request(
            f"{self._searxng_url}/search?{params}",
            headers={"User-Agent": "sump-search/1.0"},
        )
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return list(data.get("results", []) or [])[:max_results]
