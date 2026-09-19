"""搜索工具测试（直连 SearXNG JSON API）"""

import json
import urllib.error

import pytest

from sump.tools.builtin.search import SearchTool


class _FakeResponse:
    """urlopen 返回的上下文管理器假响应。"""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _install_fake_urlopen(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict | None = None,
    exc: Exception | None = None,
    captured: list | None = None,
) -> None:
    """把 urllib.request.urlopen 替换为假实现（可捕获请求、可抛异常）。"""

    def fake_urlopen(req, timeout=None):
        if captured is not None:
            captured.append((req, timeout))
        if exc is not None:
            raise exc
        return _FakeResponse(payload or {})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)


@pytest.mark.asyncio
async def test_search_formats_results(monkeypatch):
    payload = {
        "results": [
            {"title": "A 标题", "url": "https://a.example", "content": "摘要 A"},
            {"title": "B 标题", "url": "https://b.example", "content": "摘要 B"},
        ]
    }
    _install_fake_urlopen(monkeypatch, payload=payload)
    tool = SearchTool(searxng_url="http://searxng:1010")

    out = await tool.execute(query="测试词")

    assert "测试词" in out
    assert "A 标题" in out
    assert "https://a.example" in out
    assert "摘要 B" in out


@pytest.mark.asyncio
async def test_search_limits_results(monkeypatch):
    payload = {
        "results": [
            {"title": f"t{i}", "url": f"https://x/{i}", "content": "c"} for i in range(5)
        ]
    }
    _install_fake_urlopen(monkeypatch, payload=payload)
    tool = SearchTool()

    out = await tool.execute(query="q", max_results=2)

    assert "t0" in out
    assert "t1" in out
    assert "t2" not in out


@pytest.mark.asyncio
async def test_search_truncates_large_max_results(monkeypatch):
    """max_results 超过上限（10）时截断。"""
    payload = {"results": [{"title": f"t{i}", "url": "u", "content": "c"} for i in range(15)]}
    _install_fake_urlopen(monkeypatch, payload=payload)
    tool = SearchTool()

    out = await tool.execute(query="q", max_results=99)

    assert "t9" in out
    assert "t10" not in out


@pytest.mark.asyncio
async def test_search_invalid_max_results_falls_back(monkeypatch):
    payload = {"results": [{"title": "t0", "url": "u", "content": "c"}]}
    _install_fake_urlopen(monkeypatch, payload=payload)
    tool = SearchTool()

    out = await tool.execute(query="q", max_results="abc")

    assert "t0" in out


@pytest.mark.asyncio
async def test_search_empty_query(monkeypatch):
    _install_fake_urlopen(monkeypatch, payload={"results": []})
    tool = SearchTool()

    out = await tool.execute(query="   ")

    assert out.startswith("错误")


@pytest.mark.asyncio
async def test_search_no_results(monkeypatch):
    _install_fake_urlopen(monkeypatch, payload={"results": []})
    tool = SearchTool()

    out = await tool.execute(query="不存在的东西")

    assert "未找到" in out


@pytest.mark.asyncio
async def test_search_connection_error(monkeypatch):
    _install_fake_urlopen(monkeypatch, exc=urllib.error.URLError("Connection refused"))
    tool = SearchTool(searxng_url="http://127.0.0.1:1")

    out = await tool.execute(query="q")

    assert "不可用" in out
    assert "127.0.0.1:1" in out


@pytest.mark.asyncio
async def test_search_timeout(monkeypatch):
    _install_fake_urlopen(monkeypatch, exc=TimeoutError())
    tool = SearchTool()

    out = await tool.execute(query="q")

    assert "超时" in out


@pytest.mark.asyncio
async def test_search_request_url(monkeypatch):
    """请求应打到 {base}/search，携带 q 与 format=json，base 末尾斜杠被规整。"""
    captured: list = []
    _install_fake_urlopen(monkeypatch, payload={"results": []}, captured=captured)
    tool = SearchTool(searxng_url="http://host.docker.internal:1010/", timeout=7)

    await tool.execute(query="hello world", max_results=3)

    req, timeout = captured[0]
    assert req.full_url.startswith("http://host.docker.internal:1010/search?")
    assert "q=hello+world" in req.full_url
    assert "format=json" in req.full_url
    assert timeout == 7.0


@pytest.mark.asyncio
async def test_search_truncates_long_content(monkeypatch):
    payload = {"results": [{"title": "t", "url": "u", "content": "x" * 1000}]}
    _install_fake_urlopen(monkeypatch, payload=payload)
    tool = SearchTool()

    out = await tool.execute(query="q")

    assert "…" in out
    assert len(out) < 1000


def test_search_schema():
    schema = SearchTool().to_openai_schema()
    assert schema["function"]["name"] == "search"
    props = schema["function"]["parameters"]["properties"]
    assert "query" in props
    assert "max_results" in props
    assert schema["function"]["parameters"]["required"] == ["query"]
