"""审批超时与审批后继续执行测试"""

import asyncio

import pytest

from sump.agent import Agent
from sump.tools.base import Tool


class _FakeEmbedder:
    def embed(self, texts):
        return [[float(len(t)), 1.0, 0.0] for t in texts]


class _ReplyBackend:
    def __init__(self, reply: str = "ok") -> None:
        self._reply = reply

    async def chat(self, messages, tools=None):
        return {"content": self._reply, "tool_calls": None, "reasoning_content": None}

    async def chat_stream(self, messages):
        yield {"type": "content", "text": self._reply}

    async def chat_flash(self, text, *, max_tokens=256, temperature=0.3):
        return "safe"


class _DummyTool(Tool):
    name = "dummy"
    description = "dummy"
    parameters = {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        return "ok"


class TestApprovalTimeout:
    @pytest.mark.asyncio
    async def test_timeout_auto_rejects(self, config):
        config._data.setdefault("security", {})["approval_timeout"] = 0.05
        agent = Agent(config, deep_embedder=_FakeEmbedder())

        # 模拟挂起：先写待确认 tool 消息，再挂起审批
        agent.ctx.add_tool_message("tc1", "⛔ 安全审查待确认 | call_id: c1")
        agent._api_approval_pending(
            "c1", "rm -rf /", None, "tc1", {"command": "rm -rf /"},
            summary="删除文件", danger="high",
        )

        await asyncio.sleep(0.2)  # 等待超时定时器触发

        assert "c1" not in agent._pending_approvals
        tool_msgs = [
            m for m in agent.ctx.messages if m.role == "tool" and m.tool_call_id == "tc1"
        ]
        assert tool_msgs[-1].content == "审批超时，已自动拒绝执行"


class TestOrphanToolCalls:
    """被中断的 tool_calls（重启打断审批）在加载时自动补占位响应。"""

    def test_repair_adds_placeholder(self, config):
        from sump.types import Message

        agent = Agent(config, deep_embedder=_FakeEmbedder())
        agent.ctx.messages.append(Message(
            role="assistant",
            content="",
            tool_calls=[
                {"id": "c1", "type": "function", "function": {"name": "shell", "arguments": "{}"}},
                {"id": "c2", "type": "function", "function": {"name": "shell", "arguments": "{}"}},
            ],
        ))
        agent.ctx.messages.append(Message(role="tool", content="done", tool_call_id="c1"))

        agent._repair_orphan_tool_calls()

        placeholders = [
            m for m in agent.ctx.messages
            if m.role == "tool" and m.tool_call_id == "c2"
        ]
        assert len(placeholders) == 1
        assert "未完成" in placeholders[0].content
        # 占位响应紧跟 assistant 消息（保持 API 要求的顺序）
        assert agent.ctx.messages[1].tool_call_id == "c2"

    def test_repair_noop_when_complete(self, config):
        from sump.types import Message

        agent = Agent(config, deep_embedder=_FakeEmbedder())
        agent.ctx.messages.append(Message(
            role="assistant",
            content="",
            tool_calls=[
                {"id": "c1", "type": "function", "function": {"name": "shell", "arguments": "{}"}}
            ],
        ))
        agent.ctx.messages.append(Message(role="tool", content="done", tool_call_id="c1"))
        before = len(agent.ctx.messages)

        agent._repair_orphan_tool_calls()

        assert len(agent.ctx.messages) == before


class TestApproveAndContinue:
    @pytest.mark.asyncio
    async def test_approve_executes_and_continues(self, config):
        agent = Agent(config, deep_embedder=_FakeEmbedder())
        agent.llm._backend = _ReplyBackend("继续后的回复")

        agent.ctx.add_tool_message("tc1", "⛔ 安全审查待确认 | call_id: c1")
        agent._api_approval_pending(
            "c1", "echo hi", _DummyTool(), "tc1", {"command": "echo hi"},
            summary="输出文本", danger="low",
        )

        result = await agent.approve_and_continue("c1", True)

        assert result == "ok"  # 工具执行结果
        tool_msgs = [
            m for m in agent.ctx.messages if m.role == "tool" and m.tool_call_id == "tc1"
        ]
        assert tool_msgs[-1].content == "ok"
        assert "c1" not in agent._pending_approvals
