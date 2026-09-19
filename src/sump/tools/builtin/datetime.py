"""时间工具"""

from datetime import datetime

from sump.tools.base import Tool
from typing import Any


class DateTimeTool(Tool):
    name = "datetime"
    description = "获取当前日期和时间"
    parameters = {
        "type": "object",
        "properties": {},
    }

    async def execute(self, **kwargs: Any) -> Any:
        return datetime.now().astimezone().isoformat()
