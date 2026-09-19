# ===== Stage 1: 构建前端 =====
FROM node:22-alpine AS frontend
WORKDIR /build
COPY src/frontend/package.json src/frontend/package-lock.json ./
RUN npm ci
COPY src/frontend/ ./
RUN npm run build

# ===== Stage 2: 后端运行时 =====
FROM python:3.12-slim
WORKDIR /app

# 安装 uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 先拷贝依赖清单与项目元数据（利用 Docker 层缓存）
COPY pyproject.toml uv.lock README.md LICENSE ./
# 默认走清华 PyPI 镜像（国内构建快且稳；可用 --build-arg UV_DEFAULT_INDEX=... 覆盖）
ARG UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple
ENV UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX}
RUN uv sync --frozen --no-dev --no-install-project

# 拷贝源码与配置，安装项目（src 布局：api/ 与 sump/ 包）
COPY src/ ./src/
COPY configs/ ./configs/
RUN uv sync --frozen --no-dev

# 拷贝前端构建产物（server.py 启动时静态挂载）
COPY --from=frontend /build/dist ./src/frontend/dist

ENV PYTHONUNBUFFERED=1
EXPOSE 8765
CMD ["uv", "run", "--no-sync", "uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8765"]
