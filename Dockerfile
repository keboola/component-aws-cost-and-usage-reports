FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code/

COPY pyproject.toml uv.lock README.md /code/

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --all-groups --frozen

COPY src/ /code/src/
COPY tests/ /code/tests/

CMD ["python", "-u", "/code/src/component.py"]
