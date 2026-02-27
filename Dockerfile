FROM python:3.13-slim
ENV PYTHONIOENCODING=utf-8
ENV PYTHONPATH=/code/src

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /code/

COPY pyproject.toml README.md /code/
COPY src/ /code/src/
COPY tests/ /code/tests/

RUN uv pip install --system --no-cache ".[dev]"

CMD ["python", "-u", "/code/src/component.py"]
