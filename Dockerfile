FROM python:3.10-slim
ENV PYTHONIOENCODING utf-8

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY . /code/
WORKDIR /code/

# Install dependencies using uv
RUN uv pip install --system flake8
RUN uv pip install --system -e .

CMD ["python", "-u", "/code/src/component.py"]
