FROM python:3.10-slim
ENV PYTHONIOENCODING=utf-8
ENV PYTHONPATH=/code/src

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install gcc to be able to build packages - e.g. required by regex, dateparser
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /code/

# Copy pyproject.toml, README, config files, source and tests
COPY pyproject.toml README.md flake8.cfg /code/
COPY src/ /code/src/
COPY tests/ /code/tests/

# Install dependencies using UV (10-100x faster than pip)
RUN uv pip install --system --no-cache .

CMD ["python", "-u", "/code/src/component.py"]
