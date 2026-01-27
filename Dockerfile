FROM python:3.10-slim
ENV PYTHONIOENCODING utf-8

# Install build dependencies
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY . /code/
WORKDIR /code/

# Install dependencies using pip
RUN pip install --no-cache-dir flake8
RUN pip install --no-cache-dir -e .

CMD ["python", "-u", "/code/src/component.py"]
