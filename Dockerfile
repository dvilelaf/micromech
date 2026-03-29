FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --all-extras

COPY src/ src/

EXPOSE 8000

CMD ["uv", "run", "micromech", "run"]
