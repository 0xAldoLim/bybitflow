FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 FLOW_DATA_DIR=/app/data
WORKDIR /app
RUN useradd --uid 10001 --create-home researcher
COPY requirements.lock ./
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout 60 --retries 5 -r requirements.lock && mkdir /app/data && chown researcher:researcher /app/data
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
USER researcher
EXPOSE 8000
CMD ["bybit-flow", "serve", "--host", "0.0.0.0"]
