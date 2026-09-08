FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 FLOW_DATA_DIR=/app/data
WORKDIR /app
RUN useradd --uid 10001 --create-home researcher
COPY pyproject.toml README.md requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock && mkdir /app/data && chown researcher:researcher /app/data
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
USER researcher
EXPOSE 8000
CMD ["bybit-flow", "serve", "--host", "0.0.0.0"]
