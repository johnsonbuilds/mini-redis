FROM python:3.12-slim

WORKDIR /app

COPY mini_redis.py .

EXPOSE 6379

CMD ["python", "mini_redis.py"]