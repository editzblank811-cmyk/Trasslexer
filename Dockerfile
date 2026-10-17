FROM python:3.12-slim
WORKDIR /app
COPY bot.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "bot.py"]
