FROM python:3.14-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd -m appuser
USER appuser
COPY main.py .
EXPOSE 8081
CMD ["python", "main.py"]
