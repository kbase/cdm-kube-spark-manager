FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

EXPOSE 8000

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8000", "--factory", "src.main:create_application"]