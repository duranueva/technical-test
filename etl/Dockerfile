FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
CMD ["python", "src/load_raw.py", "--input", "datasets/input.csv", "--table", "raw_purchases", "--schema", "raw", "--if-exists", "replace"]
