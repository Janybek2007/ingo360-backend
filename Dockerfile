FROM python:3.11.9-alpine

WORKDIR /app

COPY requirements.txt requirements-dev.txt .

RUN python -m pip install --upgrade pip setuptools wheel \
	&& pip config set global.timeout 120 \
	&& pip config set global.retries 10

RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

COPY . .

EXPOSE 8000

# CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
