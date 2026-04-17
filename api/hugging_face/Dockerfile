FROM python:3.11-slim

WORKDIR /app

ENV EMBEDDING_PROVIDER=huggingface
ENV EMBEDDING_MODEL=sentence-transformers/all-mpnet-base-v2

COPY api/hugging_face/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY api/app.py /app/app.py

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
