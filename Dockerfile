FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and data
COPY . .

# Build the SQLite database during image creation to avoid volume issues
RUN python build_db.py && rm -f *.zip

EXPOSE 8501 8000

# Default: run Streamlit UI
CMD ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
