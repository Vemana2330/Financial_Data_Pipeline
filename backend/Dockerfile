# Use Python 3.11.7 as base image
FROM python:3.11.7

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY ./backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application code
COPY ./backend /app/backend

# Expose the port FastAPI runs on
EXPOSE 8000

# Run FastAPI app using Uvicorn
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
