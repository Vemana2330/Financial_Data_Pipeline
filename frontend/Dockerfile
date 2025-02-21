# Use Python 3.11.7 as base image
FROM python:3.11.7

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY ./frontend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Streamlit application code
COPY ./frontend/app.py .

# Ensure Streamlit config directory exists
RUN mkdir -p /root/.streamlit

# Copy the Streamlit configuration file
COPY ./frontend/config.toml /root/.streamlit/config.toml

# Expose the port Streamlit runs on
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
