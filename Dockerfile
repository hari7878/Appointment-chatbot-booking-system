# Dockerfile

# Start with an official, slim Python base image for a smaller final image size
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements file first to leverage Docker's layer caching.
# This layer will only be rebuilt if requirements.txt changes.
COPY requirements.txt .

# Install all Python dependencies from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's source code into the container's working directory
COPY . .
COPY synthea_fhir_data.db /app/synthea_fhir_data.db
# Inform Docker that the container listens on port 8501 at runtime
EXPOSE 8501

# Define the command to run your Streamlit app when the container starts.
# The --server.address=0.0.0.0 is crucial to make the app accessible from outside the container.
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
