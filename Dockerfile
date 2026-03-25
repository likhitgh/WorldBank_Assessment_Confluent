# Use an official, lightweight Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements and install them
COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts into the container
COPY scripts/ /app/scripts/

CMD ["python", "scripts/main.py"]