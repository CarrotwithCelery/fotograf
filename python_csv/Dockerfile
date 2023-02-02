# Use an existing Python image as the base image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the contents of the current directory to the container's working directory
COPY . .

# Install the required dependencies
RUN pip install -r requirements.txt

# Run the Python script
CMD ["python", "csv_loader.py"]