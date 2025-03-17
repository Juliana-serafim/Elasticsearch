FROM python:3.12-slim

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY main.py .
COPY README.md .

# Install UV and use --system flag to install without a virtual environment
RUN pip install uv && uv pip install --system .

# Expose the port
EXPOSE 8000

# Start the application
CMD ["python", "main.py"]