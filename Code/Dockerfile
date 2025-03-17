FROM python:3.9-slim

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY main.py .
COPY README.md .

# Install UV globally and then install project dependencies using uv CLI
RUN pip install uv && uv install

# Expose port 8000 (adjust if needed)
EXPOSE 8000

# Start the application using UV
CMD ["uv", "run"]
