FROM python:3.9-slim

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY main.py .
COPY README.md .

# Install UV and project dependencies using uv's pip subcommand
RUN pip install uv && uv pip install

# Expose the port (adjust if needed)
EXPOSE 8000

# Start the application using UV
CMD ["uv", "run"]
