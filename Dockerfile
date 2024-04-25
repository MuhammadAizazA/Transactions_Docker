FROM python:3.11-slim

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/MuhammadAizazA/Transactions_Docker.git .

RUN pip3 install -r requirements.txt

# Expose the port that the application listens on.
EXPOSE 8000

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Run the application.
CMD uvicorn 'main:app' --host=0.0.0.0 --port=8000