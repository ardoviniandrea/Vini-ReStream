# Stage 1: The Builder (For Node.js dependencies compilation)
FROM nvidia/cuda:12.2.2-devel-ubuntu22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install Node.js and build essentials (needed for node-gyp dependencies like sqlite3)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gnupg \
    python3 \
    libsqlite3-dev \
    pkg-config && \
    # Install Node.js 20.x from NodeSource
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory and copy package files
WORKDIR /usr/src/app
COPY app/package*.json ./

# Install all dependencies for the app
RUN npm install

# Copy all app source code so it's included in the builder stage
COPY app/ .

# ---
# Stage 2: The Final Runtime Image
# Use a smaller CUDA 'base' image for the runtime environment.
FROM nvidia/cuda:12.2.2-base-ubuntu22.04

ENV NVIDIA_DRIVER_CAPABILITIES all
ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies: Node.js, FFmpeg, Nginx, Supervisor, SQLite, and ca-certs
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    ffmpeg \
    nginx \
    supervisor \
    sqlite3 \
    ca-certificates && \
    # Re-install Node.js runtime environment (using the correct method from original file)
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create and set the working directory
WORKDIR /usr/src/app

# Copy the application files and node_modules from the 'builder' stage
COPY --from=builder /usr/src/app .

# Copy Nginx and Supervisor configs
COPY nginx/nginx.conf /etc/nginx/nginx.conf
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create directories for HLS, logs, and persistent data
RUN mkdir -p /var/www/hls && \
    mkdir -p /data && \
    mkdir -p /var/log/nginx && \
    touch /var/log/nginx/hls_access.log && \
    touch /etc/nginx/blocklist.conf

# Expose both ports (UI/API on 8995, Stream on 8994)
EXPOSE 8995
EXPOSE 8994

# Start supervisord as the main command (as root, required for Nginx/Supervisor)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
