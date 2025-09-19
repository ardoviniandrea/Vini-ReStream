# Stage 1: The Builder (Based on your T104 reference for CUDA)
# Use the full CUDA development image to build dependencies.
FROM nvidia/cuda:12.2.2-devel-ubuntu22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install Node.js and build essentials
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory and copy package files
WORKDIR /usr/src/app
COPY app/package*.json ./

# Install all dependencies for the app
RUN npm install

# Copy all app source code
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
# Give ownership to the 'node' user for directories that the node app *might* need
# but the main processes will run as root.
RUN mkdir -p /var/www/hls && \
    mkdir -p /data && \
    mkdir -p /var/log/nginx && \
    touch /var/log/nginx/hls_access.log && \
    touch /etc/nginx/blocklist.conf

# Expose the ports
EXPOSE 8995
EXPOSE 8994

# NOTE: We are NOT setting 'USER node' here.
# The container will run as ROOT, which is required for Supervisor
# to launch Nginx and for the Node process to write to the /data volume.

# Start supervisord as the main command (as root)
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

