# STAGE 1: The Builder
# Use the full CUDA development image (from T104 reference) to build Node.js dependencies
FROM nvidia/cuda:12.2.2-devel-ubuntu22.04 AS builder

# Set environment to non-interactive to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Node.js (v20), curl, and build essentials
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
WORKDIR /usr/src/app_builder
COPY app/package*.json ./

# Install only production dependencies
RUN npm install --omit=dev

# Copy the rest of the application source code
COPY app/ .

# ---

# STAGE 2: The Final Production Image
# Use the smaller CUDA 'base' image for the runtime (from T104 reference)
FROM nvidia/cuda:12.2.2-base-ubuntu22.04

# Set environment variables for NVIDIA capabilities
ENV NVIDIA_DRIVER_CAPABILITIES all
ENV DEBIAN_FRONTEND=noninteractive

# Install only the necessary runtime dependencies:
# - Node.js (from node source)
# - ffmpeg, supervisor, nginx, sqlite3 (from T07 project requirements)
# - ca-certificates (crucial for HTTPS requests from Node.js)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    ffmpeg \
    nginx \
    supervisor \
    sqlite3 \
    libsqlite3-dev \
    ca-certificates && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    # Clean up apt caches to reduce final image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user 'node' and group 'node' (standard practice)
RUN addgroup --system node && adduser --system --ingroup node node

# Set the working directory for the final app
WORKDIR /usr/src/app

# Copy the application source code AND the installed node_modules from the 'builder' stage
COPY --from=builder /usr/src/app_builder .

# Copy the Nginx config
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# Copy the Supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create the HLS directory that both ffmpeg (writing) and nginx (reading) will use
# This will also hold our new buffer cache
RUN mkdir -p /var/www/hls/buffer && \
    chown -R node:node /var/www/hls

# Create /data directory for persistent database and new settings file
RUN mkdir -p /data && \
    chown -R node:node /data

# Create Nginx log dir, hls_access.log, and blocklist.conf
RUN mkdir -p /var/log/nginx && \
    touch /var/log/nginx/hls_access.log && \
    touch /etc/nginx/blocklist.conf && \
    # Give node user permission to read the log and write to the blocklist
    chown -R node:node /var/log/nginx /etc/nginx/blocklist.conf

# Create supervisor socket dir and give node group access (required for supervisorctl in server.js)
RUN mkdir -p /var/run/supervisor && \
    chgrp node /var/run/supervisor && \
    chmod 770 /var/run/supervisor

# Switch to the non-root user
USER node

# Expose the new ports
EXPOSE 8995
EXPOSE 8994

# Start supervisord to run both Node.js and Nginx
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
