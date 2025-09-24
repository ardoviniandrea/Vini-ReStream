# Use the official NVIDIA CUDA base image for potential GPU acceleration
# Note: For production, you should use a more minimal image like ubuntu:latest or debian:stable 
# if you do not require CUDA/FFmpeg or specialized libraries.
FROM nvidia/cuda:12.4.0-devel-ubuntu22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV NODE_VERSION=18

# Install dependencies (Node.js, FFmpeg, Nginx, Supervisor)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    git \
    build-essential \
    ffmpeg \
    nginx \
    supervisor \
    libssl-dev \
    zlib1g-dev \
    libsqlite3-dev \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN set -ex \
    && ARCH= \
    && case "$(dpkg --print-architecture)" in \
      amd64) ARCH='x64' ;; \
      arm64) ARCH='arm64' ;; \
      *) echo 'unsupported arch' && exit 1 ;; \
    esac \
    && cd /tmp \
    && curl -fsSLO --compressed "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${ARCH}.tar.xz" \
    && tar -xJf "node-v${NODE_VERSION}-linux-${ARCH}.tar.xz" -C /usr/local --strip-components=1 --no-same-owner \
    && rm "node-v${NODE_VERSION}-linux-${ARCH}.tar.xz" \
    && ln -s /usr/local/bin/node /usr/local/bin/nodejs

# --- APPLICATION SETUP ---
# Create directory for HLS segments (served by Nginx)
RUN mkdir -p /var/www/hls

# Create directory for Node.js app
WORKDIR /app

# Copy application files
COPY app/package.json .
COPY app/server.js .
COPY app/public ./public
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY nginx/nginx.conf /etc/nginx/sites-available/default
RUN ln -sf /etc/nginx/sites-available/default /etc/nginx/sites-enabled/default \
    && rm -f /etc/nginx/sites-enabled/default.conf

# Create a place for persistent data (database, settings)
RUN mkdir -p /data
VOLUME /data

# Create log directories for Nginx HLS access and blocklist
RUN mkdir -p /var/log/nginx \
    && touch /var/log/nginx/hls_access.log \
    && touch /etc/nginx/blocklist.conf

# Install Node.js dependencies (CRITICAL: Added after package.json copy)
RUN npm install

# Expose both Nginx stream port (8994) and API port (8995)
EXPOSE 8994
EXPOSE 8995

# Start supervisor which will run Nginx and the Node.js server
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
