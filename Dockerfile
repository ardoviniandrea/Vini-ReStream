FROM node:20-alpine

# Install dependencies: ffmpeg (for streaming), nginx (for serving), supervisor (for process management)
RUN apk add --no-cache ffmpeg nginx supervisor

# Set up the working directory for the Node.js app
WORKDIR /usr/src/app

# Install Node.js app dependencies
COPY app/package*.json ./
RUN npm install

# Copy all app source code
COPY app/ .

# Copy the Nginx config
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# Copy the Supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create the HLS directory that both ffmpeg (writing) and nginx (reading) will use
RUN mkdir -p /var/www/hls && \
    # Give the node user (which npm runs as) permission to write to it
    chown -R node:node /var/www/hls

# Expose the new ports
EXPOSE 8995
EXPOSE 8994

# Start supervisord to run both Node.js and Nginx
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
