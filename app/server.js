const express = require('express');
const { spawn, exec } = require('child_process'); // Import exec
const cors = require('cors');
const path = require('path');
const fs = require('fs'); // Import fs

const app = express();
// The Node app now listens on port 3000 *internally*
// Nginx will proxy requests to it
const port = 3000;

app.use(cors());
app.use(express.json());
// Serve static files from 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

let ffmpegProcess = null;
let currentStreamUrl = "";

// --- NEW ---
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';
const VIEWER_TIMEOUT_MS = 15 * 1000; // 15 seconds (a viewer is "inactive" if no request for 15s)

// Function to start ffmpeg
function startStream(streamUrl) {
    if (ffmpegProcess) {
        console.log('Killing existing ffmpeg process...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }

    console.log(`Starting stream from: ${streamUrl}`);
    
    // --- MODIFIED FFMPEG ARGS ---
    const args = [
        // --- NEW ---
        // Flags to make the HTTP input more resilient to network drops
        '-reconnect', '1',
        '-reconnect_streamed', '1',
        '-reconnect_delay_max', '5',

        // Input URL
        '-i', streamUrl,

        // Copy codec, no re-encoding
        '-c', 'copy', 
        
        // HLS Output flags
        '-f', 'hls',
        '-hls_time', '4', // 4-second segments
        '-hls_list_size', '10', // Keep 10 segments in playlist

        // --- MODIFIED ---
        // 'delete_segments': Delete old segments
        // 'discont_start': Adds a discontinuity tag when timestamps jump (fixes skipping)
        // 'omit_endlist': Ensures the stream is always treated as "live"
        '-hls_flags', 'delete_segments+discont_start+omit_endlist', 

        '-hls_segment_filename', '/var/www/hls/segment_%03d.ts',
        '/var/www/hls/live.m3u8' // The output playlist
    ];

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = streamUrl;

    ffmpegProcess.stdout.on('data', (data) => {
        // You can uncomment this for detailed logging
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        console.error(`ffmpeg stderr: ${data}`);
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`ffmpeg process exited with code ${code}`);
        if (ffmpegProcess) {
            // Process exited unexpectedly
            ffmpegProcess = null;
            currentStreamUrl = "";
        }
    });

    ffmpegProcess.on('error', (err) => {
        console.error('Failed to start ffmpeg process:', err);
        ffmpegProcess = null;
        currentStreamUrl = "";
    });
}

// Function to reload nginx config
function reloadNginx() {
    // --- MODIFIED ---
    // Specify the correct config file path for supervisorctl
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) {
            console.error('Failed to reload nginx:', stderr);
        } else {
            console.log('Nginx reloaded successfully.');
        }
    });
}

// --- API Endpoints ---

app.post('/api/start', (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'Missing "url" in request body' });
    }

    // --- NEW ---
    // Clear old logs and blocklist when a new stream starts
    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('Cleared HLS log and blocklist for new stream.');
        reloadNginx(); // Reload nginx to apply empty blocklist
    } catch (writeErr) {
        console.error('Failed to clear logs or blocklist:', writeErr);
    }

    try {
        startStream(url);
        res.json({ message: 'Stream started successfully' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to start stream', details: error.message });
    }
});

app.post('/api/stop', (req, res) => {
    if (ffmpegProcess) {
        console.log('Stopping stream via API request...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
        currentStreamUrl = "";

        // --- NEW ---
        // Clear logs and blocklist on stop
        try {
            fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
            fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
            console.log('Cleared HLS log and blocklist on stream stop.');
            reloadNginx(); // Reload nginx to apply empty blocklist
        } catch (writeErr) {
            console.error('Failed to clear logs or blocklist:', writeErr);
        }
        
        res.json({ message: 'Stream stopped' });
    } else {
        res.json({ message: 'Stream not running' });
    }
});

app.get('/api/status', (req, res) => {
    // Report if the process is running and what URL it's using
    res.json({ 
        running: (ffmpegProcess !== null),
        url: currentStreamUrl 
    });
});


// --- MODIFIED ENDPOINT: /api/viewers ---
app.get('/api/viewers', (req, res) => {
    if (!ffmpegProcess) {
        return res.json([]); // No stream running, no viewers
    }

    // First, read the blocklist
    let blockedIps = new Set();
    try {
        const blocklistData = fs.readFileSync(BLOCKLIST_PATH, 'utf8');
        const blocklistLines = blocklistData.split('\n');
        for (const line of blocklistLines) {
            if (line.startsWith('deny ')) {
                blockedIps.add(line.substring(5, line.length - 1)); // 'deny 1.2.3.4;' -> '1.2.3.4'
            }
        }
    } catch (readErr) {
        console.error('Failed to read blocklist, continuing without it.', readErr);
    }

    // Now, read the access log
    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) {
            console.error('Failed to read HLS log:', err);
            return res.status(500).json({ error: 'Failed to read viewer log' });
        }

        const lines = data.split('\n').filter(line => line.trim() !== '');
        const viewers = new Map();
        const now = Date.now();

        // Nginx log format: 1.2.3.4 - [18/Sep/2025:15:41:00 +0200]
        const logRegex = /([\d\.:a-f]+) - \[([^\]]+)\]/; // Support IPv4 and IPv6

        for (const line of lines) {
            const match = line.match(logRegex);
            if (!match) continue;

            const ip = match[1];
            // Convert Nginx time '18/Sep/2025:15:41:00 +0200' to a format Date.parse() likes
            // '18 Sep 2025 15:41:00 +0200'
            const timestampStr = match[2].replace('/', ' ').replace('/', ' ').replace(':', ' ');
            const timestamp = Date.parse(timestampStr);

            if (isNaN(timestamp)) {
                console.warn(`Could not parse timestamp: ${match[2]}`);
                continue;
            }

            // Update viewer's last seen time
            const viewer = viewers.get(ip) || { 
                ip, 
                firstSeen: timestamp, 
                lastSeen: timestamp,
                isBlocked: blockedIps.has(ip) // Check if IP is in the set
            };

            if (timestamp > viewer.lastSeen) {
                viewer.lastSeen = timestamp;
            }
            if (timestamp < viewer.firstSeen) {
                viewer.firstSeen = timestamp;
            }
            // Ensure isBlocked status is up-to-date
            viewer.isBlocked = blockedIps.has(ip); 
            viewers.set(ip, viewer);
        }

        // Filter for active viewers
        const activeViewers = Array.from(viewers.values()).filter(v => 
            (now - v.lastSeen) < VIEWER_TIMEOUT_MS
        );

        // Sort by most recent
        activeViewers.sort((a, b) => b.lastSeen - a.lastSeen);

        res.json(activeViewers);
    });
});

// --- NEW ENDPOINT: /api/terminate ---
app.post('/api/terminate', (req, res) => {
    const { ip } = req.body;
    if (!ip) {
        return res.status(400).json({ error: 'Missing "ip" in request body' });
    }

    // Check if IP is already blocked to avoid duplicates
    fs.readFile(BLOCKLIST_PATH, 'utf8', (readErr, data) => {
        if (readErr) {
            console.error('Failed to read blocklist:', readErr);
            return res.status(500).json({ error: 'Failed to read blocklist' });
        }

        if (data.includes(`deny ${ip};`)) {
            // This is the 409 Conflict you were seeing
            return res.status(409).json({ message: `${ip} is already blocked.` });
        }

        const blockRule = `deny ${ip};\n`;
        fs.appendFile(BLOCKLIST_PATH, blockRule, (appendErr) => {
            if (appendErr) {
                console.error('Failed to append to blocklist:', appendErr);
                return res.status(500).json({ error: 'Failed to update blocklist' });
            }

            console.log(`Added ${ip} to blocklist. Reloading Nginx...`);
            reloadNginx(); // Reload Nginx to apply the new rule
            res.json({ message: `Successfully terminated connection for ${ip}` });
        });
    });
});


// Serve the index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, '127.0.0.1', () => {
    // Listens on localhost only, Nginx will handle public traffic
    console.log(`Stream control API listening on port ${port}`);
});
