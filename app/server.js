const express = require('express');
const { spawn } = require('child_process');
const cors = require('cors');
const path = require('path');

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

// Function to start ffmpeg
function startStream(streamUrl) {
    if (ffmpegProcess) {
        console.log('Killing existing ffmpeg process...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }

    console.log(`Starting stream from: ${streamUrl}`);
    
    // --- MODIFIED ---
    // Updated path to /var/www/hls to match the Nginx config
    const args = [
        '-i', streamUrl,
        '-c', 'copy', // Copy codec, no re-encoding
        '-f', 'hls',
        '-hls_time', '4', // 4-second segments
        '-hls_list_size', '10', // Keep 10 segments in playlist
        '-hls_flags', 'delete_segments', // Delete old segments
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

// --- API Endpoints ---

app.post('/api/start', (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'Missing "url" in request body' });
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

// Serve the index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
    // Listens on localhost only, Nginx will handle public traffic
    console.log(`Stream control API listening on port ${port}`);
});

