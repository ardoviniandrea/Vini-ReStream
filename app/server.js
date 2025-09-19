const express = require('express');
const { spawn, exec } = require('child_process');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
// NEW: Dependencies for the pre-fetch buffer
const axios = require('axios'); // For downloading segments
const m3u8Parser = require('m3u8-parser'); // For reading playlists

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- Persistent Data Paths ---
const DATA_DIR = '/data';
const DB_PATH = path.join(DATA_DIR, 'restream.db');
const SETTINGS_PATH = path.join(DATA_DIR, 'settings.json'); // NEW: For settings
const HLS_DIR = '/var/www/hls'; // Nginx serves from here
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

// --- DB Setup ---
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('Error opening database:', err.message);
    } else {
        console.log('Connected to the SQLite database.');
        db.run(`CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )`, (err) => {
            if (err) console.error("Error creating users table:", err.message);
            else console.log("'users' table is ready.");
        });
    }
});

// --- Session Setup ---
// Use environment variable from docker-compose, with a fallback
const SESSION_SECRET = process.env.SESSION_SECRET || 'supersecretkeyforrestream';
if (SESSION_SECRET === 'supersecretkeyforrestream') {
    console.warn('[SECURITY] Using default SESSION_SECRET. Please set this in your .env file!');
}

app.use(session({
    store: new SQLiteStore({
        db: path.basename(DB_PATH),
        dir: DATA_DIR,
        table: 'sessions'
    }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 24 * 7 } // 1 week
}));

// --- Auth Middleware ---
const isAuthenticated = (req, res, next) => {
    if (req.session.userId) {
        next();
    } else {
        res.status(401).json({ error: 'Unauthorized. Please log in.' });
    }
};

// --- Stream State & Constants ---
let ffmpegProcess = null;
let currentStreamUrl = "";
let bufferManager = null; // NEW: To hold the buffer service
const VIEWER_TIMEOUT_MS = 15 * 1000; // 15 seconds
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';

// --- NEW: Settings Management (Ideas 1 & 2) ---

const DEFAULT_SETTINGS = {
    buffer: {
        enabled: false,
        delaySeconds: 30
    },
    // Define default profiles
    profiles: [
        {
            id: 'default-cpu-copy',
            name: 'CPU - Direct Copy (Default)',
            command: '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -user_agent "{userAgent}" -i "{streamUrl}" -c copy',
            isDefault: true
        },
        {
            id: 'default-gpu-nvidia',
            name: 'NVIDIA - NVENC Encode (GPU)',
            command: '-hwaccel nvdec -user_agent "{userAgent}" -i "{streamUrl}" -c:a copy -c:v h264_nvenc -preset p6 -tune hq',
            isDefault: false
        }
    ],
    activeProfileId: 'default-cpu-copy'
};

// Function to get current settings
function getSettings() {
    if (!fs.existsSync(SETTINGS_PATH)) {
        console.log('Settings file not found, creating default settings.');
        fs.writeFileSync(SETTINGS_PATH, JSON.stringify(DEFAULT_SETTINGS, null, 2));
        return DEFAULT_SETTINGS;
    }
    try {
        const settingsData = fs.readFileSync(SETTINGS_PATH, 'utf-8');
        // Simple merge to ensure new default keys are added if missing
        const savedSettings = JSON.parse(settingsData);
        const mergedSettings = { ...DEFAULT_SETTINGS, ...savedSettings };
        mergedSettings.buffer = { ...DEFAULT_SETTINGS.buffer, ...savedSettings.buffer };
        
        // Ensure default profiles exist
        DEFAULT_SETTINGS.profiles.forEach(defaultProfile => {
            if (!mergedSettings.profiles.some(p => p.id === defaultProfile.id)) {
                mergedSettings.profiles.push(defaultProfile);
            }
        });

        return mergedSettings;
    } catch (e) {
        console.error('Error reading settings.json, falling back to defaults:', e);
        return DEFAULT_SETTINGS;
    }
}

// Function to save settings
function saveSettings(settings) {
    try {
        fs.writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2));
        console.log('Settings saved successfully.');
    } catch (e) {
        console.error('Error saving settings.json:', e);
    }
}

// --- NEW: Pre-fetch Buffer Manager (Idea 1) ---

class BufferManager {
    constructor(sourceUrl, bufferSeconds) {
        this.sourceUrl = sourceUrl;
        // Calculate how many segments to hold based on an assumed segment duration (4s)
        this.targetSegmentCount = Math.max(3, Math.ceil(bufferSeconds / 4)); // Hold at least 3 segments
        this.segmentBuffer = []; // Holds filenames of segments we have
        this.processedSegments = new Set(); // Tracks all segments we've seen
        this.localPlaylistPath = path.join(HLS_DIR, 'local_playlist.m3u8');
        this.interval = null;
        this.mediaSequence = 0;
        this.isStopped = false;
        this.httpClient = axios.create({ timeout: 5000, responseType: 'stream' }); // 5s timeout
        console.log(`[Buffer] Manager started. Target buffer: ${this.targetSegmentCount} segments.`);
    }

    start() {
        this.isStopped = false;
        const tick = async () => {
            if (this.isStopped) return;
            try {
                await this.fetchAndProcessPlaylist();
            } catch (e) {
                console.error(`[Buffer] Error in tick: ${e.message}`);
            }
            if (!this.isStopped) {
                this.interval = setTimeout(tick, 2000); // Poll every 2 seconds
            }
        };
        tick(); // Start immediately
    }

    stop() {
        this.isStopped = true;
        if (this.interval) {
            clearTimeout(this.interval);
        }
        console.log('[Buffer] Manager stopped.');
        this.cleanupHlsFiles(); // Clean up everything on stop
    }

    async fetchAndProcessPlaylist() {
        // 1. Fetch the source playlist
        const response = await axios.get(this.sourceUrl, { timeout: 5000 });
        const parser = new m3u8Parser.Parser();
        parser.push(response.data);
        parser.end();

        const playlist = parser.manifest;
        if (!playlist.segments || playlist.segments.length === 0) {
            console.warn('[Buffer] Source playlist is empty or invalid.');
            return;
        }

        // 2. Download any new segments
        const segmentsToDownload = playlist.segments.filter(seg => !this.processedSegments.has(seg.uri));
        for (const segment of segmentsToDownload) {
            if (this.isStopped) break;
            const segmentFilename = path.basename(new URL(segment.uri, this.sourceUrl).pathname);
            const segmentUrl = new URL(segment.uri, this.sourceUrl).href;
            const localPath = path.join(HLS_DIR, segmentFilename);

            try {
                // Download the segment
                const segmentResponse = await this.httpClient.get(segmentUrl);
                const writer = fs.createWriteStream(localPath);
                segmentResponse.data.pipe(writer);
                await new Promise((resolve, reject) => {
                    writer.on('finish', resolve);
                    writer.on('error', reject);
                });

                // Add to our buffer ONLY after it's successfully downloaded
                this.segmentBuffer.push({ ...segment, localFile: segmentFilename });
                this.processedSegments.add(segment.uri);
                console.log(`[Buffer] Downloaded segment: ${segmentFilename}`);

            } catch (e) {
                console.error(`[Buffer] Failed to download segment ${segmentFilename}: ${e.message}`);
                // Stop processing this loop if a download fails, will retry on next tick
                break; 
            }
        }

        // 3. Prune the buffer to match the target size
        const segmentsToPurge = [];
        while (this.segmentBuffer.length > this.targetSegmentCount) {
            segmentsToPurge.push(this.segmentBuffer.shift()); // Remove from the start
        }

        // 4. Update Media Sequence
        if (this.segmentBuffer.length > 0 && segmentsToPurge.length > 0) {
            this.mediaSequence++;
        }
        if (playlist.mediaSequence && this.mediaSequence === 0) {
            this.mediaSequence = playlist.mediaSequence - this.segmentBuffer.length;
            if (this.mediaSequence < 0) this.mediaSequence = 0;
        }


        // 5. Generate the local playlist file for ffmpeg to read
        this.generateLocalPlaylist(playlist.targetDuration);

        // 6. Asynchronously delete the purged segment files from disk
        this.cleanupSegments(segmentsToPurge);
    }

    generateLocalPlaylist(targetDuration) {
        if (this.segmentBuffer.length === 0) return;

        let content = `#EXTM3U\n`;
        content += `#EXT-X-VERSION:3\n`;
        content += `#EXT-X-TARGETDURATION:${Math.ceil(targetDuration || 6)}\n`;
        content += `#EXT-X-MEDIA-SEQUENCE:${this.mediaSequence}\n`;
        
        this.segmentBuffer.forEach(segment => {
            content += `#EXTINF:${segment.duration.toFixed(6)},\n`;
            content += `${segment.localFile}\n`; // Use the local filename
        });

        // Write the local playlist that ffmpeg will consume
        fs.writeFileSync(this.localPlaylistPath, content);
    }

    cleanupSegments(segmentsToPurge) {
        segmentsToPurge.forEach(segment => {
            const localPath = path.join(HLS_DIR, segment.localFile);
            fs.unlink(localPath, (err) => {
                if (err) console.error(`[Buffer] Failed to delete old segment ${segment.localFile}: ${err.message}`);
                else console.log(`[Buffer] Cleaned up segment: ${segment.localFile}`);
            });
        });
    }

    cleanupHlsFiles() {
        // Clear all segments and playlists from the HLS directory
        fs.readdir(HLS_DIR, (err, files) => {
            if (err) return;
            for (const file of files) {
                if (file.endsWith('.ts') || file.endsWith('.m3u8')) {
                    fs.unlink(path.join(HLS_DIR, file), err => {
                        if (err) console.error(`[Cleanup] Failed to delete ${file}: ${err.message}`);
                    });
                }
            }
            console.log('[Cleanup] All HLS segments and playlists cleared.');
        });
    }
}

// --- Stream Helper Functions ---

// NEW: Spawns ffmpeg using args from the active profile
function spawnFfmpeg(sourceUrl, profileCommand) {
    if (ffmpegProcess) {
        console.log('Killing existing ffmpeg process...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }

    // Replace placeholders in the profile command
    const processedCommand = profileCommand
        .replace(/{streamUrl}/g, sourceUrl)
        .replace(/{userAgent}/g, DEFAULT_USER_AGENT); // Use a standard default UA

    // Split the profile command string into an args array
    let profileArgs = [];
    try {
        profileArgs = processedCommand.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g).map(arg => arg.replace(/^["']|["']$/g, ''));
    } catch(e) {
        console.error(`[FFmpeg] FATAL: Could not parse profile command: ${processedCommand}`);
        return;
    }

    // These are the *static* HLS output args for this project.
    const outputArgs = [
        '-f', 'hls',
        '-hls_time', '4',
        '-hls_list_size', '10',
        '-hls_flags', 'delete_segments+discont_start+omit_endlist',
        '-hls_segment_filename', `${HLS_DIR}/segment_%03d.ts`,
        `${HLS_DIR}/live.m3u8`
    ];

    const finalArgs = [...profileArgs, ...outputArgs];

    console.log(`[FFmpeg] Starting process with command: ffmpeg ${finalArgs.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', finalArgs);
    currentStreamUrl = sourceUrl; // Store the *original* source URL for the UI

    ffmpegProcess.stderr.on('data', (data) => {
        console.error(`[ffmpeg stderr]: ${data}`);
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        if (ffmpegProcess) {
            ffmpegProcess = null;
            currentStreamUrl = "";
        }
        // If the process stops (e.g., error), also stop the buffer manager
        if (bufferManager) {
            bufferManager.stop();
            bufferManager = null;
        }
    });

    ffmpegProcess.on('error', (err) => {
        console.error('[ffmpeg] Failed to start process:', err);
        ffmpegProcess = null;
        currentStreamUrl = "";
        if (bufferManager) {
            bufferManager.stop();
            bufferManager = null;
        }
    });
}

// Function to stop all streaming processes
function stopAllStreams() {
    console.log('Stopping all stream processes...');
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
        currentStreamUrl = "";
    }
    if (bufferManager) {
        bufferManager.stop(); // This will also trigger cleanup
        bufferManager = null;
    } else {
        // If buffer wasn't running, do a manual cleanup
        (new BufferManager()).cleanupHlsFiles();
    }

    // Clear logs and blocklist on stop
    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('Cleared HLS log and blocklist on stream stop.');
        reloadNginx();
    } catch (writeErr) {
        console.error('Failed to clear logs or blocklist:', writeErr);
    }
}

// Function to reload nginx config
function reloadNginx() {
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) {
            console.error('Failed to reload nginx:', stderr);
        } else {
            console.log('Nginx reloaded successfully.');
        }
    });
}


// --- Auth API Endpoints (Unchanged) ---
app.get('/api/auth/check', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (err) {
            console.error("Auth check DB error:", err);
            return res.status(500).json({ error: 'Database error' });
        }
        const hasUsers = row.count > 0;
        if (req.session.userId) {
            res.json({ loggedIn: true, username: req.session.username, hasUsers: hasUsers });
        } else {
            res.json({ loggedIn: false, hasUsers: hasUsers });
        }
    });
});

app.post('/api/auth/register', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password || password.length < 4) {
        return res.status(400).json({ error: 'Username and a password (min 4 chars) are required' });
    }
    db.get("SELECT COUNT(*) as count FROM users", async (err, row) => {
        if (err) {
            return res.status(500).json({ error: 'Database error checking users' });
        }
        const hasUsers = row.count > 0;
        if (hasUsers && !req.session.userId) {
            return res.status(403).json({ error: 'Only an admin can create new users.' });
        }
        try {
            const hashedPassword = await bcrypt.hash(password, 10);
            db.run("INSERT INTO users (username, password) VALUES (?, ?)", [username, hashedPassword], function(err) {
                if (err) {
                    if (err.message.includes('UNIQUE constraint failed')) {
                        return res.status(409).json({ error: 'Username already taken' });
                    }
                    return res.status(500).json({ error: 'Error creating user' });
                }
                const newUserId = this.lastID;
                if (!hasUsers) {
                    req.session.userId = newUserId;
                    req.session.username = username;
                }
                res.status(201).json({ message: 'User created', id: newUserId, username: username });
            });
        } catch (hashErr) {
            res.status(500).json({ error: 'Error hashing password' });
        }
    });
});

app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password are required' });
    }
    db.get("SELECT * FROM users WHERE username = ?", [username], async (err, user) => {
        if (err || !user) {
            return res.status(401).json({ error: 'Invalid username or password' });
        }
        try {
            const isMatch = await bcrypt.compare(password, user.password);
            if (isMatch) {
                req.session.userId = user.id;
                req.session.username = user.username;
                res.json({ message: 'Login successful', username: user.username });
            } else {
                return res.status(401).json({ error: 'Invalid username or password' });
            }
        } catch (compareErr) {
            return res.status(500).json({ error: "Server error during login" });
        }
    });
});

app.post('/api/auth/logout', (req, res) => {
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({ error: 'Failed to log out' });
        }
        res.clearCookie('connect.sid');
        res.json({ message: 'Logout successful' });
    });
});

// --- User Management API (Unchanged) ---
app.get('/api/users', isAuthenticated, (req, res) => {
    db.all("SELECT id, username FROM users ORDER BY username", (err, rows) => {
        if (err) {
            return res.status(500).json({ error: 'Database error fetching users' });
        }
        const otherUsers = rows.filter(u => u.id !== req.session.userId);
        res.json(otherUsers);
    });
});

app.delete('/api/users/:id', isAuthenticated, (req, res) => {
    const userIdToDelete = parseInt(req.params.id, 10);
    if (req.session.userId === userIdToDelete) {
         return res.status(400).json({ error: 'You cannot delete yourself.' });
    }
    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (err || row.count <= 1) {
            return res.status(400).json({ error: 'Cannot delete the last user.' });
        }
        db.run("DELETE FROM users WHERE id = ?", [userIdToDelete], function(err) {
            if (err || this.changes === 0) {
                return res.status(500).json({ error: 'Failed to delete user' });
            }
            res.json({ message: 'User deleted successfully' });
        });
    });
});

// --- NEW: Settings API Endpoints ---
app.get('/api/settings', isAuthenticated, (req, res) => {
    res.json(getSettings());
});

app.post('/api/settings', isAuthenticated, (req, res) => {
    const newSettings = req.body;
    // Basic validation
    if (!newSettings || !newSettings.buffer || !newSettings.profiles || !newSettings.activeProfileId) {
        return res.status(400).json({ error: 'Invalid settings object.' });
    }
    saveSettings(newSettings);
    res.json(newSettings);
});


// --- Stream API Endpoints (MODIFIED) ---

app.post('/api/start', isAuthenticated, (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'Missing "url" in request body' });
    }

    // Stop any previously running stream/buffer first
    stopAllStreams();

    try {
        const settings = getSettings();
        const activeProfile = settings.profiles.find(p => p.id === settings.activeProfileId) || settings.profiles.find(p => p.isDefault);

        if (!activeProfile) {
            console.error('FATAL: No active or default profile found. Cannot start stream.');
            return res.status(500).json({ error: 'No stream profile configured.' });
        }

        if (settings.buffer.enabled) {
            // IDEA 1: Start with Pre-fetch Buffer
            console.log('[Stream Start] Using Pre-fetch Buffer mode.');
            bufferManager = new BufferManager(url, settings.buffer.delaySeconds);
            bufferManager.start();
            
            // Give the buffer a moment to create the initial playlist
            setTimeout(() => {
                const localPlaylistUrl = `http://127.0.0.1:8994/local_playlist.m3u8`;
                spawnFfmpeg(localPlaylistUrl, activeProfile.command);
            }, 3000); // Wait 3s for first segments to likely be ready

        } else {
            // Start directly from the source URL
            console.log('[Stream Start] Using Direct mode.');
            spawnFfmpeg(url, activeProfile.command);
        }
        
        currentStreamUrl = url; // Store the original URL for the UI
        res.json({ message: 'Stream started successfully' });

    } catch (error) {
        res.status(500).json({ error: 'Failed to start stream', details: error.message });
    }
});

app.post('/api/stop', isAuthenticated, (req, res) => {
    stopAllStreams();
    res.json({ message: 'Stream stopped' });
});

app.get('/api/status', isAuthenticated, (req, res) => {
    res.json({ 
        running: (ffmpegProcess !== null),
        url: currentStreamUrl 
    });
});

// --- Viewer & Termination API (Unchanged) ---
app.get('/api/viewers', isAuthenticated, (req, res) => {
    if (!ffmpegProcess) {
        return res.json([]); 
    }
    let blockedIps = new Set();
    try {
        const blocklistLines = fs.readFileSync(BLOCKLIST_PATH, 'utf8').split('\n');
        for (const line of blocklistLines) {
            if (line.startsWith('deny ')) {
                blockedIps.add(line.substring(5, line.length - 1));
            }
        }
    } catch (readErr) {
        // Log not existing is fine
    }

    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) {
            return res.status(500).json({ error: 'Failed to read viewer log' });
        }
        const lines = data.split('\n').filter(line => line.trim() !== '');
        const viewers = new Map();
        const now = Date.now();
        const logRegex = /([\d\.:a-f]+) - \[([^\]]+)\]/; 

        for (const line of lines) {
            const match = line.match(logRegex);
            if (!match) continue;

            const ip = match[1];
            const timestampStr = match[2].replace(/\//g, ' ').replace(':', ' ');
            const timestamp = Date.parse(timestampStr);
            if (isNaN(timestamp)) continue;

            const viewer = viewers.get(ip) || { ip, firstSeen: timestamp, lastSeen: timestamp, isBlocked: blockedIps.has(ip) };
            if (timestamp > viewer.lastSeen) viewer.lastSeen = timestamp;
            if (timestamp < viewer.firstSeen) viewer.firstSeen = timestamp;
            viewer.isBlocked = blockedIps.has(ip); 
            viewers.set(ip, viewer);
        }
        const activeViewers = Array.from(viewers.values()).filter(v => (now - v.lastSeen) < VIEWER_TIMEOUT_MS);
        activeViewers.sort((a, b) => b.lastSeen - a.lastSeen);
        res.json(activeViewers);
    });
});

app.post('/api/terminate', isAuthenticated, (req, res) => {
    const { ip } = req.body;
    if (!ip) {
        return res.status(400).json({ error: 'Missing "ip"' });
    }
    try {
        const data = fs.readFileSync(BLOCKLIST_PATH, 'utf8');
        if (data.includes(`deny ${ip};`)) {
            return res.status(409).json({ message: `${ip} is already blocked.` });
        }
    } catch (e) {
        // File might not exist yet, that's fine
    }

    const blockRule = `deny ${ip};\n`;
    fs.appendFile(BLOCKLIST_PATH, blockRule, (appendErr) => {
        if (appendErr) {
            return res.status(500).json({ error: 'Failed to update blocklist' });
        }
        console.log(`Added ${ip} to blocklist. Reloading Nginx...`);
        reloadNginx();
        res.json({ message: `Successfully terminated connection for ${ip}` });
    });
});

// --- Root Handler ---
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, '127.0.0.1', () => {
    console.log(`Stream control API listening on port ${port}`);
});
