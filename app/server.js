const express = require('express');
const { spawn, exec } = require('child_process');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');

// --- NEW DEPENDENCIES ---
// We need axios for reliable HTTP requests (to download segments) and m3u8-parser
const axios = require('axios');
const { Parser } = require('m3u8-parser');

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- DB Setup ---
const DATA_DIR = '/data'; // This is the persistent volume mounted by Docker
const DB_PATH = path.join(DATA_DIR, 'restream.db');
const SETTINGS_PATH = path.join(DATA_DIR, 'settings.json'); // --- NEW: Settings file ---
let db;

try {
    if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR);
    }
    
    db = new sqlite3.Database(DB_PATH, (err) => {
        if (err) {
            console.error('Error opening database:', err.message);
            // If the DB can't be opened, the app is useless.
            process.exit(1);
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
} catch (dirErr) {
    console.error("Failed to create or access /data directory.", dirErr);
    process.exit(1);
}

// --- Session Setup ---
const SESSION_SECRET = process.env.SESSION_SECRET || 'supersecretkeyforrestream';
if (SESSION_SECRET === 'supersecretkeyforrestream') {
    console.warn("WARNING: Using default SESSION_SECRET. Please set this in your docker-compose.yml or .env file for production.");
}

app.use(session({
    store: new SQLiteStore({
        db: 'restream.db',
        dir: DATA_DIR,
        table: 'sessions'
    }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 24 * 7 }
}));

// --- Auth Middleware ---
const isAuthenticated = (req, res, next) => {
    if (req.session.userId) {
        next();
    } else {
        res.status(401).json({ error: 'Unauthorized. Please log in.' });
    }
};

// ================================================================
// --- NEW: SETTINGS MANAGEMENT ---
// ================================================================

function getDefaultSettings() {
    return {
        profiles: [
            {
                id: 'default-cpu',
                name: 'Default (CPU Stream Copy - HLS Only)', // Renamed for clarity
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: true,
                isDefault: true // --- FIX: Add isDefault flag
            },
            {
                id: 'nvidia-gpu',
                name: 'NVIDIA (NVENC Re-encode - HLS Only)', // Renamed for clarity
                command: '-hwaccel nvdec -user_agent "{userAgent}" -i "{streamUrl}" -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true // --- FIX: Add isDefault flag
            },
            // --- NEW: MPD/DASH Profiles based on user logs ---
            // --- UPDATED: Added reconnect flags to all MPD profiles ---
            {
                id: 'mpd-1080p-copy',
                name: 'MPD/DASH 1080p (Stream Copy)',
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -map 0:4 -map 0:5 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            },
            {
                id: 'mpd-720p-copy',
                name: 'MPD/DASH 720p (Stream Copy)',
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -map 0:3 -map 0:5 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            },
            {
                id: 'mpd-1080p-nvenc',
                name: 'MPD/DASH 1080p (NVIDIA NVENC)',
                command: '-hwaccel nvdec -user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -map 0:4 -map 0:5 -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            }
        ],
        buffer: {
            enabled: true,
            delaySeconds: 30
        }
    };
}


function getSettings() {
    if (!fs.existsSync(SETTINGS_PATH)) {
        console.log('Settings file not found, creating default settings.');
        const defaults = getDefaultSettings();
        try {
            fs.writeFileSync(SETTINGS_PATH, JSON.stringify(defaults, null, 2));
            return defaults;
        } catch (e) {
            console.error("Failed to write default settings:", e);
            return getDefaultSettings(); // Return from memory
        }
    }
    try {
        const settingsData = fs.readFileSync(SETTINGS_PATH, 'utf8');
        let settings = JSON.parse(settingsData);

        // --- NEW: Migration logic to add isDefault flag to old profiles ---
        let needsSave = false;
        if (settings.profiles && settings.profiles.length > 0) {
            const defaultIds = ['default-cpu', 'nvidia-gpu', 'mpd-1080p-copy', 'mpd-720p-copy', 'mpd-1080p-nvenc'];
            // --- FIX: Add reconnect flags to any existing MPD profiles that are missing them ---
            for (const profile of settings.profiles) {
                if (defaultIds.includes(profile.id) && profile.isDefault !== true) {
                    profile.isDefault = true;
                    needsSave = true;
                } else if (!defaultIds.includes(profile.id) && profile.isDefault !== false) {
                    profile.isDefault = false;
                    needsSave = true;
                }
                
                if (profile.id.startsWith('mpd-') && !profile.command.includes('-reconnect 1')) {
                    console.log(`Upgrading profile "${profile.name}" with reconnect flags.`);
                    profile.command = profile.command.replace('-i "{streamUrl}"', '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}"');
                    needsSave = true;
                }
            }
        }
        if (needsSave) {
            console.log('Migrating settings to add flags...');
            saveSettings(settings);
        }
        // --- End Migration ---

        return settings;
    } catch (e) {
        console.error("Failed to parse settings.json, returning defaults:", e);
        return getDefaultSettings(); // Return defaults if parsing fails
    }
}


function saveSettings(settings) {
    try {
        fs.writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2));
        console.log('Settings saved successfully.');
        return true;
    } catch (e) {
        console.error("Failed to save settings:", e);
        return false;
    }
}

function getActiveProfile() {
    const settings = getSettings();
    // --- MODIFIED: Find active profile by ID from settings ---
    let activeProfile = settings.profiles.find(p => p.id === settings.activeProfileId);
    if (!activeProfile) {
        // Fallback if activeId is invalid or not set
        activeProfile = settings.profiles.find(p => p.active === true);
    }
    if (!activeProfile) {
        // Fallback to first profile
        activeProfile = settings.profiles[0];
    }
    return activeProfile || getDefaultSettings().profiles[0]; // Absolute fallback
}


// --- NEW: Settings & Profiles API Endpoints (Protected) ---

app.get('/api/settings', isAuthenticated, (req, res) => {
    res.json(getSettings());
});

app.post('/api/settings', isAuthenticated, (req, res) => {
    const newSettings = req.body;
    if (!newSettings || !newSettings.profiles || !newSettings.buffer) {
        return res.status(400).json({ error: 'Invalid settings object.' });
    }

    // --- MODIFIED: We now save the *ID* of the active profile ---
    // The 'active' boolean on the profile itself is deprecated, but we keep
    // it for one last check to find the ID.
    let activeId = newSettings.activeProfileId;
    if (!activeId) {
        const activeProfile = newSettings.profiles.find(p => p.active === true);
        activeId = activeProfile ? activeProfile.id : newSettings.profiles[0]?.id;
    }
    
    // Ensure the activeId is valid
    if (!newSettings.profiles.find(p => p.id === activeId)) {
        activeId = newSettings.profiles[0]?.id; // Default to first if not found
    }
    
    newSettings.activeProfileId = activeId;
    
    // Clean up old 'active' flags (they are no longer the source of truth)
    newSettings.profiles.forEach(p => {
        p.active = (p.id === activeId);
    });
    
    if (saveSettings(newSettings)) {
        // --- FIX: Return the full settings object so the UI stays in sync ---
        res.json(newSettings);
    } else {
        res.status(500).json({ error: 'Failed to save settings to disk.' });
    }
});



// ================================================================
// --- STREAM STATE & HELPERS ---
// ================================================================

let ffmpegProcess = null;
let currentStreamUrl = "";
let bufferManager = null; // --- NEW: Handle for the buffer manager
let manifestRefresher = null; // --- NEW: Handle for the MPD manifest refresher ---
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';
const VIEWER_TIMEOUT_MS = 15 * 1000;

// Function to reload nginx config
function reloadNginx() {
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) console.error('Failed to reload nginx:', stderr);
        else console.log('Nginx reloaded successfully.');
    });
}

// Function to clean up all HLS files and local playlists
function cleanupHlsFiles() {
    console.log('[Cleanup] All HLS segments and playlists cleared.');
    try {
        // Delete main nginx-served playlist
        if (fs.existsSync('/var/www/hls/live.m3u8')) {
            fs.unlinkSync('/var/www/hls/live.m3u8');
        }
        // Delete all .ts segments
        fs.readdirSync('/var/www/hls').forEach(file => {
            if (file.endsWith('.ts')) {
                fs.unlinkSync(path.join('/var/www/hls', file));
            }
        });
        // Delete our internally generated local playlist
        if (fs.existsSync('/var/www/hls/local_playlist.m3u8')) {
            fs.unlinkSync('/var/www/hls/local_playlist.m3u8');
        }
        // --- NEW: Delete local MPD manifest ---
        if (fs.existsSync('/var/www/hls/local_manifest.mpd')) {
            fs.unlinkSync('/var/www/hls/local_manifest.mpd');
        }
    } catch (e) {
        console.error('[Cleanup] Error during HLS file cleanup:', e.message);
    }
}

// Function to stop all streaming processes
function stopAllStreamProcesses() {
    console.log('Stopping all stream processes...');
    if (bufferManager) {
        bufferManager.stop();
        bufferManager = null;
    }
    // --- NEW: Stop the manifest refresher ---
    if (manifestRefresher) {
        manifestRefresher.stop();
        manifestRefresher = null;
    }
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }
    currentStreamUrl = "";
    cleanupHlsFiles(); // Clean up files on any stop
}


// ================================================================
// --- NEW: MANIFEST REFRESHER (FOR .MPD STREAMS) ---
// ================================================================

class ManifestRefresher {
    constructor(sourceUrl, userAgent) {
        this.sourceUrl = sourceUrl;
        this.userAgent = userAgent;
        this.localManifestPath = path.join('/var/www/hls', 'local_manifest.mpd');
        this.refreshIntervalMs = 20 * 1000; // Refresh every 20 seconds
        this.intervalId = null;
        this.stopFlag = false;
        console.log(`[Refresher] Manifest Refresher started. Will refresh every ${this.refreshIntervalMs / 1000}s.`);
    }

    async refreshManifest() {
        if (this.stopFlag) return;
        console.log('[Refresher] Fetching fresh manifest...');
        try {
            const response = await axios.get(this.sourceUrl, {
                timeout: 5000,
                headers: { 'User-Agent': this.userAgent }
            });
            
            // --- NEW: Rewrite manifest to use absolute segment URLs ---
            // This is crucial. The original manifest has relative URLs.
            // We must rewrite them to be absolute so FFmpeg knows where to get them.
            const manifestData = response.data;
            const baseUrl = this.sourceUrl.substring(0, this.sourceUrl.lastIndexOf('/') + 1);
            
            // This regex finds <BaseURL> tags and replaces/adds them.
            // It also finds SegmentTemplate media attributes and prepends the base URL.
            let rewrittenData = manifestData
                .replace(/<BaseURL>.*<\/BaseURL>/g, `<BaseURL>${baseUrl}</BaseURL>`) // Replace existing BaseURL
                .replace(/<SegmentTemplate(.*?)media="([^"]*?)"/g, `<SegmentTemplate$1media="${baseUrl}$2"`); // Fix segment templates

            // If no BaseURL was present, add one at the top.
            if (!rewrittenData.includes('<BaseURL>')) {
                 rewrittenData = rewrittenData.replace(/<Period>/, `<Period>\n<BaseURL>${baseUrl}</BaseURL>`);
            }

            fs.writeFileSync(this.localManifestPath, rewrittenData);
            console.log('[Refresher] Fresh manifest saved successfully.');
            
            return this.localManifestPath;

        } catch (error) {
            console.error('[Refresher] Error fetching source manifest:', error.message);
            // Don't stop, just retry on the next interval
        }
    }

    async start() {
        // Do the first fetch immediately and wait for it
        try {
            await this.refreshManifest();
            // Now start the timer for subsequent refreshes
            this.intervalId = setInterval(() => this.refreshManifest(), this.refreshIntervalMs);
            return this.localManifestPath;
        } catch (e) {
            console.error('[Refresher] CRITICAL: Failed first manifest fetch. Aborting stream start.');
            throw e; // Throw error to stop the stream from starting
        }
    }

    stop() {
        this.stopFlag = true;
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
        console.log('[Refresher] Manifest Refresher stopped.');
        // Clean up the local manifest file
        try {
            if (fs.existsSync(this.localManifestPath)) {
                fs.unlinkSync(this.localManifestPath);
            }
        } catch (e) {
            console.error('[Refresher] Failed to delete local manifest on stop:', e);
        }
    }
}


// ================================================================
// --- PRE-FETCH BUFFER MANAGER (FOR .M3U8 STREAMS) ---
// ================================================================

class BufferManager {
    constructor(sourceUrl, bufferSeconds) {
        this.sourceUrl = sourceUrl;
        this.targetBufferSegments = Math.max(1, Math.floor(bufferSeconds / 4)); // Assuming avg 4s segments
        this.bufferDir = path.join('/var/www/hls', 'buffer'); // <<< MODIFIED: Write directly into nginx dir
        this.localPlaylistPath = path.join('/var/www/hls', 'local_playlist.m3u8'); // Nginx serves this
        this.segmentQueue = [];    // List of segment filenames (e.g., "seg-101.ts")
        this.downloadedSegments = new Set();
        this.segmentBaseUrl = '';
        this.stopFlag = false;
        this.timeoutId = null;
        this.initialPlaylistReady = null; // --- FIX: Promise for race condition
        this.resolveInitialPlaylist = null;
        this.rejectInitialPlaylist = null;

        console.log(`[Buffer] Manager started. Target buffer: ${this.targetBufferSegments} segments.`);

        // Ensure buffer directory exists and is clean
        try {
            if (fs.existsSync(this.bufferDir)) {
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
            fs.mkdirSync(this.bufferDir, { recursive: true }); // <<< MODIFIED: Added recursive
        } catch (e) {
            console.error('[Buffer] Failed to create or clean buffer directory:', e);
        }
    }

    /**
     * Public start method. Returns a promise that resolves when the first playlist is ready.
     */
    start() {
        this.initialPlaylistReady = new Promise((resolve, reject) => {
            this.resolveInitialPlaylist = resolve;
            this.rejectInitialPlaylist = reject;
        });

        this.fetchPlaylist(); // Start the loop
        return this.initialPlaylistReady; // Return the promise
    }

    stop() {
        this.stopFlag = true;
        if (this.timeoutId) {
            clearTimeout(this.timeoutId);
        }
        console.log('[Buffer] Manager stopped.');
        // Clean up temporary buffer dir on stop
        try {
            if (fs.existsSync(this.bufferDir)) {
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
        } catch (e) {
            console.error('[Buffer] Failed to delete buffer directory on stop:', e);
        }
    }

    async fetchPlaylist() {
        if (this.stopFlag) return;

        try {
            const response = await axios.get(this.sourceUrl, { timeout: 3000 });
            const parser = new Parser();
            parser.push(response.data);
            parser.end();

            const playlist = parser.manifest;
            if (!playlist.segments || playlist.segments.length === 0) {
                console.warn('[Buffer] Source playlist is empty or invalid.');
                if (this.rejectInitialPlaylist) { // FIX: Reject the promise if it's still pending
                    this.rejectInitialPlaylist(new Error("Source playlist is empty or invalid."));
                    this.rejectInitialPlaylist = null; // Ensure it only fires once
                }
                throw new Error("Empty or invalid playlist");
            }

            // Determine the base URL for segments (relative vs absolute)
            const firstSegmentUri = playlist.segments[0].uri;
            if (firstSegmentUri.startsWith('http')) {
                this.segmentBaseUrl = ''; // Segments have full URLs
            } else {
                // Segments are relative. Construct base URL from main playlist URL.
                const urlObj = new URL(this.sourceUrl);
                urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
                this.segmentBaseUrl = urlObj.toString();
            }

            // Get *full URIs* for segments
            const segmentUris = playlist.segments.map(s => {
                // If segmentBaseUrl is empty, the URI is already absolute
                return this.segmentBaseUrl ? new URL(s.uri, this.segmentBaseUrl).href : s.uri;
            });
            this.segmentQueue = segmentUris; // Update queue with FULL URLs

            // Start downloading segments from the queue
            this.downloadSegments(playlist.segments); // Pass original segments for filenames
            // Write the local playlist for FFmpeg to read from
            this.writeLocalPlaylist(playlist);

            // FIX: Resolve the promise only AFTER the first playlist is written
            if (this.resolveInitialPlaylist) {
                console.log('[Buffer] Initial playlist is ready.');
                this.resolveInitialPlaylist({ localPlaylistPath: this.localPlaylistPath });
                this.resolveInitialPlaylist = null; // Ensure it only fires once
                this.rejectInitialPlaylist = null;
            }

            // Schedule the next fetch based on segment duration
            const refreshInterval = (playlist.segments[0].duration || 4) * 1000;
            this.timeoutId = setTimeout(() => this.fetchPlaylist(), refreshInterval / 2); // Refresh at half duration

        } catch (error) {
            console.error('[Buffer] Error fetching source playlist:', error.message);
            if (!this.stopFlag) {
                this.timeoutId = setTimeout(() => this.fetchPlaylist(), 2000); // Retry faster on error
            }
            // FIX: If we fail *before* the first playlist is ready, reject the promise
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(error);
                this.rejectInitialPlaylist = null;
                this.resolveInitialPlaylist = null;
            }
        }
    }

    async downloadSegments(originalSegments) {
        // Use the full URI queue to download, but the original segments for filenames
        for (let i = 0; i < this.segmentQueue.length; i++) {
            const segmentUrl = this.segmentQueue[i];
            const filename = originalSegments[i].uri.split('/').pop();
            
            if (this.stopFlag) return;
            if (!this.downloadedSegments.has(filename)) {
                const localPath = path.join(this.bufferDir, filename);

                try {
                    const response = await axios.get(segmentUrl, { responseType: 'stream', timeout: 5000 });
                    const writer = fs.createWriteStream(localPath);
                    response.data.pipe(writer);
                    
                    await new Promise((resolve, reject) => {
                        writer.on('finish', resolve);
                        writer.on('error', reject);
                    });

                    this.downloadedSegments.add(filename);
                } catch (error) {
                    console.warn(`[Buffer] Failed to download segment ${filename}:`, error.message);
                    break; 
                }
            }
        }
        // Clean up old segments
        this.cleanupOldSegments(originalSegments.map(s => s.uri.split('/').pop()));
    }

    writeLocalPlaylist(playlist) {
        // This creates a new .m3u8 file that points to our locally buffered segments.
        // These segments are in /var/www/hls/buffer/
        
        // Filter playlist to only segments we *actually* have downloaded
        const availableSegments = playlist.segments.filter(s => this.downloadedSegments.has(s.uri.split('/').pop()));

        // We only want the *end* of the available list, up to our target buffer size
        const bufferedSegments = availableSegments.slice(-this.targetBufferSegments);
        
        if(bufferedSegments.length === 0) return; // Not ready yet

        let m3u8Content = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${Math.ceil(playlist.targetDuration)}\n`;
        
        // Find the media sequence of the *first segment we are including*
        const firstSegmentData = playlist.segments.find(s => s.uri.split('/').pop() === bufferedSegments[0].uri.split('/').pop());
        const mediaSequence = firstSegmentData ? firstSegmentData.mediaSequence : 0;

        m3u8Content += `#EXT-X-MEDIA-SEQUENCE:${mediaSequence}\n`;

        for (const segment of bufferedSegments) {
            if (segment.discontinuity) {
                m3u8Content += '#EXT-X-DISCONTINUITY\n';
            }
            m3u8Content += `#EXTINF:${segment.duration.toFixed(6)},\n`;
            // Point to the *relative path* that Nginx will serve
            m3u8Content += `buffer/${segment.uri.split('/').pop()}\n`; 
        }

        try {
             fs.writeFileSync(this.localPlaylistPath, m3u8Content);
        } catch (e) {
            console.error('[Buffer] Failed to write local playlist file:', e.message);
        }
    }

    cleanupOldSegments(currentSegmentFilenames) {
        // This logic keeps our buffer folder from growing forever
        const segmentsToKeep = new Set(currentSegmentFilenames); 
        for (const filename of this.downloadedSegments) {
            if (!segmentsToKeep.has(filename)) {
                try {
                    const localPath = path.join(this.bufferDir, filename);
                    if (fs.existsSync(localPath)) {
                        fs.unlinkSync(localPath);
                    }
                    this.downloadedSegments.delete(filename);
                } catch (e) {
                    console.warn(`[Buffer] Failed to cleanup segment ${filename}:`, e.message);
                }
            }
        }
    }
}


// ================================================================
// --- STREAM START/STOP (MODIFIED) ---
// ================================================================

async function startStream(sourceUrl) {
    if (ffmpegProcess) {
        console.log('Killing existing ffmpeg process...');
        stopAllStreamProcesses();
    }

    console.log(`[Stream Start] Starting stream from: ${sourceUrl}`);
    const settings = getSettings();
    const activeProfile = getActiveProfile();
    const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
    let streamInputUrl = sourceUrl;
    const isLocalPlaylist = sourceUrl.includes('local_playlist.m3u8'); // Check if we are restarting ourself

    // --- NEW LOGIC: Detect MPD/DASH stream and use Manifest Refresher ---
    const isMpdStream = sourceUrl.endsWith('.mpd') || sourceUrl.includes('.mpd?');
    
    if (isMpdStream && !isLocalPlaylist) {
        console.log('[Stream Start] MPD (DASH) stream detected. Using Manifest Refresher.');
        try {
            manifestRefresher = new ManifestRefresher(sourceUrl, userAgent);
            const localManifestPath = await manifestRefresher.start(); // Wait for the first fetch
            
            // Point FFmpeg to the locally-served, stable manifest file
            streamInputUrl = `http://127.0.0.1:8994/${path.basename(localManifestPath)}`;
            console.log(`[Stream Start] Manifest Refresher is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] CRITICAL: Manifest Refresher failed to initialize:", error.message);
            stopAllStreamProcesses();
            return; // Stop here
        }
    
    // --- ORIGINAL HLS BUFFER LOGIC ---
    } else if (settings.buffer.enabled && !isLocalPlaylist && !isMpdStream) {
        console.log('[Stream Start] HLS stream detected. Using Pre-fetch Buffer mode.');
        try {
            bufferManager = new BufferManager(sourceUrl, settings.buffer.delaySeconds);
            // --- RACE CONDITION FIX ---
            const { localPlaylistPath } = await bufferManager.start();
            
            streamInputUrl = `http://127.0.0.1:8994/${path.basename(localPlaylistPath)}`;
            console.log(`[Stream Start] Buffer is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] Buffer Manager failed to initialize:", error.message);
            stopAllStreamProcesses(); 
            return;
        }
    } else if (isLocalPlaylist) {
        console.log('[Stream Start] Restarting FFmpeg against existing local playlist.');
    } else {
        console.log('[Stream Start] Using Direct Stream mode (Buffer disabled or unsupported stream type).');
    }

    // --- IDEA 2 LOGIC ---
    const commandWithPlaceholders = activeProfile.command
        .replace(/{streamUrl}/g, streamInputUrl)
        .replace(/{userAgent}/g, userAgent);

    // --- NEW: MPD/DASH Warning (still relevant) ---
    if (isMpdStream && !commandWithPlaceholders.includes('-map')) {
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        console.warn('[FFmpeg] WARNING: You are streaming an MPD (DASH) source without a "-map" flag.');
        console.warn('[FFmpeg] This will likely fail by grabbing the lowest quality stream or exiting with an error.');
        console.warn('[FFmpeg] Please use a profile that maps specific video/audio streams (e.g., -map 0:4 -map 0:5).');
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
    }

    // --- FIX: BUG #2 - Use quote-safe parser instead of .split(' ') ---
    const args = (commandWithPlaceholders.match(/(?:[^\s"]+|"[^"]*")+/g) || [])
                 .map(arg => arg.replace(/^"|"$/g, '')); // Split by space BUT respect quotes, then remove the quotes.
    
    console.log(`[FFmpeg] Starting process with command: ffmpeg ${args.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = sourceUrl; // Store the *original* source URL

    ffmpegProcess.stdout.on('data', (data) => {
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        const stderrStr = data.toString();
        if (!stderrStr.startsWith('frame=') && !stderrStr.startsWith('size=') && !stderrStr.startsWith('Opening') && !stderrStr.includes('dropping overlapping extension')) {
             console.error(`[ffmpeg stderr]: ${stderrStr.trim()}`);
        }
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        let safeToStop = true; // Flag to prevent recursion
        
        // --- MODIFIED: Check for buffer OR manifest refresher ---
        if (code !== 0 && code !== 255) { // 255 is normal kill
            if ((bufferManager && !bufferManager.stopFlag) || (manifestRefresher && !manifestRefresher.stopFlag)) {
                console.warn('[ffmpeg] Process failed. Attempting to restart FFmpeg against local playlist/manifest...');
                safeToStop = false; // Don't stop helpers, we are restarting
                startStream(streamInputUrl); // Pass the LOCAL URL to restart
            }
        }
        
        if (safeToStop && ffmpegProcess) { // Check if it hasn't been stopped by /api/stop
            stopAllStreamProcesses();
        }
    });

    ffmpegProcess.on('error', (err) => {
        console.error('[ffmpeg] Failed to start process:', err);
        stopAllStreamProcesses();
    });
}


// ================================================================
// --- ORIGINAL AUTH & API ENDPOINTS (MODIFIED) ---
// ================================================================

// --- Auth API Endpoints ---

app.get('/api/auth/check', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (err) return res.status(500).json({ error: 'Database error' });
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
        if (err) return res.status(500).json({ error: 'Database error' });
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
        if (err) return res.status(500).json({ error: 'Database error' });
        if (!user) return res.status(401).json({ error: 'Invalid username or password' });

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
        if (err) return res.status(500).json({ error: 'Failed to log out' });
        res.clearCookie('connect.sid');
        res.json({ message: 'Logout successful' });
    });
});

// --- User Management API Endpoints (Protected) ---

app.get('/api/users', isAuthenticated, (req, res) => {
    db.all("SELECT id, username FROM users ORDER BY username", (err, rows) => {
        if (err) return res.status(500).json({ error: 'Database error' });
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
        if (err) return res.status(500).json({ error: 'Database error' });
        if (row.count <= 1) {
            return res.status(400).json({ error: 'Cannot delete the last user.' });
        }
        db.run("DELETE FROM users WHERE id = ?", [userIdToDelete], function(err) {
            if (err) return res.status(500).json({ error: 'Failed to delete user' });
            if (this.changes === 0) return res.status(404).json({ error: 'User not found' });
            res.json({ message: 'User deleted' });
        });
    });
});

// --- Stream API Endpoints (NOW MODIFIED) ---

app.post('/api/start', isAuthenticated, (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'Missing "url" in request body' });
    }

    // Clear old logs and blocklist
    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('Cleared HLS log and blocklist for new stream.');
        reloadNginx();
    } catch (writeErr) {
        console.error('Failed to clear logs or blocklist:', writeErr);
    }

    try {
        // --- MODIFIED ---
        // We no longer wait for startStream, as it's now async and has a buffer warmup.
        // We start it and return success immediately. The UI will show loading.
        startStream(url); 
        res.json({ message: 'Stream process initiated successfully' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to start stream', details: error.message });
    }
});

app.post('/api/stop', isAuthenticated, (req, res) => {
    console.log('Stopping stream via API request...');
    stopAllStreamProcesses(); // Use our new global stop function

    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('Cleared HLS log and blocklist on stream stop.');
        reloadNginx();
    } catch (writeErr) {
        console.error('Failed to clear logs or blocklist:', writeErr);
    }
    
    res.json({ message: 'Stream stopped' });
});

app.get('/api/status', isAuthenticated, (req, res) => {
    res.json({ 
        running: (ffmpegProcess !== null || bufferManager !== null || manifestRefresher !== null), // Stream is "running" if any helper is active
        url: currentStreamUrl 
    });
});

app.get('/api/viewers', isAuthenticated, (req, res) => {
    // MODIFIED: Check buffer OR ffmpeg process OR manifest refresher
    if (ffmpegProcess === null && bufferManager === null && manifestRefresher === null) {
        return res.json([]); // No stream running, no viewers
    }
    let blockedIps = new Set();
    try {
        const blocklistData = fs.readFileSync(BLOCKLIST_PATH, 'utf8');
        blocklistData.split('\n').forEach(line => {
            if (line.startsWith('deny ')) {
                blockedIps.add(line.substring(5, line.length - 1));
            }
        });
    } catch (readErr) {
        // Non-fatal, just log
    }

    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) {
            // Non-fatal, just return empty
            return res.json([]);
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
        return res.status(400).json({ error: 'Missing "ip" in request body' });
    }
    fs.readFile(BLOCKLIST_PATH, 'utf8', (readErr, data) => {
        if (readErr) return res.status(500).json({ error: 'Failed to read blocklist' });
        if (data.includes(`deny ${ip};`)) {
            return res.status(409).json({ message: `${ip} is already blocked.` });
        }
        const blockRule = `deny ${ip};\n`;
        fs.appendFile(BLOCKLIST_PATH, blockRule, (appendErr) => {
            if (appendErr) return res.status(500).json({ error: 'Failed to update blocklist' });
            console.log(`Added ${ip} to blocklist. Reloading Nginx...`);
            reloadNginx();
            res.json({ message: `Successfully terminated connection for ${ip}` });
        });
    });
});


// Serve the index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, '127.0.0.1', () => {
    console.log(`Stream control API listening on port ${port}`);
});
