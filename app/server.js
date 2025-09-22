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
// We need axios for reliable HTTP requests (to download segments AND proxy)
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
// --- NEW: SETTINGS MANAGEMENT (VOD Proxy Update) ---
// ================================================================

// --- NEW: User agent constant ---
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

function getDefaultSettings() {
    return {
        // "profiles" is now "liveProfiles". VODs no longer use profiles.
        liveProfiles: [
            {
                id: 'default-cpu',
                name: 'Default (CPU Stream Copy - HLS Only)',
                // --- ADDED reconnect and live_start_index flags ---
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -live_start_index -1 -i "{streamUrl}" -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                isDefault: true
            },
            {
                id: 'nvidia-gpu',
                name: 'NVIDIA (NVENC Re-encode - HLS Only)',
                // --- ADDED reconnect and live_start_index flags ---
                command: '-hwaccel nvdec -user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -live_start_index -1 -i "{streamUrl}" -map 0:3 -map 0:4 -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                isDefault: true
            },
            {
                id: 'mpd-1080p-copy',
                name: 'MPD/DASH 1080p (Stream Copy)',
                // --- ADDED live_start_index flag ---
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -live_start_index -1 -i "{streamUrl}" -map 0:4 -map 0:5 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                isDefault: true
            },
            {
                id: 'mpd-720p-copy',
                name: 'MPD/DASH 720p (Stream Copy)',
                // --- ADDED live_start_index flag ---
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -live_start_index -1 -i "{streamUrl}" -map 0:3 -map 0:5 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                isDefault: true
            },
            {
                id: 'mpd-1080p-nvenc',
                name: 'MPD/DASH 1080p (NVIDIA NVENC)',
                 // --- ADDED live_start_index flag ---
                command: '-hwaccel nvdec -user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -live_start_index -1 -i "{streamUrl}" -map 0:4 -map 0:5 -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                isDefault: true
            }
        ],
        // activeProfileId is now activeLiveProfileId
        activeLiveProfileId: 'default-cpu', // Default to this one
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
        saveSettings(defaults); // Save defaults and return
        return defaults;
    }
    try {
        const settingsData = fs.readFileSync(SETTINGS_PATH, 'utf8');
        let settings = JSON.parse(settingsData);

        // --- NEW: Migration Logic ---
        let needsSave = false;
        
        // 1. Check for old "profiles" key and migrate to "liveProfiles"
        if (settings.profiles) {
            console.log("Migrating settings: 'profiles' -> 'liveProfiles'");
            settings.liveProfiles = settings.profiles;
            delete settings.profiles;
            needsSave = true;
        }

        // 2. Check for old "activeProfileId" and migrate to "activeLiveProfileId"
        if (settings.activeProfileId) {
            console.log("Migrating settings: 'activeProfileId' -> 'activeLiveProfileId'");
            settings.activeLiveProfileId = settings.activeProfileId;
            delete settings.activeProfileId;
            needsSave = true;
        }

        // 3. Check for "vodProfiles" and remove (they are obsolete)
        if (settings.vodProfiles) {
            console.log("Migrating settings: Removing obsolete 'vodProfiles'");
            delete settings.vodProfiles;
            needsSave = true;
        }

        // 4. Check for "liveProfiles" and ensure defaults exist
        if (!settings.liveProfiles) {
            settings.liveProfiles = getDefaultSettings().liveProfiles;
            needsSave = true;
        }

        // 5. Check "activeLiveProfileId"
        if (!settings.activeLiveProfileId || !settings.liveProfiles.find(p => p.id === settings.activeLiveProfileId)) {
            settings.activeLiveProfileId = settings.liveProfiles.find(p => p.isDefault)?.id || settings.liveProfiles[0]?.id;
            needsSave = true;
        }
        
        // 6. Ensure all default profiles have the new flags
        const defaultProfiles = getDefaultSettings().liveProfiles;
        for (const defaultProfile of defaultProfiles) {
            const currentProfile = settings.liveProfiles.find(p => p.id === defaultProfile.id);
            if (currentProfile && currentProfile.isDefault) {
                // Add missing flags
                if (!currentProfile.command.includes("-live_start_index")) {
                    console.log(`Migrating settings: Adding -live_start_index to ${currentProfile.name}`);
                    // This is a simple add, crude but effective for this flag
                    currentProfile.command = currentProfile.command.replace("-i \"{streamUrl}\"", "-live_start_index -1 -i \"{streamUrl}\"");
                    needsSave = true;
                }
                if (!currentProfile.command.includes("-reconnect")) {
                     console.log(`Migrating settings: Adding -reconnect flags to ${currentProfile.name}`);
                    currentProfile.command = currentProfile.command.replace("-i \"{streamUrl}\"", "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i \"{streamUrl}\"");
                    needsSave = true;
                }
            }
        }

        if (needsSave) {
            console.log('Saving migrated settings file...');
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
        // Clean up any old/obsolete keys before saving
        const settingsToSave = {
            liveProfiles: settings.liveProfiles || getDefaultSettings().liveProfiles,
            activeLiveProfileId: settings.activeLiveProfileId || getDefaultSettings().activeLiveProfileId,
            buffer: settings.buffer || getDefaultSettings().buffer
        };
        
        fs.writeFileSync(SETTINGS_PATH, JSON.stringify(settingsToSave, null, 2));
        console.log('Settings saved successfully.');
        return true;
    } catch (e) {
        console.error("Failed to save settings:", e);
        return false;
    }
}

// Renamed to getActiveLiveProfile
function getActiveLiveProfile() {
    const settings = getSettings();
    let activeProfile = settings.liveProfiles.find(p => p.id === settings.activeLiveProfileId);
    
    if (!activeProfile) {
        // Fallback to first default, or first profile
        activeProfile = settings.liveProfiles.find(p => p.isDefault === true) || settings.liveProfiles[0];
    }
    return activeProfile || getDefaultSettings().liveProfiles[0]; // Absolute fallback
}


// --- NEW: Settings & Profiles API Endpoints (Protected & Updated) ---

app.get('/api/settings', isAuthenticated, (req, res) => {
    res.json(getSettings());
});

app.post('/api/settings', isAuthenticated, (req, res) => {
    const newSettings = req.body;
    
    // Validate the new structure
    if (!newSettings || !newSettings.liveProfiles || !newSettings.buffer || !newSettings.activeLiveProfileId) {
        return res.status(400).json({ error: 'Invalid settings object. Missing required keys.' });
    }
    
    // Ensure the activeLiveProfileId is valid
    if (!newSettings.liveProfiles.find(p => p.id === newSettings.activeLiveProfileId)) {
        newSettings.activeLiveProfileId = newSettings.liveProfiles[0]?.id; // Default to first if not found
    }
    
    // (active: boolean is no longer used, no need to clean up)
    
    if (saveSettings(newSettings)) {
        res.json(getSettings()); // Send back the cleaned/saved settings
    } else {
        res.status(500).json({ error: 'Failed to save settings to disk.' });
    }
});



// ================================================================
// --- STREAM STATE & HELPERS ---
// ================================================================

// --- NEW: Updated Global State ---
let ffmpegProcess = null;
let bufferManager = null; // Handle for the buffer manager
let currentStreamUrl = ""; // The *original* URL from the user
let currentStreamType = "NONE"; // "NONE", "LIVE", "VOD"
let currentVodUrl = ""; // The proxied m3u8 URL
let vodBaseUrl = ""; // The base URL for VOD segments

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
        // Delete the buffer directory
        const bufferDir = path.join('/var/www/hls', 'buffer');
        if (fs.existsSync(bufferDir)) {
            fs.rmSync(bufferDir, { recursive: true, force: true });
        }
    } catch (e) {
        console.error('[Cleanup] Error during HLS file cleanup:', e.message);
    }
}

// --- UPDATED: Global stop function ---
function stopAllStreamProcesses() {
    console.log('[Stop] Stopping all stream processes...');
    
    // 1. Stop Live Buffer Manager
    if (bufferManager) {
        bufferManager.stop();
        bufferManager = null;
    }
    
    // 2. Stop Live FFmpeg Process
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }
    
    // 3. Reset VOD Proxy State
    currentVodUrl = "";
    vodBaseUrl = "";
    
    // 4. Reset Global State
    currentStreamUrl = "";
    currentStreamType = "NONE";
    
    // 5. Clean up files
    cleanupHlsFiles();
    
    // 6. Clear Nginx logs
    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('[Stop] Cleared HLS log and blocklist.');
        reloadNginx();
    } catch (writeErr) {
        console.error('[Stop] Failed to clear logs or blocklist:', writeErr);
    }
}


// ================================================================
// --- PRE-FETCH BUFFER MANAGER (Used for LIVE only) ---
// ================================================================

class BufferManager {
    constructor(sourceUrl, bufferSeconds) {
        this.sourceUrl = sourceUrl;
        this.targetBufferSegments = Math.max(1, Math.floor(bufferSeconds / 4)); // Assuming avg 4s segments
        this.bufferDir = path.join('/var/www/hls', 'buffer'); 
        this.localPlaylistPath = path.join('/var/www/hls', 'local_playlist.m3u8'); // Nginx serves this
        this.segmentQueue = [];    
        this.downloadedSegments = new Set();
        this.segmentBaseUrl = '';
        this.stopFlag = false;
        this.timeoutId = null;
        this.initialPlaylistReady = null; 
        this.resolveInitialPlaylist = null;
        this.rejectInitialPlaylist = null;

        console.log(`[Buffer] Manager started. Target buffer: ${this.targetBufferSegments} segments.`);

        try {
            if (fs.existsSync(this.bufferDir)) {
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
            fs.mkdirSync(this.bufferDir, { recursive: true });
        } catch (e) {
            console.error('[Buffer] Failed to create or clean buffer directory:', e);
        }
    }

    start() {
        this.initialPlaylistReady = new Promise((resolve, reject) => {
            this.resolveInitialPlaylist = resolve;
            this.rejectInitialPlaylist = reject;
        });
        this.fetchPlaylist(); // Start the loop
        return this.initialPlaylistReady; 
    }

    stop() {
        this.stopFlag = true;
        if (this.timeoutId) {
            clearTimeout(this.timeoutId);
        }
        console.log('[Buffer] Manager stopped.');
        // Cleanup is handled by stopAllStreamProcesses
    }

    async fetchPlaylist() {
        if (this.stopFlag) return;

        try {
            const response = await axios.get(this.sourceUrl, { 
                timeout: 3000,
                headers: { 'User-Agent': USER_AGENT } // Add user agent
            });
            const parser = new Parser();
            parser.push(response.data);
            parser.end();

            const playlist = parser.manifest;
            if (!playlist.segments || playlist.segments.length === 0) {
                console.warn('[Buffer] Source playlist is empty or invalid.');
                if (this.rejectInitialPlaylist) { 
                    this.rejectInitialPlaylist(new Error("Source playlist is empty or invalid."));
                    this.rejectInitialPlaylist = null; 
                }
                throw new Error("Empty or invalid playlist");
            }

            const firstSegmentUri = playlist.segments[0].uri;
            if (firstSegmentUri.startsWith('http')) {
                this.segmentBaseUrl = ''; 
            } else {
                const urlObj = new URL(this.sourceUrl);
                urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
                this.segmentBaseUrl = urlObj.toString();
            }

            const segmentUris = playlist.segments.map(s => {
                return this.segmentBaseUrl ? new URL(s.uri, this.segmentBaseUrl).href : s.uri;
            });
            this.segmentQueue = segmentUris; 

            this.downloadSegments(playlist.segments); 
            this.writeLocalPlaylist(playlist);

            if (this.resolveInitialPlaylist) {
                console.log('[Buffer] Initial playlist is ready.');
                this.resolveInitialPlaylist({ localPlaylistPath: this.localPlaylistPath });
                this.resolveInitialPlaylist = null; 
                this.rejectInitialPlaylist = null;
            }

            const refreshInterval = (playlist.segments[0].duration || 4) * 1000;
            this.timeoutId = setTimeout(() => this.fetchPlaylist(), refreshInterval / 2); 

        } catch (error) {
            console.error('[Buffer] Error fetching source playlist:', error.message);
            if (!this.stopFlag) {
                this.timeoutId = setTimeout(() => this.fetchPlaylist(), 2000); // Retry faster on error
            }
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(error);
                this.rejectInitialPlaylist = null;
                this.resolveInitialPlaylist = null;
            }
        }
    }

    async downloadSegments(originalSegments) {
        for (let i = 0; i < this.segmentQueue.length; i++) {
            const segmentUrl = this.segmentQueue[i];
            const filename = originalSegments[i].uri.split('/').pop();
            
            if (this.stopFlag) return;
            if (!this.downloadedSegments.has(filename)) {
                const localPath = path.join(this.bufferDir, filename);

                try {
                    const response = await axios.get(segmentUrl, { 
                        responseType: 'stream', 
                        timeout: 5000,
                        headers: { 'User-Agent': USER_AGENT } // Add user agent
                    });
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
        this.cleanupOldSegments(originalSegments.map(s => s.uri.split('/').pop()));
    }

    writeLocalPlaylist(playlist) {
        const availableSegments = playlist.segments.filter(s => this.downloadedSegments.has(s.uri.split('/').pop()));
        const bufferedSegments = availableSegments.slice(-this.targetBufferSegments);
        
        if(bufferedSegments.length === 0) return; 

        let m3u8Content = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${Math.ceil(playlist.targetDuration)}\n`;
        
        const firstSegmentData = playlist.segments.find(s => s.uri.split('/').pop() === bufferedSegments[0].uri.split('/').pop());
        const mediaSequence = firstSegmentData ? firstSegmentData.mediaSequence : 0;

        m3u8Content += `#EXT-X-MEDIA-SEQUENCE:${mediaSequence}\n`;

        for (const segment of bufferedSegments) {
            if (segment.discontinuity) {
                m3u8Content += '#EXT-X-DISCONTINUITY\n';
            }
            m3u8Content += `#EXTINF:${segment.duration.toFixed(6)},\n`;
            m3u8Content += `buffer/${segment.uri.split('/').pop()}\n`; 
        }

        try {
             fs.writeFileSync(this.localPlaylistPath, m3u8Content);
        } catch (e) {
            console.error('[Buffer] Failed to write local playlist file:', e.message);
        }
    }

    cleanupOldSegments(currentSegmentFilenames) {
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
// --- NEW: STREAM START/STOP LOGIC ---
// ================================================================

/**
 * NEW: Starts the FFmpeg and Buffer process for a LIVE stream.
 */
async function startLiveStream(sourceUrl) {
    console.log(`[Stream Start] LIVE Mode starting for: ${sourceUrl}`);
    const settings = getSettings();
    const activeProfile = getActiveLiveProfile();
    let streamInputUrl = sourceUrl;
    const isLocalPlaylist = sourceUrl.includes('local_playlist.m3u8');

    // Check for MPD and force direct, as buffer doesn't support it
    let forceDirect = false;
    if (sourceUrl.endsWith('.mpd') || sourceUrl.includes('.mpd?')) {
        console.warn('[Stream Start] MPD (DASH) stream detected. Buffer only supports HLS (.m3u8). Forcing Direct Stream mode.');
        forceDirect = true;
    }

    // Start Buffer Manager (if enabled and not MPD)
    if (settings.buffer.enabled && !isLocalPlaylist && !forceDirect) {
        console.log('[Stream Start] Using Pre-fetch Buffer mode.');
        try {
            bufferManager = new BufferManager(sourceUrl, settings.buffer.delaySeconds);
            const { localPlaylistPath } = await bufferManager.start();
            
            // Point FFmpeg to the Nginx-served local playlist
            streamInputUrl = `http://127.0.0.1:8994/local_playlist.m3u8`;
            console.log(`[Stream Start] Buffer is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] Buffer Manager failed to initialize:", error.message);
            stopAllStreamProcesses(); 
            return; // Exit if buffer fails
        }
    } else if (isLocalPlaylist) {
        console.log('[Stream Start] Restarting FFmpeg against existing buffer.');
    } else {
        console.log('[Stream Start] Using Direct Stream mode (Buffer disabled or MPD detected).');
    }

    // Get the command from the active profile
    const commandWithPlaceholders = activeProfile.command
        .replace(/{streamUrl}/g, streamInputUrl)
        .replace(/{userAgent}/g, USER_AGENT);

    // Warn if user is trying to stream MPD without a -map flag
    if (forceDirect && !commandWithPlaceholders.includes('-map')) {
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        console.warn('[FFmpeg] WARNING: You are streaming an MPD (DASH) source without a "-map" flag.');
        console.warn('[FFmpeg] This will likely fail. Please use a profile that maps specific streams (e.g., -map 0:4 -map 0:5).');
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
    }

    // Split command into args safely (respecting quotes)
    const args = (commandWithPlaceholders.match(/(?:[^\s"]+|"[^"]*")+/g) || [])
                 .map(arg => arg.replace(/^"|"$/g, ''));
    
    console.log(`[FFmpeg] Starting process with command: ffmpeg ${args.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamType = "LIVE"; // Set state
    currentStreamUrl = sourceUrl; // Store the *original* source URL

    ffmpegProcess.stdout.on('data', (data) => { /* console.log(`ffmpeg stdout: ${data}`); */ });

    ffmpegProcess.stderr.on('data', (data) => {
        const stderrStr = data.toString();
        // Filter out noisy progress messages
        if (!stderrStr.startsWith('frame=') && !stderrStr.startsWith('size=') && !stderrStr.startsWith('Opening') && !stderrStr.includes('dropping overlapping extension')) {
             console.error(`[ffmpeg stderr]: ${stderrStr.trim()}`);
        }
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        let safeToStop = true; 
        if (code !== 0 && code !== 255) { // 255 is normal kill
            // If buffer is still running, it means ffmpeg died unexpectedly
            if (bufferManager && bufferManager.stopFlag === false) {
                console.warn('[ffmpeg] Process failed. Attempting to restart FFmpeg against buffer...');
                safeToStop = false; 
                startLiveStream(streamInputUrl); // Pass the LOCAL playlist URL to restart
            }
        }
        
        if (safeToStop && ffmpegProcess) { 
            // If we are not restarting, stop everything.
            // This check prevents stopAll from being called after /api/stop
            if(currentStreamType === "LIVE") {
                 stopAllStreamProcesses();
            }
        }
    });

    ffmpegProcess.on('error', (err) => {
        console.error('[ffmpeg] Failed to start process:', err);
        stopAllStreamProcesses();
    });
}

/**
 * --- MODIFIED: Sets the state for the VOD proxy. ---
 */
function startVodProxy(sourceUrl) {
    console.log(`[Stream Start] VOD Mode starting for: ${sourceUrl}`);
    
    try {
        // --- FIX: Calculate and store the base URL for segments ---
        const urlObj = new URL(sourceUrl);
        // Get path up to the last '/'
        urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
        // Clear search params from the *base* URL
        urlObj.search = ''; 
        
        vodBaseUrl = urlObj.toString(); // This is now clean, e.g., https://.../path/
        
        currentVodUrl = sourceUrl; // This remains the full URL to the main playlist
        currentStreamType = "VOD";
        currentStreamUrl = sourceUrl; // Store original URL
        
        console.log(`[VOD Proxy] Ready. Serving from /stream/live.m3u8`);
        console.log(`[VOD Proxy] Segment Base URL set to: ${vodBaseUrl}`);
        return true;

    } catch (error) {
        console.error(`[VOD Proxy] Invalid VOD URL provided: ${error.message}`);
        return false;
    }
}


// ================================================================
// --- AUTH & USER API ENDPOINTS (Unchanged) ---
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

// ================================================================
// --- UPDATED: STREAM CONTROL API ENDPOINTS ---
// ================================================================

app.post('/api/start', isAuthenticated, (req, res) => {
    // --- NEW: Expect "url" and "type" ---
    const { url, type } = req.body;
    if (!url || !type) {
        return res.status(400).json({ error: 'Missing "url" or "type" in request body' });
    }
    
    // Stop any existing stream first
    stopAllStreamProcesses();

    if (type === 'LIVE') {
        // Start live stream (async, don't wait)
        startLiveStream(url);
        res.json({ message: 'LIVE stream process initiated.' });
    
    } else if (type === 'VOD') {
        // Start VOD proxy (sync)
        if (startVodProxy(url)) {
            res.json({ message: 'VOD proxy started successfully.' });
        } else {
            res.status(400).json({ error: 'Invalid VOD URL.' });
        }
        
    } else {
        res.status(400).json({ error: 'Invalid stream "type". Must be "LIVE" or "VOD".' });
    }
});

app.post('/api/stop', isAuthenticated, (req, res) => {
    console.log('[API] /api/stop requested.');
    stopAllStreamProcesses();
    res.json({ message: 'All stream processes stopped' });
});

app.get('/api/status', isAuthenticated, (req, res) => {
    // --- NEW: Return type ---
    res.json({ 
        running: (currentStreamType !== "NONE"),
        url: currentStreamUrl,
        type: currentStreamType
    });
});

// --- VIEWER & TERMINATE (Unchanged, work for LIVE only) ---
app.get('/api/viewers', isAuthenticated, (req, res) => {
    // This only works for LIVE streams served by Nginx
    if (currentStreamType !== "LIVE") {
        return res.json([]); 
    }
    
    let blockedIps = new Set();
    try {
        const blocklistData = fs.readFileSync(BLOCKLIST_PATH, 'utf8');
        blocklistData.split('\n').forEach(line => {
            if (line.startsWith('deny ')) {
                blockedIps.add(line.substring(5, line.length - 1));
            }
        });
    } catch (readErr) { /* non-fatal */ }

    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) return res.json([]); 

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


// ================================================================
// --- MODIFIED: VOD PROXY ENDPOINTS ---
// ================================================================

/**
 * Main playlist proxy. Fetches the source VOD playlist and rewrites
 * all segment/playlist URLs to point back to our server.
 */
app.get('/stream/live.m3u8', async (req, res) => {
    if (currentStreamType !== 'VOD' || !currentVodUrl) {
        return res.status(404).json({ error: 'No VOD stream is active.' });
    }

    try {
        console.log(`[VOD Proxy] Fetching main playlist: ${currentVodUrl}`);
        const response = await axios.get(currentVodUrl, {
            headers: { 'User-Agent': USER_AGENT }
        });

        let playlistData = response.data;

        // Rewrite URLs: Prepend our proxy path to all relative URLs
        const finalLines = playlistData.split('\n').map(line => {
            const trimmedLine = line.trim();
            if (trimmedLine.length === 0 || trimmedLine.startsWith('#')) {
                return line; // Keep tags and empty lines
            }
            
            // This is a segment or sub-playlist URL. Prepend our path.
            // We strip any leading slashes to prevent // issues
            return `/stream/${trimmedLine.replace(/^\//, '')}`;
        });

        const rewrittenPlaylist = finalLines.join('\n');
        
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.send(rewrittenPlaylist);

    } catch (error) {
        console.error(`[VOD Proxy] Error fetching main playlist: ${error.message}`);
        res.status(500).json({ error: 'Failed to fetch source playlist.' });
    }
});

/**
 * --- MODIFIED: Segment and sub-playlist proxy ---
 * This endpoint is now "playlist-aware".
 * If it proxies a .m3u8 file, it rewrites it.
 * If it proxies a .ts file, it streams it.
 */
app.get('/stream/:segment(*)', async (req, res) => {
    if (currentStreamType !== 'VOD' || !vodBaseUrl) {
        return res.status(404).json({ error: 'No VOD stream is active.' });
    }

    // e.g., "path/to/sub.m3u8" or "path/to/seg.ts"
    const requestedPath = req.params.segment; 
    
    // Construct the full URL to fetch from the origin
    const targetUrl = new URL(requestedPath, vodBaseUrl).href;

    // --- NEW: Check if the requested asset is a playlist ---
    const isPlaylist = requestedPath.split('?')[0].endsWith('.m3u8');

    try {
        if (isPlaylist) {
            // It's a playlist: fetch as text, rewrite, and send
            console.log(`[VOD Proxy] Rewriting playlist: ${requestedPath}`);
            const response = await axios.get(targetUrl, {
                responseType: 'text', // Fetch as text
                headers: { 'User-Agent': USER_AGENT }
            });

            // Find the sub-path, e.g., "path/to/"
            const subPath = requestedPath.includes('/') 
                ? requestedPath.substring(0, requestedPath.lastIndexOf('/') + 1)
                : "";

            const rewrittenPlaylist = response.data.split('\n').map(line => {
                const trimmedLine = line.trim();
                if (trimmedLine.length === 0 || trimmedLine.startsWith('#')) {
                    return line; // Keep tags and empty lines
                }
                
                // This is a segment or sub-playlist URL.
                // Prepend our full proxy path including the sub-path
                return `/stream/${subPath}${trimmedLine}`;
            }).join('\n');
            
            res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
            res.send(rewrittenPlaylist);

        } else {
            // It's a segment (.ts, .aac, etc.): pipe it directly
            // console.log(`[VOD Proxy] Piping segment: ${requestedPath}`); // Too noisy for logs
            const response = await axios.get(targetUrl, {
                responseType: 'stream',
                headers: { 'User-Agent': USER_AGENT }
            });

            // Set content type for segments if known
            if (requestedPath.endsWith('.ts')) {
                res.setHeader('Content-Type', 'video/mp2t');
            } else if (requestedPath.endsWith('.aac')) {
                 res.setHeader('Content-Type', 'audio/aac');
            }
            
            response.data.pipe(res);
        }

    } catch (error)
        {
        // Don't log 404s as full errors, they are common
        if (error.response && error.response.status === 404) {
             console.warn(`[VOD Proxy] Asset not found (404): ${requestedPath}`);
             res.status(404).json({ error: 'VOD asset not found.' });
        } else {
            console.error(`[VOD Proxy] Error fetching asset ${requestedPath} (URL: ${targetUrl}): ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch VOD asset.' });
        }
    }
});


// ================================================================
// --- Server Start ---
// ================================================================

// Serve the index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, '127.0.0.1', () => {
    console.log(`Stream control API listening on port ${port}`);
});
