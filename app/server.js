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
const axios = require('axios');
const { Parser: HlsParser } = require('m3u8-parser');
// --- NEW: MPD (DASH) Parser ---
const { parse: MpdParser, stringify: MpdStringify } = require('@eyevinn/mpd-parser');


const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- DB Setup ---
const DATA_DIR = '/data'; // This is the persistent volume mounted by Docker
const DB_PATH = path.join(DATA_DIR, 'restream.db');
const SETTINGS_PATH = path.join(DATA_DIR, 'settings.json');
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
// --- SETTINGS MANAGEMENT ---
// ================================================================

function getDefaultSettings() {
    return {
        profiles: [
            {
                id: 'default-cpu',
                name: 'Default (CPU Stream Copy - HLS Only)',
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: true,
                isDefault: true
            },
            {
                id: 'nvidia-gpu',
                name: 'NVIDIA (NVENC Re-encode - HLS Only)',
                command: '-hwaccel nvdec -user_agent "{userAgent}" -i "{streamUrl}" -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            },
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
                command: '-hwaccel nvdec -user_agent "{userAgent}" -i "{streamUrl}" -map 0:4 -map 0:5 -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
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

        let needsSave = false;
        if (settings.profiles && settings.profiles.length > 0) {
            const defaultIds = ['default-cpu', 'nvidia-gpu', 'mpd-1080p-copy', 'mpd-720p-copy', 'mpd-1080p-nvenc'];
            for (const profile of settings.profiles) {
                if (defaultIds.includes(profile.id) && profile.isDefault !== true) {
                    profile.isDefault = true;
                    needsSave = true;
                } else if (!defaultIds.includes(profile.id) && profile.isDefault !== false) {
                    profile.isDefault = false;
                    needsSave = true;
                }
            }
        }
        if (needsSave) {
            console.log('Migrating settings to include isDefault flag...');
            saveSettings(settings);
        }

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
    let activeProfile = settings.profiles.find(p => p.id === settings.activeProfileId);
    if (!activeProfile) {
        activeProfile = settings.profiles.find(p => p.active === true);
    }
    if (!activeProfile) {
        activeProfile = settings.profiles[0];
    }
    return activeProfile || getDefaultSettings().profiles[0];
}


app.get('/api/settings', isAuthenticated, (req, res) => {
    res.json(getSettings());
});

app.post('/api/settings', isAuthenticated, (req, res) => {
    const newSettings = req.body;
    if (!newSettings || !newSettings.profiles || !newSettings.buffer) {
        return res.status(400).json({ error: 'Invalid settings object.' });
    }

    let activeId = newSettings.activeProfileId;
    if (!activeId) {
        const activeProfile = newSettings.profiles.find(p => p.active === true);
        activeId = activeProfile ? activeProfile.id : newSettings.profiles[0]?.id;
    }
    
    if (!newSettings.profiles.find(p => p.id === activeId)) {
        activeId = newSettings.profiles[0]?.id;
    }
    
    newSettings.activeProfileId = activeId;
    
    newSettings.profiles.forEach(p => {
        p.active = (p.id === activeId);
    });
    
    if (saveSettings(newSettings)) {
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
let bufferManager = null;
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';
const VIEWER_TIMEOUT_MS = 15 * 1000;

function reloadNginx() {
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) console.error('Failed to reload nginx:', stderr);
        else console.log('Nginx reloaded successfully.');
    });
}

// Function to clean up all HLS/DASH files and local playlists
function cleanupHlsFiles() {
    console.log('[Cleanup] All HLS/DASH segments and manifests cleared.');
    try {
        // Delete main nginx-served HLS playlist
        if (fs.existsSync('/var/www/hls/live.m3u8')) {
            fs.unlinkSync('/var/www/hls/live.m3u8');
            console.log('[Cleanup] Removed /var/www/hls/live.m3u8');
        }
        // Delete all .ts segments
        fs.readdirSync('/var/www/hls').forEach(file => {
            if (file.endsWith('.ts')) {
                fs.unlinkSync(path.join('/var/www/hls', file));
            }
        });
        // Delete our internally generated local HLS playlist
        if (fs.existsSync('/var/www/hls/local_playlist.m3u8')) {
            fs.unlinkSync('/var/www/hls/local_playlist.m3u8');
            console.log('[Cleanup] Removed /var/www/hls/local_playlist.m3u8');
        }
        // --- NEW: Delete internally generated local MPD manifest ---
        if (fs.existsSync('/var/www/hls/local_manifest.mpd')) {
            fs.unlinkSync('/var/www/hls/local_manifest.mpd');
            console.log('[Cleanup] Removed /var/www/hls/local_manifest.mpd');
        }
    } catch (e) {
        console.error('[Cleanup] Error during HLS/DASH file cleanup:', e.message);
    }
}

// Function to stop all streaming processes
function stopAllStreamProcesses() {
    console.log('Stopping all stream processes...');
    if (bufferManager) {
        bufferManager.stop();
        bufferManager = null;
    }
    if (ffmpegProcess) {
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }
    currentStreamUrl = "";
    cleanupHlsFiles(); // Clean up files on any stop
}


// ================================================================
// --- NEW: PRE-FETCH BUFFER MANAGER (HLS & MPD) ---
// ================================================================

class BufferManager {
    constructor(sourceUrl, bufferSeconds) {
        this.sourceUrl = sourceUrl;
        this.bufferDir = path.join('/var/www/hls', 'buffer');
        this.localHlsPlaylistPath = path.join('/var/www/hls', 'local_playlist.m3u8');
        this.localMpdManifestPath = path.join('/var/www/hls', 'local_manifest.mpd');
        
        this.targetBufferSegments = Math.max(1, Math.floor(bufferSeconds / 4)); // For HLS
        this.streamType = 'unknown'; // 'hls' or 'mpd'
        
        // --- MODIFIED: segmentQueue is now an array of objects: { url, filename } ---
        this.segmentQueue = [];
        this.downloadedSegments = new Set(); // Stores filenames
        this.segmentBaseUrl = '';
        this.stopFlag = false;
        this.timeoutId = null;
        this.initialPlaylistReady = null;
        this.resolveInitialPlaylist = null;
        this.rejectInitialPlaylist = null;

        // Ensure buffer directory exists and is clean
        try {
            if (fs.existsSync(this.bufferDir)) {
                console.log(`[Buffer] Clearing existing buffer directory: ${this.bufferDir}`);
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
            fs.mkdirSync(this.bufferDir, { recursive: true });
            console.log(`[Buffer] Created new buffer directory: ${this.bufferDir}`);
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

        // --- NEW: Detect stream type ---
        if (this.sourceUrl.endsWith('.mpd') || this.sourceUrl.includes('.mpd?')) {
            this.streamType = 'mpd';
            console.log(`[Buffer] Manager started for MPD. Target buffer: ${this.targetBufferSegments * 4}s.`);
        } else {
            this.streamType = 'hls';
            console.log(`[Buffer] Manager started for HLS. Target buffer: ${this.targetBufferSegments} segments.`);
        }

        this.fetchManifest(); // Start the loop
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
                console.log(`[Buffer] Deleting buffer directory on stop: ${this.bufferDir}`);
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
        } catch (e) {
            console.error('[Buffer] Failed to delete buffer directory on stop:', e);
        }
    }

    /**
     * Generic manifest fetcher that routes to HLS or MPD logic
     */
    async fetchManifest() {
        if (this.stopFlag) return;

        try {
            let refreshInterval = 2000; // Default refresh
            
            if (this.streamType === 'hls') {
                refreshInterval = await this.fetchHlsPlaylist();
            } else if (this.streamType === 'mpd') {
                refreshInterval = await this.fetchMpdManifest();
            }

            // Schedule the next fetch
            this.timeoutId = setTimeout(() => this.fetchManifest(), refreshInterval);

        } catch (error) {
            console.error(`[Buffer] Error fetching source ${this.streamType} manifest:`, error.message);
            if (!this.stopFlag) {
                this.timeoutId = setTimeout(() => this.fetchManifest(), 2000); // Retry faster on error
            }
            // If we fail *before* the first playlist is ready, reject the promise
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(error);
                this.rejectInitialPlaylist = null;
                this.resolveInitialPlaylist = null;
            }
        }
    }

    /**
     * Fetches and processes an HLS (.m3u8) playlist.
     * Returns the recommended refresh interval.
     */
    async fetchHlsPlaylist() {
        const response = await axios.get(this.sourceUrl, { timeout: 3000 });
        const parser = new HlsParser();
        parser.push(response.data);
        parser.end();

        const playlist = parser.manifest;
        if (!playlist.segments || playlist.segments.length === 0) {
            console.warn('[Buffer-HLS] Source playlist is empty or invalid.');
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(new Error("Source HLS playlist is empty or invalid."));
                this.rejectInitialPlaylist = null;
            }
            throw new Error("Empty or invalid HLS playlist");
        }

        // Determine the base URL for segments (relative vs absolute)
        const firstSegmentUri = playlist.segments[0].uri;
        if (firstSegmentUri.startsWith('http')) {
            this.segmentBaseUrl = ''; // Segments have full URLs
        } else {
            const urlObj = new URL(this.sourceUrl);
            urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
            this.segmentBaseUrl = urlObj.toString();
        }

        // --- MODIFIED: Update segmentQueue with { url, filename } objects ---
        this.segmentQueue = playlist.segments.map(s => {
            const filename = s.uri.split('/').pop();
            const url = this.segmentBaseUrl ? new URL(s.uri, this.segmentBaseUrl).href : s.uri;
            return { url, filename };
        });

        // Start downloading segments from the queue
        this.downloadSegments();
        // Write the local playlist for FFmpeg to read from
        this.writeLocalHlsPlaylist(playlist);

        // Resolve the promise only AFTER the first playlist is written
        if (this.resolveInitialPlaylist) {
            console.log('[Buffer-HLS] Initial HLS playlist is ready.');
            this.resolveInitialPlaylist({ localManifestPath: this.localHlsPlaylistPath });
            this.resolveInitialPlaylist = null;
            this.rejectInitialPlaylist = null;
        }

        // Clean up old segments
        this.cleanupOldSegments();
        
        // Return refresh interval
        return (playlist.segments[0].duration || 4) * 1000 / 2; // Refresh at half duration
    }

    /**
     * Fetches and processes an MPD (DASH) manifest.
     * Returns the recommended refresh interval.
     */
    async fetchMpdManifest() {
        const response = await axios.get(this.sourceUrl, { timeout: 3000 });
        const manifest = parse(response.data);

        if (!manifest.periods || manifest.periods.length === 0) {
            console.warn('[Buffer-MPD] Source manifest is empty or invalid.');
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(new Error("Source MPD manifest is empty or invalid."));
                this.rejectInitialPlaylist = null;
            }
            throw new Error("Empty or invalid MPD manifest");
        }

        // Determine base URL
        this.segmentBaseUrl = new URL(manifest.baseURL || '.', this.sourceUrl).href;

        // --- MODIFIED: Update segmentQueue with { url, filename } objects ---
        const newSegmentQueue = [];
        
        for (const period of manifest.periods) {
            for (const adaptationSet of period.adaptationSets) {
                const segmentTemplate = adaptationSet.segmentTemplate;
                if (segmentTemplate) {
                    // Handle Initialization Segment
                    if (segmentTemplate.initialization) {
                        const initFilename = segmentTemplate.initialization.replace('$RepresentationID$', adaptationSet.id);
                        const initUrl = new URL(initFilename, this.segmentBaseUrl).href;
                        if (!this.segmentQueue.find(s => s.filename === initFilename)) {
                             newSegmentQueue.push({ url: initUrl, filename: initFilename });
                        }
                    }

                    // Handle Media Segments
                    // This is a simplified parser for $Time$ templates. A full one is complex.
                    // We will just grab the *template* URL and hope FFmpeg handles it.
                    // This is a limitation; a full implementation would need to parse $Time$ and $Number$.
                    // For now, we will just download segments listed.
                    // Let's assume SegmentTimeline for now.
                    if (segmentTemplate.segmentTimeline) {
                        let currentTime = 0;
                        for(const s of segmentTemplate.segmentTimeline) {
                            for(let i = 0; i < (s.r || 0) + 1; i++) {
                                const mediaFilename = segmentTemplate.media
                                    .replace('$RepresentationID$', adaptationSet.id)
                                    .replace('$Time$', currentTime);
                                const mediaUrl = new URL(mediaFilename, this.segmentBaseUrl).href;
                                newSegmentQueue.push({ url: mediaUrl, filename: mediaFilename });
                                currentTime += s.d;
                            }
                        }
                    } else if (segmentTemplate.media) {
                         // Fallback for non-timeline templates (less common for live)
                         // This is too complex to parse fully without more info.
                         // We will rely on downloading the *init* segment and rewriting the manifest.
                         // FFmpeg will hopefully read from our rewritten *relative* path.
                    }
                }
            }
        }
        
        // Only add init segments if they are not already downloaded
        for (const segment of newSegmentQueue) {
            if (!this.downloadedSegments.has(segment.filename)) {
                this.segmentQueue.push(segment);
            }
        }
        
        // Prune segmentQueue to a reasonable size (e.g., last 20 segments)
        if(this.segmentQueue.length > 20) {
            this.segmentQueue = this.segmentQueue.slice(-20);
        }

        // Start downloading segments from the queue
        this.downloadSegments();
        // Write the local manifest for FFmpeg to read from
        this.writeLocalMpdManifest(manifest);

        // Resolve the promise only AFTER the first manifest is written
        if (this.resolveInitialPlaylist) {
            console.log('[Buffer-MPD] Initial MPD manifest is ready.');
            this.resolveInitialPlaylist({ localManifestPath: this.localMpdManifestPath });
            this.resolveInitialPlaylist = null;
            this.rejectInitialPlaylist = null;
        }

        // Clean up old segments
        this.cleanupOldSegments();

        // Return refresh interval
        const refreshSeconds = manifest.minimumUpdatePeriod || 5;
        return (refreshSeconds * 1000) / 2; // Refresh at half duration
    }


    /**
     * Generic segment downloader.
     * Iterates this.segmentQueue and downloads new segments.
     */
    async downloadSegments() {
        const segmentsToDownload = this.segmentQueue.filter(s => !this.downloadedSegments.has(s.filename));

        for (const segment of segmentsToDownload) {
            if (this.stopFlag) return;
            
            const localPath = path.join(this.bufferDir, segment.filename);
            
            // Create subdirectories if segment is in one (common in DASH)
            const segmentDir = path.dirname(localPath);
            if (!fs.existsSync(segmentDir)) {
                fs.mkdirSync(segmentDir, { recursive: true });
            }

            try {
                const response = await axios.get(segment.url, { responseType: 'stream', timeout: 5000 });
                const writer = fs.createWriteStream(localPath);
                response.data.pipe(writer);
                
                await new Promise((resolve, reject) => {
                    writer.on('finish', resolve);
                    writer.on('error', reject);
                });

                this.downloadedSegments.add(segment.filename);
                // console.log(`[Buffer] Downloaded segment ${segment.filename}`);
            } catch (error) {
                console.warn(`[Buffer] Failed to download segment ${segment.filename}:`, error.message);
                // Don't stop on single segment failure
            }
        }
    }

    /**
     * Writes a local HLS playlist file for FFmpeg.
     */
    writeLocalHlsPlaylist(playlist) {
        // Filter playlist to only segments we *actually* have downloaded
        const availableSegments = playlist.segments.filter(s => this.downloadedSegments.has(s.uri.split('/').pop()));

        // We only want the *end* of the available list, up to our target buffer size
        const bufferedSegments = availableSegments.slice(-this.targetBufferSegments);
        
        if (bufferedSegments.length === 0) {
            // console.warn('[Buffer-HLS] No buffered segments available to write playlist.');
            return;
        }

        let m3u8Content = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${Math.ceil(playlist.targetDuration)}\n`;
        
        const firstSegmentData = playlist.segments.find(s => s.uri.split('/').pop() === bufferedSegments[0].uri.split('/').pop());
        const mediaSequence = firstSegmentData ? firstSegmentData.mediaSequence : (playlist.mediaSequence || 0);

        m3u8Content += `#EXT-X-MEDIA-SEQUENCE:${mediaSequence}\n`;

        for (const segment of bufferedSegments) {
            if (segment.discontinuity) {
                m3u8Content += '#EXT-X-DISCONTINUITY\n';
            }
            m3u8Content += `#EXTINF:${segment.duration.toFixed(6)},\n`;
            // Point to the *relative path* that Nginx will serve from /var/www/hls/
            m3u8Content += `buffer/${segment.uri.split('/').pop()}\n`; 
        }

        try {
             fs.writeFileSync(this.localHlsPlaylistPath, m3u8Content);
        } catch (e) {
            console.error('[Buffer-HLS] Failed to write local HLS playlist:', e.message);
        }
    }

    /**
     * Writes a local MPD manifest file for FFmpeg.
     */
    writeLocalMpdManifest(manifest) {
        try {
            // Create a deep copy to avoid modifying the original
            let localManifest = JSON.parse(JSON.stringify(manifest));
            
            // Remove baseURL, all paths will be relative to our buffer dir
            delete localManifest.baseURL; 

            for (const period of localManifest.periods) {
                for (const adaptationSet of period.adaptationSets) {
                    const segmentTemplate = adaptationSet.segmentTemplate;
                    if (segmentTemplate) {
                        // --- This is the key: Rewrite paths to point to our buffer directory ---
                        if (segmentTemplate.initialization) {
                            segmentTemplate.initialization = `buffer/${segmentTemplate.initialization}`;
                        }
                        if (segmentTemplate.media) {
                            segmentTemplate.media = `buffer/${segmentTemplate.media}`;
                        }
                    }
                }
            }

            // Convert the modified manifest object back to XML
            const mpdContent = MpdStringify(localManifest);
            fs.writeFileSync(this.localMpdManifestPath, mpdContent);

        } catch (e) {
            console.error('[Buffer-MPD] Failed to write local MPD manifest:', e.message);
        }
    }

    /**
     * Generic segment cleanup logic.
     */
    cleanupOldSegments() {
        // This logic keeps our buffer folder from growing forever
        const segmentsToKeep = new Set(this.segmentQueue.map(s => s.filename));
        
        for (const filename of this.downloadedSegments) {
            if (!segmentsToKeep.has(filename)) {
                try {
                    const localPath = path.join(this.bufferDir, filename);
                    if (fs.existsSync(localPath)) {
                        fs.unlinkSync(localPath);
                        // console.log(`[Buffer] Cleaned up segment ${filename}`);
                    }
                    this.downloadedSegments.delete(filename);
                } catch (e) {
                    // This can fail if a segment is in a subdirectory, fs.unlinkSync won't delete the dir
                    // We'll accept this minor disk leak for now, as the whole folder is deleted on stop.
                    // console.warn(`[Buffer] Failed to cleanup segment ${filename}:`, e.message);
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
    let streamInputUrl = sourceUrl;
    
    // Check if we are restarting ourself (e.g., from a buffer)
    const isLocalPlaylist = sourceUrl.includes('local_playlist.m3u8') || sourceUrl.includes('local_manifest.mpd');

    // --- NEW: MPD/DASH Warning ---
    // Check the *original* source URL for .mpd
    if (sourceUrl.endsWith('.mpd') || sourceUrl.includes('.mpd?')) {
        const testCommand = activeProfile.command.replace(/{streamUrl}/g, "").replace(/{userAgent}/g, "");
        if (!testCommand.includes('-map')) {
            console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
            console.warn('[FFmpeg] WARNING: You are streaming an MPD (DASH) source without a "-map" flag.');
            console.warn('[FFmpeg] This will likely fail by grabbing the lowest quality stream or exiting with an error.');
            console.warn('[FFmpeg] Please use a profile that maps specific video/audio streams (e.g., -map 0:4 -map 0:5).');
            console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        }
    }

    // --- MODIFIED: Buffer Logic now handles HLS and MPD ---
    if (settings.buffer.enabled && !isLocalPlaylist) {
        console.log('[Stream Start] Using Pre-fetch Buffer mode.');
        try {
            bufferManager = new BufferManager(sourceUrl, settings.buffer.delaySeconds);
            
            // --- MODIFIED: This promise now returns a generic path ---
            const { localManifestPath } = await bufferManager.start();
            
            // --- MODIFIED: Construct URL from the returned path ---
            const manifestFilename = path.basename(localManifestPath);
            streamInputUrl = `http://127.0.0.1:8994/${manifestFilename}`;
            
            console.log(`[Stream Start] Buffer is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] Buffer Manager failed to initialize:", error.message);
            stopAllStreamProcesses(); 
            return;
        }
    } else if (isLocalPlaylist) {
        console.log('[Stream Start] Restarting FFmpeg against existing buffer.');
    } else {
        console.log('[Stream Start] Using Direct Stream mode (Buffer disabled in settings).');
    }

    const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
    
    const commandWithPlaceholders = activeProfile.command
        .replace(/{streamUrl}/g, streamInputUrl)
        .replace(/{userAgent}/g, userAgent);

    const args = (commandWithPlaceholders.match(/(?:[^\s"]+|"[^"]*")+/g) || [])
                 .map(arg => arg.replace(/^"|"$/g, ''));
    
    console.log(`[FFmpeg] Starting process with command: ffmpeg ${args.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = sourceUrl; // Store the *original* source URL

    ffmpegProcess.stdout.on('data', (data) => {
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        const stderrStr = data.toString();
        // Suppress verbose logs
        if (!stderrStr.startsWith('frame=') && !stderrStr.startsWith('size=') && 
            !stderrStr.startsWith('Opening') && !stderrStr.includes('dropping overlapping extension') &&
            !stderrStr.includes('dropping stale segment')) {
             console.error(`[ffmpeg stderr]: ${stderrStr.trim()}`);
        }
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        let safeToStop = true;
        if (code !== 0 && code !== 255) { // 255 is normal kill
            if (bufferManager && bufferManager.stopFlag === false) {
                console.warn('[ffmpeg] Process failed. Attempting to restart FFmpeg against buffer...');
                safeToStop = false;
                startStream(streamInputUrl); // Pass the LOCAL manifest URL to restart
            }
        }
        
        if (safeToStop && ffmpegProcess) {
            stopAllStreamProcesses();
        }
    });

    ffmpegProcess.on('error', (err) => {
        console.error('[ffmpeg] Failed to start process:', err);
        stopAllStreamProcesses();
    });
}


// ================================================================
// --- ORIGINAL AUTH & API ENDPOINTS ---
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
        if (err) return res.status(5R00).json({ error: 'Database error' });
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

// --- Stream API Endpoints ---

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
        startStream(url); 
        res.json({ message: 'Stream process initiated successfully' });
    } catch (error) {
        res.status(500).json({ error: 'Failed to start stream', details: error.message });
    }
});

app.post('/api/stop', isAuthenticated, (req, res) => {
    console.log('Stopping stream via API request...');
    stopAllStreamProcesses(); 

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
        running: (ffmpegProcess !== null || bufferManager !== null),
        url: currentStreamUrl 
    });
});

app.get('/api/viewers', isAuthenticated, (req, res) => {
    if (ffmpegProcess === null && bufferManager === null) {
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
    } catch (readErr) {
        // Non-fatal
    }

    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) {
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
