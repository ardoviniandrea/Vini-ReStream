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
const { Parser: HlsParser } = require('m3u8-parser'); // Renamed to HlsParser for clarity
// --- FIX #2: Corrected package name ---
const mpdParser = require('mpd-parser'); // Removed @eyevinn/

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

        // --- NEW: Migration logic to add isDefault flag to old profiles ---
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
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';
const VIEWER_TIMEOUT_MS = 15 * 1000;
const HLS_DIR = '/var/www/hls'; // Main Nginx serve directory
const BUFFER_DIR = path.join(HLS_DIR, 'buffer'); // Subdirectory for segments

// Function to reload nginx config
function reloadNginx() {
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) console.error('[Nginx] Failed to reload nginx:', stderr);
        else console.log('[Nginx] Reloaded successfully.');
    });
}

// Function to clean up all HLS files and local playlists
function cleanupHlsFiles() {
    console.log('[Cleanup] Clearing all temporary stream files...');
    try {
        // Delete main nginx-served playlist
        const filesToDelete = [
            'live.m3u8',
            'local_playlist.m3u8',
            'local_manifest.mpd'
        ];

        fs.readdirSync(HLS_DIR).forEach(file => {
            if (file.endsWith('.ts') || filesToDelete.includes(file)) {
                try {
                    fs.unlinkSync(path.join(HLS_DIR, file));
                    console.log(`[Cleanup] Deleted: ${file}`);
                } catch (e) {
                    console.warn(`[Cleanup] Failed to delete file ${file}: ${e.message}`);
                }
            }
        });

        // Also ensure the buffer directory is gone
        if (fs.existsSync(BUFFER_DIR)) {
            fs.rmSync(BUFFER_DIR, { recursive: true, force: true });
            console.log('[Cleanup] Deleted buffer directory.');
        }

    } catch (e) {
        console.error('[Cleanup] Error during HLS file cleanup:', e.message);
    }
}

// Function to stop all streaming processes
function stopAllStreamProcesses() {
    console.log('[Stream Stop] Stopping all stream processes...');
    if (bufferManager) {
        bufferManager.stop();
        bufferManager = null;
    }
    if (ffmpegProcess) {
        // Use SIGKILL to ensure the process is terminated immediately
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
        this.sourceBaseUrl = ''; // Base URL for relative segment paths
        this.bufferDir = BUFFER_DIR;
        this.stopFlag = false;
        this.timeoutId = null;
        this.downloadedSegments = new Set(); // Tracks filenames we have
        this.initialPlaylistReady = null; // Promise for race condition
        this.resolveInitialPlaylist = null;
        this.rejectInitialPlaylist = null;

        // --- Type-specific properties ---
        this.streamType = this.sourceUrl.includes('.mpd') ? 'mpd' : 'hls';
        this.localManifestPath = ''; // Path to the manifest we generate
        
        // HLS properties
        this.targetBufferSegments = Math.max(3, Math.floor(bufferSeconds / 4)); // Avg 4s segments
        
        // MPD properties
        this.mpdManifest = null; // To store the parsed MPD manifest
        this.segmentUrls = []; // A flat list of ALL segment URLs to fetch

        console.log(`[Buffer] Manager started for ${this.streamType.toUpperCase()} stream.`);
        console.log(`[Buffer] Target buffer: ~${bufferSeconds} seconds.`);

        // Ensure buffer directory exists and is clean
        try {
            if (fs.existsSync(this.bufferDir)) {
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
            }
            fs.mkdirSync(this.bufferDir, { recursive: true });
            console.log('[Buffer] Created clean buffer directory at:', this.bufferDir);
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

        // Set the base URL for resolving relative segment paths
        const urlObj = new URL(this.sourceUrl);
        urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
        this.sourceBaseUrl = urlObj.toString();

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
        console.log('[Buffer] Cleaning up buffer directory on stop...');
        try {
            if (fs.existsSync(this.bufferDir)) {
                fs.rmSync(this.bufferDir, { recursive: true, force: true });
                console.log('[Buffer] Buffer directory deleted.');
            }
        } catch (e) {
            console.error('[Buffer] Failed to delete buffer directory on stop:', e.message);
        }
    }

    /**
     * Main loop, fetches the appropriate manifest.
     */
    async fetchManifest() {
        if (this.stopFlag) return;

        try {
            if (this.streamType === 'hls') {
                await this.fetchHlsManifest();
            } else {
                await this.fetchMpdManifest();
            }
        } catch (error) {
            console.error(`[Buffer] Error fetching source ${this.streamType} manifest:`, error.message || error);
            if (!this.stopFlag) {
                // Retry faster on error
                this.timeoutId = setTimeout(() => this.fetchManifest(), 2000); 
            }
            // FIX: If we fail *before* the first playlist is ready, reject the promise
            if (this.rejectInitialPlaylist) {
                this.rejectInitialPlaylist(error);
                this.rejectInitialPlaylist = null;
                this.resolveInitialPlaylist = null;
            }
        }
    }

    // -------------------------------------------------
    // --- HLS (.m3u8) Specific Logic ---
    // -------------------------------------------------

    async fetchHlsManifest() {
        this.localManifestPath = path.join(HLS_DIR, 'local_playlist.m3u8');
        
        const response = await axios.get(this.sourceUrl, { timeout: 3000 });
        const parser = new HlsParser();
        parser.push(response.data);
        parser.end();

        const playlist = parser.manifest;

        // --- NEW: Handle HLS Master Playlists (Variant Streams) ---
        if (playlist.playlists && playlist.playlists.length > 0) {
            console.log(`[Buffer] Detected HLS Master Playlist with ${playlist.playlists.length} variants.`);
            // Automatically select a variant. Let's pick the last one (often highest quality).
            const selectedVariant = playlist.playlists[playlist.playlists.length - 1];
            // Resolve the new URL relative to the master playlist's base URL
            const newUrl = new URL(selectedVariant.uri, this.sourceBaseUrl).href;

            // Update the manager's source URL and base URL to point to the selected variant stream
            this.sourceUrl = newUrl;
            const urlObj = new URL(newUrl);
            const pathOnly = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
            urlObj.pathname = pathOnly;
            this.sourceBaseUrl = urlObj.toString(); // This is now the base for media segments

            console.log(`[Buffer] Switched to variant stream (resolving to): ${newUrl}`);
            console.log(`[Buffer] New media segment base URL: ${this.sourceBaseUrl}`);

            // Re-run the fetch logic immediately with the new media playlist URL
            this.timeoutId = setTimeout(() => this.fetchManifest(), 100); // Short delay
            return; // Stop processing the current master playlist
        }

        // --- Original Logic (for Media Playlists) ---
        if (!playlist.segments || playlist.segments.length === 0) {
            throw new Error("Source HLS playlist is empty or invalid (not a master and no segments found).");
        }

        // Get *full URIs* for segments
        const segments = playlist.segments.map(s => ({
            ...s,
            fullUri: new URL(s.uri, this.sourceBaseUrl).href,
            filename: s.uri.split('/').pop().split('?')[0] // Get clean filename
        }));

        // Start downloading segments from the queue
        this.downloadSegments(segments); 
        
        // Write the local playlist for FFmpeg to read from
        this.writeLocalHlsPlaylist(playlist, segments);

        // Resolve the promise if this is the first successful run
        if (this.resolveInitialPlaylist) {
            console.log('[Buffer] Initial HLS playlist is ready.');
            this.resolveInitialPlaylist({ localManifestPath: this.localManifestPath });
            this.resolveInitialPlaylist = null; 
            this.rejectInitialPlaylist = null;
        }

        // Schedule the next fetch
        const refreshInterval = (playlist.targetDuration || 4) * 1000;
        this.timeoutId = setTimeout(() => this.fetchManifest(), refreshInterval / 2);
    }

    writeLocalHlsPlaylist(playlist, segments) {
        // Filter playlist to only segments we *actually* have downloaded
        const availableSegments = segments.filter(s => this.downloadedSegments.has(s.filename));

        // We only want the *end* of the available list, up to our target buffer size
        const bufferedSegments = availableSegments.slice(-this.targetBufferSegments);
        
        if (bufferedSegments.length === 0) {
            console.warn('[Buffer] No buffered HLS segments available to write playlist.');
            return; 
        }

        let m3u8Content = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${Math.ceil(playlist.targetDuration)}\n`;
        
        // Find the media sequence of the *first segment we are including*
        // Fallback to 0 if mediaSequence isn't present
        const firstSegment = bufferedSegments[0];
        const mediaSequence = firstSegment.mediaSequence || 
                              (firstSegment.timeline ? firstSegment.timeline : 0); // Use timeline as fallback
        m3u8Content += `#EXT-X-MEDIA-SEQUENCE:${mediaSequence}\n`;

        for (const segment of bufferedSegments) {
            if (segment.discontinuity) {
                m3u8Content += '#EXT-X-DISCONTINUITY\n';
            }
            m3u8Content += `#EXTINF:${segment.duration.toFixed(6)},\n`;
            // Point to the *relative path* that Nginx will serve
            m3u8Content += `buffer/${segment.filename}\n`; 
        }

        // We are creating a non-live playlist for ffmpeg, so we add the endlist tag.
        // FFmpeg is smart enough to reload this file when it changes.
        // m3u8Content += '#EXT-X-ENDLIST\n'; 
        // --- EDIT: Omit endlist, as it causes ffmpeg to exit. 
        // FFmpeg's "-re" flag is what makes it a "live" input.

        try {
             fs.writeFileSync(this.localManifestPath, m3u8Content);
        } catch (e) {
            console.error('[Buffer] Failed to write local HLS playlist:', e.message);
        }
    }


    // -------------------------------------------------
    // --- MPD (DASH) Specific Logic ---
    // -------------------------------------------------

    async fetchMpdManifest() {
        this.localManifestPath = path.join(HLS_DIR, 'local_manifest.mpd');
        
        const response = await axios.get(this.sourceUrl, { timeout: 3000 });
        this.mpdManifest = mpdParser.parse(response.data, {
            sourceUrl: this.sourceUrl // Pass sourceUrl to resolve relative paths
        });

        const segmentsToFetch = [];
        const allSegmentFilenames = new Set();

        // Iterate through all adaptations (video, audio)
        this.mpdManifest.playlists.forEach(playlist => {
            // Add init segment (e.g., init.mp4)
            if (playlist.sidx && playlist.sidx.uri) {
                const filename = playlist.sidx.uri.split('/').pop().split('?')[0];
                segmentsToFetch.push({
                    fullUri: new URL(playlist.sidx.uri, this.sourceBaseUrl).href,
                    filename: filename
                });
                allSegmentFilenames.add(filename);
            }

            // Add all media segments
            playlist.segments.forEach(segment => {
                const filename = segment.uri.split('/').pop().split('?')[0];
                segmentsToFetch.push({
                    ...segment,
                    fullUri: new URL(segment.uri, this.sourceBaseUrl).href,
                    filename: filename
                });
                allSegmentFilenames.add(filename);
            });
        });

        if (segmentsToFetch.length === 0) {
            throw new Error("Source MPD manifest is empty or invalid.");
        }

        // Start downloading all segments
        this.downloadSegments(segmentsToFetch);

        // Write the local manifest file for FFmpeg
        this.writeLocalMpdManifest();

        // Resolve the promise if this is the first successful run
        if (this.resolveInitialPlaylist) {
            console.log('[Buffer] Initial MPD manifest is ready.');
            this.resolveInitialPlaylist({ localManifestPath: this.localManifestPath });
            this.resolveInitialPlaylist = null; 
            this.rejectInitialPlaylist = null;
        }

        // Schedule the next fetch
        const refreshInterval = (this.mpdManifest.minimumUpdatePeriod || 2) * 1000;
        this.timeoutId = setTimeout(() => this.fetchManifest(), Math.max(1000, refreshInterval));
    }

    writeLocalMpdManifest() {
        if (!this.mpdManifest) {
            console.warn('[Buffer] No MPD manifest data to write.');
            return;
        }

        // Deep-clone the manifest to avoid modifying the original
        const localManifest = JSON.parse(JSON.stringify(this.mpdManifest));
        let segmentsAvailable = false;

        // This is the critical part: rewrite all segment URLs
        localManifest.playlists.forEach(playlist => {
            // Rewrite init segment URL
            if (playlist.sidx && playlist.sidx.uri) {
                const filename = playlist.sidx.uri.split('/').pop().split('?')[0];
                if (this.downloadedSegments.has(filename)) {
                    playlist.sidx.uri = `buffer/${filename}`;
                    segmentsAvailable = true;
                }
            }
            // Rewrite all media segment URLs
            playlist.segments.forEach(segment => {
                const filename = segment.uri.split('/').pop().split('?')[0];
                 if (this.downloadedSegments.has(filename)) {
                    segment.uri = `buffer/${filename}`;
                    segmentsAvailable = true;
                }
            });
        });

        if (!segmentsAvailable) {
            console.warn('[Buffer] No buffered MPD segments available to write manifest.');
            return;
        }
        
        try {
            // Convert the modified JSON object back to XML
            const manifestXml = mpdParser.toMpd(localManifest);
            fs.writeFileSync(this.localManifestPath, manifestXml);
        } catch (e) {
            console.error('[Buffer] Failed to write local MPD manifest:', e.message);
        }
    }


    // -------------------------------------------------
    // --- Common Download & Cleanup Logic ---
    // -------------------------------------------------

    /**
     * Downloads segments from a list of segment objects.
     * @param {Array<Object>} segments - Array of segment objects, must have { fullUri, filename }
     */
    async downloadSegments(segments) {
        const segmentsToDownload = [];
        
        for (const segment of segments) {
            if (this.stopFlag) return;
            if (!segment.filename) {
                console.warn('[Buffer] Segment missing filename, skipping:', segment);
                continue;
            }
            
            if (!this.downloadedSegments.has(segment.filename)) {
                segmentsToDownload.push(segment);
            }
        }
        
        // Run downloads in parallel (max 5 at a time)
        const parallelLimit = 5;
        for (let i = 0; i < segmentsToDownload.length; i += parallelLimit) {
            if (this.stopFlag) return;
            const chunk = segmentsToDownload.slice(i, i + parallelLimit);
            await Promise.all(chunk.map(s => this.downloadSegment(s)));
        }

        // Clean up old segments
        this.cleanupOldSegments(segments.map(s => s.filename));
    }

    /**
     * Downloads a single segment and saves it.
     */
    async downloadSegment(segment) {
        if (this.stopFlag || this.downloadedSegments.has(segment.filename)) {
            return;
        }

        const localPath = path.join(this.bufferDir, segment.filename);

        try {
            const response = await axios.get(segment.fullUri, { 
                responseType: 'stream', 
                timeout: 5000 
            });
            const writer = fs.createWriteStream(localPath);
            response.data.pipe(writer);
            
            await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
            });

            this.downloadedSegments.add(segment.filename);
        } catch (error) {
            console.warn(`[Buffer] Failed to download segment ${segment.filename}:`, error.message);
            // If download fails, delete the partial file
            try {
                if (fs.existsSync(localPath)) {
                    fs.unlinkSync(localPath);
                }
            } catch (e) {}
        }
    }

    /**
     * Cleans up segments that are no longer in the manifest.
     */
    cleanupOldSegments(currentSegmentFilenames) {
        const segmentsToKeep = new Set(currentSegmentFilenames); 
        
        for (const filename of this.downloadedSegments) {
            if (!segmentsToKeep.has(filename)) {
                try {
                    const localPath = path.join(this.bufferDir, filename);
                    if (fs.existsSync(localPath)) {
                        fs.unlinkSync(localPath);
                        console.log(`[Cleanup] Deleted stale segment: ${filename}`);
                    }
                    this.downloadedSegments.delete(filename);
                } catch (e) {
                    console.warn(`[Cleanup] Failed to cleanup segment ${filename}:`, e.message);
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
        console.log('[Stream Start] Killing existing ffmpeg process...');
        stopAllStreamProcesses();
    }

    console.log(`[Stream Start] Starting stream from: ${sourceUrl}`);
    const settings = getSettings();
    const activeProfile = getActiveProfile();
    let streamInputUrl = sourceUrl;
    
    // Check if we are restarting FFmpeg against an already-running buffer
    const isRestartingBuffer = sourceUrl.includes('local_playlist.m3u8') || sourceUrl.includes('local_manifest.mpd');
    
    // --- Buffer Logic (Now supports HLS & MPD) ---
    if (settings.buffer.enabled && !isRestartingBuffer) {
        console.log('[Stream Start] Using Pre-fetch Buffer mode.');
        try {
            bufferManager = new BufferManager(sourceUrl, settings.buffer.delaySeconds);
            
            // Wait for the buffer to be ready
            const { localManifestPath } = await bufferManager.start();
            
            // Point FFmpeg to the *local* manifest file served by Nginx
            streamInputUrl = `http://127.0.0.1:8994/${path.basename(localManifestPath)}`;
            console.log(`[Stream Start] Buffer is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] Buffer Manager failed to initialize:", error.message);
            stopAllStreamProcesses(); 
            return; // Don't start ffmpeg if buffer fails
        }
    } else if (isRestartingBuffer) {
        console.log('[Stream Start] Restarting FFmpeg against existing buffer.');
    } else {
        console.log('[Stream Start] Using Direct Stream mode (Buffer disabled in settings).');
    }

    // --- Profile & Command Logic ---
    const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
    
    const commandWithPlaceholders = activeProfile.command
        .replace(/{streamUrl}/g, streamInputUrl)
        .replace(/{userAgent}/g, userAgent);

    // --- MPD/DASH Warning ---
    if (sourceUrl.includes('.mpd') && !commandWithPlaceholders.includes('-map')) {
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        console.warn('[FFmpeg] WARNING: You are streaming an MPD (DASH) source without a "-map" flag.');
        console.warn('[FFmpeg] This will likely fail by grabbing the lowest quality stream or exiting with an error.');
        console.warn('[FFmpeg] Please use a profile that maps specific video/audio streams (e.g., -map 0:4 -map 0:5).');
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
    }

    // Split command string into arguments, respecting quotes
    const args = (commandWithPlaceholders.match(/(?:[^\s"]+|"[^"]*")+/g) || [])
                 .map(arg => arg.replace(/^"|"$/g, '')); // Remove surrounding quotes
    
    console.log(`[FFmpeg] Starting process with command: ffmpeg ${args.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = sourceUrl; // Store the *original* source URL

    ffmpegProcess.stdout.on('data', (data) => {
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        const stderrStr = data.toString();
        // Filter out noisy progress messages
        if (!stderrStr.startsWith('frame=') && !stderrStr.startsWith('size=') && !stderrStr.startsWith('Opening') && !stderrStr.includes('dropping overlapping extension')) {
             console.error(`[ffmpeg stderr]: ${stderrStr.trim()}`);
        }
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        let safeToStop = true; // Flag to prevent recursion
        
        // 255 is 'killed by user' (e.g., from /api/stop)
        if (code !== 0 && code !== 255) { 
            // If ffmpeg failed and the buffer is still running, try to restart ffmpeg
            if (bufferManager && bufferManager.stopFlag === false) {
                console.warn('[ffmpeg] Process failed. Attempting to restart FFmpeg against buffer...');
                safeToStop = false; // Don't stop buffer, we are restarting
                startStream(streamInputUrl); // Pass the LOCAL manifest URL to restart
            }
        }
        
        // If it's safe to stop (e.g., clean exit) or if buffer isn't running
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
// --- ORIGINAL AUTH & API ENDPOINTS (MODIFIED) ---
// ================================================================

// --- Auth API Endpoints ---

app.get('/api/auth/check', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        // --- FIX #1: Corrected typo 5R00 to 500 ---
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
    console.log('[API] Received /api/stop request.');
    stopAllStreamProcesses(); // Use our new global stop function

    try {
        fs.writeFileSync(HLS_LOG_PATH, '', 'utf8');
        fs.writeFileSync(BLOCKLIST_PATH, '', 'utf8');
        console.log('[API] Cleared HLS log and blocklist on stream stop.');
        reloadNginx();
    } catch (writeErr) {
        console.error('[API] Failed to clear logs or blocklist:', writeErr);
    }
    
    res.json({ message: 'Stream stopped' });
});

app.get('/api/status', isAuthenticated, (req, res) => {
    res.json({ 
        running: (ffmpegProcess !== null || bufferManager !== null), // Stream is "running" if buffer or ffmpeg is active
        url: currentStreamUrl 
    });
});

app.get('/api/viewers', isAuthenticated, (req, res) => {
    // MODIFIED: Check buffer OR ffmpeg process
    if (ffmpegProcess === null && bufferManager === null) {
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

// --- FIX: Listen on 127.0.0.1 (localhost) ---
// This ensures the app only accepts connections from the Nginx proxy
// and not directly from the outside world.
app.listen(port, '127.0.0.1', () => {
    console.log(`Stream control API listening on port ${port}`);
});
