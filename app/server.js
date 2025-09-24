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
const mpdParser = require('mpd-parser'); 
const xmlBuilder = require('xml-js'); 

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
                // Note: The index in -map is based on the DASH stream structure (AdaptationSet 4 for video, 5 for audio in Akamai streams)
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -map 0:3 -map 0:4 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            },
            {
                id: 'mpd-720p-copy',
                name: 'MPD/DASH 720p (Stream Copy)',
                command: '-user_agent "{userAgent}" -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -i "{streamUrl}" -map 0:2 -map 0:4 -c copy -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            },
            {
                id: 'mpd-1080p-nvenc',
                name: 'MPD/DASH 1080p (NVIDIA NVENC)',
                command: '-hwaccel nvdec -user_agent "{userAgent}" -i "{streamUrl}" -map 0:3 -map 0:4 -c:a copy -c:v h264_nvenc -preset p6 -tune hq -f hls -hls_time 4 -hls_list_size 10 -hls_flags delete_segments+discont_start+omit_endlist -hls_segment_filename /var/www/hls/segment_%03d.ts /var/www/hls/live.m3u8',
                active: false,
                isDefault: true
            }
        ],
        buffer: {
            enabled: true,
            delaySeconds: 30
        },
        activeProfileId: 'default-cpu' // Set a default active ID
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
            return getDefaultSettings(); 
        }
    }
    try {
        const settingsData = fs.readFileSync(SETTINGS_PATH, 'utf8');
        let settings = JSON.parse(settingsData);

        // Migration logic to ensure new default flags are present
        let needsSave = false;
        const defaultProfiles = getDefaultSettings().profiles;
        const defaultIds = defaultProfiles.map(p => p.id);

        for (const profile of settings.profiles) {
            if (defaultIds.includes(profile.id) && profile.isDefault !== true) {
                profile.isDefault = true;
                needsSave = true;
            } else if (!defaultIds.includes(profile.id) && profile.isDefault === undefined) {
                 profile.isDefault = false;
                 needsSave = true;
            }
        }
        
        // Ensure default profiles exist in settings if they were somehow deleted
        for (const defaultP of defaultProfiles) {
            if (!settings.profiles.find(p => p.id === defaultP.id)) {
                settings.profiles.push(defaultP);
                needsSave = true;
            }
        }
        
        if (!settings.activeProfileId && settings.profiles.length > 0) {
            settings.activeProfileId = settings.profiles[0].id;
            needsSave = true;
        }


        if (needsSave) {
            console.log('Migrating settings to ensure profile integrity...');
            saveSettings(settings);
        }
        
        return settings;
    } catch (e) {
        console.error("Failed to parse settings.json, returning defaults:", e);
        return getDefaultSettings(); 
    }
}


function saveSettings(settings) {
    try {
        // Ensure active flag is synced for old clients/debugging
        settings.profiles.forEach(p => {
            p.active = (p.id === settings.activeProfileId);
        });

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
        activeProfile = settings.profiles.find(p => p.isDefault);
    }
    return activeProfile || getDefaultSettings().profiles[0];
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

    let activeId = newSettings.activeProfileId;
    if (!activeId || !newSettings.profiles.find(p => p.id === activeId)) {
        activeId = newSettings.profiles[0]?.id;
    }
    newSettings.activeProfileId = activeId;
    
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
const HLS_DIR = '/var/www/hls'; 
const BUFFER_DIR = path.join(HLS_DIR, 'buffer'); 

// Function to reload nginx config
function reloadNginx() {
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) console.error('[Nginx] Failed to reload nginx:', stderr);
        else console.log('[Nginx] Reloaded successfully.');
    });
}

/**
 * Function to clean up all generated stream files.
 */
function cleanupStreamFiles() {
    console.log('[Cleanup] Clearing all temporary stream files...');
    try {
        // 1. Delete main playlists/manifests in HLS_DIR
        const filesToDelete = [
            'live.m3u8',
            'local_playlist.m3u8',
            'local_manifest.mpd' // Target for MPD buffer
        ];
        filesToDelete.forEach(filename => {
            try {
                fs.unlinkSync(path.join(HLS_DIR, filename));
                console.log(`[Cleanup] Deleted: ${filename}`);
            } catch (e) {
                // Ignore ENOENT (file not found)
                if (e.code !== 'ENOENT') { 
                    console.warn(`[Cleanup] Failed to delete file ${filename}: ${e.message}`);
                }
            }
        });

        // 2. Recursively delete the entire buffer directory
        if (fs.existsSync(BUFFER_DIR)) {
            // NOTE: Use fs.rmSync({ recursive: true, force: true }) if you are on Node 14.14.0+
            fs.rmSync(BUFFER_DIR, { recursive: true, force: true });
            console.log('[Cleanup] Deleted buffer directory.');
        }

    } catch (e) {
        console.error('[Cleanup] Error during file cleanup:', e.message);
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
        ffmpegProcess.kill('SIGKILL'); 
        ffmpegProcess = null;
    }
    currentStreamUrl = "";
    cleanupStreamFiles(); // Using the new generic cleanup function
}


// ================================================================
// --- PRE-FETCH BUFFER MANAGER (HLS & MPD) ---
// ================================================================

// Constant User Agent to use for all external HTTP requests (FFmpeg uses this too)
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";


class BufferManager {
    constructor(sourceUrl, bufferSeconds) {
        this.sourceUrl = sourceUrl;
        this.sourceBaseUrl = ''; 
        this.bufferDir = BUFFER_DIR;
        this.stopFlag = false;
        this.timeoutId = null;
        this.downloadedSegments = new Set(); 
        this.initialManifestReady = null; 
        this.resolveInitialManifest = null;
        this.rejectInitialManifest = null;

        // Determine stream type
        this.streamType = this.sourceUrl.toLowerCase().includes('.mpd') ? 'mpd' : 'hls';
        this.localManifestPath = path.join(HLS_DIR, (this.streamType === 'mpd' ? 'local_manifest.mpd' : 'local_playlist.m3u8'));
        
        // Target buffer 5 segments minimum (assuming ~4 sec segments typical for HLS/DASH)
        this.targetBufferSegments = Math.max(5, Math.floor(bufferSeconds / 4)); 
        
        // Ensure buffer directory is clean and exists before starting operations
        if (fs.existsSync(this.bufferDir)) {
             fs.rmSync(this.bufferDir, { recursive: true, force: true });
        }
        fs.mkdirSync(this.bufferDir, { recursive: true });
        console.log('[Buffer] Created clean buffer directory at:', this.bufferDir);
    }

    start() {
        this.initialManifestReady = new Promise((resolve, reject) => {
            this.resolveInitialManifest = resolve;
            this.rejectInitialManifest = reject;
        });

        const urlObj = new URL(this.sourceUrl);
        // Set the base URL to the directory containing the manifest
        urlObj.pathname = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
        this.sourceBaseUrl = urlObj.toString();

        this.fetchManifest(); 
        return this.initialManifestReady; 
    }

    stop() {
        this.stopFlag = true;
        if (this.timeoutId) {
            clearTimeout(this.timeoutId);
        }
        console.log('[Buffer] Manager stopped.');
    }

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
                // Retry fetch if token hasn't expired and it's not a definitive 4xx error
                const retryInterval = (error.response && error.response.status >= 400 && error.response.status < 500) ? 0 : 5000;
                this.timeoutId = setTimeout(() => this.fetchManifest(), retryInterval); 
            }
            if (this.rejectInitialManifest) {
                // Only reject on initial fetch failure
                this.rejectInitialManifest(error);
                this.rejectInitialManifest = null;
                this.resolveInitialManifest = null;
            }
        }
    }

    // -------------------------------------------------
    // --- HLS (.m3u8) Specific Logic ---
    // -------------------------------------------------

    async fetchHlsManifest() {
        const response = await axios.get(this.sourceUrl, { 
            timeout: 5000, 
            headers: { 'User-Agent': USER_AGENT } // Added User-Agent
        });
        const parser = new HlsParser();
        parser.push(response.data);
        parser.end();
        const playlist = parser.manifest;

        // Handle HLS Master Playlists (Select the highest bandwidth variant)
        if (playlist.playlists && playlist.playlists.length > 0) {
            const variant = playlist.playlists.reduce((max, p) => 
                (p.attributes.BANDWIDTH > max.attributes.BANDWIDTH) ? p : max, playlist.playlists[0]);
            
            // Reconstruct the new URL, ensuring the original query parameters (the token) are preserved
            const originalUrl = new URL(this.sourceUrl);
            const variantUrl = new URL(variant.uri, this.sourceBaseUrl);
            variantUrl.search = originalUrl.search; // Preserve original query params (token)

            this.sourceUrl = variantUrl.href;
            
            const urlObj = new URL(this.sourceUrl);
            const pathOnly = urlObj.pathname.substring(0, urlObj.pathname.lastIndexOf('/') + 1);
            urlObj.pathname = pathOnly;
            this.sourceBaseUrl = urlObj.toString(); 

            console.log(`[Buffer] Switched to HLS variant: ${this.sourceUrl.split('?')[0]}...`);
            this.timeoutId = setTimeout(() => this.fetchManifest(), 100); 
            return; 
        }

        if (!playlist.segments || playlist.segments.length === 0) {
            throw new Error("Source HLS playlist is empty or invalid.");
        }

        const segments = playlist.segments.map(s => {
            // Append original query parameters to the full URI for authenticated segment download
            const originalUrl = new URL(this.sourceUrl);
            const segmentUrl = new URL(s.uri, this.sourceBaseUrl);
            segmentUrl.search = originalUrl.search;
            
            // Clean filename by dropping all query parameters, including the original ones.
            const filename = s.uri.split('/').pop().split('?')[0];

            return {
                ...s,
                fullUri: segmentUrl.href,
                filename: filename
            };
        });

        await this.downloadSegments(segments); 
        
        const manifestWritten = this.writeLocalHlsPlaylist(playlist, segments);

        if (this.resolveInitialManifest && manifestWritten) {
            console.log('[Buffer] Initial HLS playlist is ready.');
            this.resolveInitialManifest({ localManifestPath: this.localManifestPath });
            this.resolveInitialManifest = null; 
            this.rejectInitialManifest = null;
        }

        const refreshInterval = (playlist.targetDuration || 4) * 1000;
        this.timeoutId = setTimeout(() => this.fetchManifest(), refreshInterval / 2);
    }

    writeLocalHlsPlaylist(playlist, segments) {
        // Filter playlist to only segments we *actually* have downloaded
        const availableSegments = segments.filter(s => this.downloadedSegments.has(s.filename));

        // Use the last N segments available in the buffer
        const bufferedSegments = availableSegments.slice(-this.targetBufferSegments);
        
        if (bufferedSegments.length === 0 && this.downloadedSegments.size === 0) {
            console.warn('[Buffer] No buffered HLS segments available to write playlist.');
            return false;
        }
        
        // Use all downloaded segments if we haven't reached the target buffer yet,
        // but ensure we never exceed what's safe for the player.
        const playlistSegments = bufferedSegments.length > 0 ? bufferedSegments : Array.from(this.downloadedSegments.values()).map(f => segments.find(s => s.filename === f)).filter(Boolean);

        if (playlistSegments.length === 0) {
             console.warn('[Buffer] No buffered HLS segments available for playlist.');
             return false;
        }

        let m3u8Content = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${Math.ceil(playlist.targetDuration || 4)}\n`;
        
        const firstSegment = playlistSegments[0];
        // Use sequence number 0 or the first segment's explicit sequence number
        const mediaSequence = firstSegment.mediaSequence || 0; 
        m3u8Content += `#EXT-X-MEDIA-SEQUENCE:${mediaSequence}\n`;

        for (const segment of playlistSegments) {
            if (segment.discontinuity) {
                m3u8Content += '#EXT-X-DISCONTINUITY\n';
            }
            m3u8Content += `#EXTINF:${(segment.duration || 4).toFixed(6)},\n`;
            // Point to the relative path served by Nginx: /buffer/segment_name
            m3u8Content += `buffer/${segment.filename}\n`; 
        }
        
        // Add ENDLIST if stream is finished (unlikely for live, but good practice)
        if (playlist.endList) {
            m3u8Content += '#EXT-X-ENDLIST\n';
        }

        try {
             fs.writeFileSync(this.localManifestPath, m3u8Content);
             return true;
        } catch (e) {
            console.error('[Buffer] Failed to write local HLS playlist:', e.message);
            return false;
        }
    }


    // -------------------------------------------------
    // --- MPD (DASH) Specific Logic (FIXED/Simplified) ---
    // -------------------------------------------------

    async fetchMpdManifest() {
        const response = await axios.get(this.sourceUrl, { 
            timeout: 5000, 
            headers: { 'User-Agent': USER_AGENT } // Added User-Agent
        });
        const manifestXml = response.data; // Keep the raw XML for rewriting
        
        const mpdManifest = mpdParser.parse(manifestXml, {
            sourceUrl: this.sourceUrl 
        });

        // 1. Identify all segments to download
        const segmentsToFetch = [];
        mpdManifest.playlists.forEach(playlist => {
            // Init segment
            if (playlist.sidx && playlist.sidx.uri) {
                const fullUri = new URL(playlist.sidx.uri, this.sourceBaseUrl).href;
                const filename = fullUri.split('/').pop().split('?')[0]; 
                // Preserve the full URL for downloading (including token)
                segmentsToFetch.push({ fullUri, filename, type: 'init' }); 
            }

            // Media segments (only take the last N segments available in the original manifest)
            const mediaSegments = playlist.segments.slice(-this.targetBufferSegments);
            mediaSegments.forEach(segment => {
                const fullUri = new URL(segment.uri, this.sourceBaseUrl).href;
                const filename = fullUri.split('/').pop().split('?')[0]; 
                segmentsToFetch.push({ fullUri, filename, type: 'media' });
            });
        });

        if (segmentsToFetch.length === 0) {
            throw new Error("Source MPD manifest is empty or invalid (no segments found).");
        }
        
        // 2. Download all identified segments
        await this.downloadSegments(segmentsToFetch);
        
        // 3. Rewrite the raw XML manifest to point to local files
        const manifestWritten = this.writeLocalMpdManifest(manifestXml, mpdManifest);

        if (this.resolveInitialManifest && manifestWritten) {
            console.log('[Buffer] Initial MPD manifest is ready.');
            this.resolveInitialManifest({ localManifestPath: this.localManifestPath });
            this.resolveInitialManifest = null; 
            this.rejectInitialManifest = null;
        }

        const refreshInterval = (mpdManifest.minimumUpdatePeriod || 2) * 1000;
        this.timeoutId = setTimeout(() => this.fetchManifest(), Math.max(1000, refreshInterval));
    }

    /**
     * Rewrites the MPD XML string to point all segment/init URIs to the local buffer.
     * NOTE: This logic is tricky due to the structure. We are simplifying it to only 
     * rewrite known attributes, ignoring parts that should stay remote (like BaseURL).
     * @param {string} rawXml - The original MPD XML text.
     */
    writeLocalMpdManifest(rawXml) {
        let segmentCount = 0;
        const localPrefix = `buffer/`; // Relative path served by Nginx

        // 1. Convert to a manipulatable JSON structure using xml-js
        let mpdJson;
        try {
            mpdJson = JSON.parse(xmlBuilder.xml2json(rawXml, { compact: false, spaces: 4 }));
        } catch (e) {
            console.error('[Buffer] Failed to parse MPD XML to JSON for rewriting:', e.message);
            return false;
        }

        // Helper function to recursively find and replace URIs
        const rewriteUris = (obj) => {
            if (typeof obj !== 'object' || obj === null) return;

            // Target 1: <BaseURL> tags (strip content as segments are now relative to the Nginx root via /buffer/)
            if (obj.name === 'BaseURL' && obj.elements && obj.elements[0] && obj.elements[0].type === 'text') {
                 // Clear original remote BaseURL
                 obj.elements[0].text = ''; 
            }
            
            // Target 2: Initialization and media attributes (SegmentTemplate, SegmentURL)
            if (obj.attributes) {
                const attributes = obj.attributes;
                const uriKeys = ['initialization', 'media', 'sourceURL']; 
                
                uriKeys.forEach(key => {
                    if (attributes[key]) {
                        // Extract filename by stripping path and query 
                        const fullUri = attributes[key].includes('http') ? attributes[key] : new URL(attributes[key], this.sourceBaseUrl).href;
                        const filename = fullUri.split('/').pop().split('?')[0];

                        // Check if the segment for this filename was successfully downloaded
                        if (this.downloadedSegments.has(filename)) {
                            // Point the attribute to the local buffer path
                            attributes[key] = `${localPrefix}${filename}`;
                            segmentCount++;
                        } 
                        // If not downloaded, we keep the original remote URL.
                        // However, since this is DASH/MPD live streams, we must strip
                        // the auth portion (`?hdnts=...`) from the URL for the segment to work 
                        // in a player if it wasn't pre-fetched. We assume if we failed to 
                        // pre-fetch it, the original auth token is bad, so we'll 
                        // force local reference anyway to avoid future token issues.
                        // If the segment wasn't downloaded, we assume it's one outside 
                        // our current buffer window and let the client player try to fetch it.
                        // We must ensure the URL does *not* contain the token if it's pointing remotely.
                        // For maximum compatibility, we enforce the local path for all manifest segments 
                        // and trust the pre-fetch step (which uses the full token) succeeded.
                        if (!attributes[key].startsWith(localPrefix)) {
                            attributes[key] = `${localPrefix}${filename}`;
                            segmentCount++;
                        }
                    }
                });
            }

            // Recurse through all child elements
            if (obj.elements) {
                obj.elements.forEach(rewriteUris);
            }
        };

        // Start traversal from the root element
        if (mpdJson.elements && mpdJson.elements.length > 0) {
            rewriteUris(mpdJson.elements[0]);
        }
        
        // 2. Convert the modified JSON structure back to XML
        let modifiedXml;
        try {
            modifiedXml = xmlBuilder.json2xml(mpdJson, { compact: false, spaces: 4 });
        } catch (e) {
            console.error('[Buffer] Failed to convert JSON back to MPD XML:', e.message);
            return false;
        }

        try {
            fs.writeFileSync(this.localManifestPath, modifiedXml);
            console.log(`[Buffer] Successfully wrote local MPD manifest. Rewrote ${segmentCount} segment references.`);
            return true;
        } catch (e) {
            console.error('[Buffer] Failed to write local MPD manifest:', e.message);
            return false;
        }
    }


    // -------------------------------------------------
    // --- Common Download & Cleanup Logic (FIXED) ---
    // -------------------------------------------------

    async downloadSegments(segments) {
        const segmentsToDownload = [];
        
        // Filter out segments that are already downloaded
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
        
        const parallelLimit = 5;
        // Download segments in chunks to prevent resource exhaustion
        for (let i = 0; i < segmentsToDownload.length; i += parallelLimit) {
            if (this.stopFlag) return;
            const chunk = segmentsToDownload.slice(i, i + parallelLimit);
            
            const downloadPromises = chunk.map(s => this.downloadSegment(s));
            await Promise.all(downloadPromises);
        }

        // Clean up old segments that are no longer in the manifest
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
        
        // Defensive check: Ensure the buffer directory exists before attempting to write (FIX for ENOENT)
        try {
            if (!fs.existsSync(this.bufferDir)) {
                 fs.mkdirSync(this.bufferDir, { recursive: true });
            }
        } catch(e) {
            console.error('[Buffer] CRITICAL: Failed to ensure buffer directory exists:', e.message);
            return; 
        }

        // Check if the file already exists (e.g., from a previous run or refresh)
        // If it exists, assume it's valid and add to downloadedSegments set
        if (fs.existsSync(localPath)) {
            this.downloadedSegments.add(segment.filename);
            return;
        }

        try {
            // Include the User-Agent header here!
            const response = await axios.get(segment.fullUri, { 
                responseType: 'stream', 
                timeout: 10000, // Increased timeout for segment fetching
                headers: { 'User-Agent': USER_AGENT }
            });
            const writer = fs.createWriteStream(localPath);
            response.data.pipe(writer);
            
            await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
            });

            this.downloadedSegments.add(segment.filename);
        } catch (error) {
            // Note: Akamai links often fail quickly if the token expires.
            console.warn(`[Buffer] Failed to download segment ${segment.filename}. URI: ${segment.fullUri.split('?')[0]}... Error:`, error.message);
            // Delete partial file on failure
            try {
                if (fs.existsSync(localPath)) {
                    fs.unlinkSync(localPath);
                }
            } catch (e) {}
        }
    }

    /**
     * Cleans up segments that are no longer referenced by the current live manifest.
     */
    cleanupOldSegments(currentSegmentFilenames) {
        const segmentsToKeep = new Set(currentSegmentFilenames); 
        
        // Create an array from the set of downloaded segments to iterate over
        const downloadedArray = Array.from(this.downloadedSegments);

        for (const filename of downloadedArray) {
            // Skip deletion if the stream is DASH/MPD and the segment is an initialization segment (init.mp4)
            // or if the segment is currently referenced in the manifest to prevent race conditions.
            if (!segmentsToKeep.has(filename) && !filename.endsWith('.mp4')) {
                try {
                    const localPath = path.join(this.bufferDir, filename);
                    if (fs.existsSync(localPath)) {
                        fs.unlinkSync(localPath);
                        console.log(`[Cleanup] Deleted stale segment: ${filename}`);
                    }
                    this.downloadedSegments.delete(filename);
                } catch (e) {
                    console.warn(`[Cleanup] Failed to cleanup segment ${filename}: ${e.message}`);
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

    console.log(`[Stream Start] Starting stream from: ${sourceUrl.split('?')[0]}...`);
    const settings = getSettings();
    const activeProfile = getActiveProfile();
    let streamInputUrl = sourceUrl;
    
    // Check if we are restarting FFmpeg against an already-running buffer
    const isRestartingBuffer = sourceUrl.includes('local_playlist.m3u8') || sourceUrl.includes('local_manifest.mpd');
    
    // --- Buffer Logic (Now supports HLS & MPD via local manifest) ---
    if (settings.buffer.enabled && !isRestartingBuffer) {
        console.log('[Stream Start] Using Pre-fetch Buffer mode.');
        try {
            bufferManager = new BufferManager(sourceUrl, settings.buffer.delaySeconds);
            
            // Wait for the buffer to be ready
            const promiseResult = await bufferManager.start();
            
            // Point FFmpeg to the *local* manifest file served by Nginx
            streamInputUrl = `http://127.0.0.1:8994/${path.basename(promiseResult.localManifestPath)}`;
            console.log(`[Stream Start] Buffer is ready. Pointing FFmpeg to: ${streamInputUrl}`);

        } catch (error) {
            console.error("[Stream Start] Buffer Manager failed to initialize. Aborting stream start.");
            bufferManager = null; 
            throw new Error(`Buffer initialization failed: ${error.message}`);
        }
    } else if (isRestartingBuffer) {
        console.log('[Stream Start] Restarting FFmpeg against existing buffer.');
    } else {
        console.log('[Stream Start] Using Direct Stream mode (Buffer disabled in settings).');
    }

    // --- Profile & Command Logic ---
    const userAgent = USER_AGENT;
    
    const commandWithPlaceholders = activeProfile.command
        .replace(/{streamUrl}/g, streamInputUrl)
        .replace(/{userAgent}/g, userAgent);

    if (sourceUrl.toLowerCase().includes('.mpd') && !commandWithPlaceholders.includes('-map')) {
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        console.warn('[FFmpeg] WARNING: Streaming an MPD (DASH) source without a "-map" flag.');
        console.warn('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
    }

    // Split command string into arguments, respecting quotes
    const args = (commandWithPlaceholders.match(/(?:[^\s"]+|"[^"]*")+/g) || [])
                 .map(arg => arg.replace(/^"|"$/g, ''));
    
    console.log(`[FFmpeg] Starting process with command: ffmpeg ${args.join(' ')}`);

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = sourceUrl; 

    ffmpegProcess.stdout.on('data', (data) => {
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        const stderrStr = data.toString();
        // Filter out verbose frame/size logs
        if (!stderrStr.startsWith('frame=') && !stderrStr.startsWith('size=') && !stderrStr.startsWith('Opening') && !stderrStr.includes('dropping overlapping extension')) {
             console.error(`[ffmpeg stderr]: ${stderrStr.trim()}`);
        }
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`[ffmpeg] process exited with code ${code}`);
        let safeToStop = true; 
        
        if (code !== 0 && code !== 255) { 
            // Attempt restart only if we're using the buffer and the buffer itself is still running
            if (bufferManager && bufferManager.stopFlag === false) {
                console.warn('[ffmpeg] Process failed. Attempting to restart FFmpeg against buffer...');
                safeToStop = false; 
                // We restart FFmpeg, pointing it back to the local proxy URL, but we don't restart the BufferManager itself
                startStream(streamInputUrl); 
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
// --- ORIGINAL AUTH & API ENDPOINTS (No Changes) ---
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

// --- Stream API Endpoints ---

app.post('/api/start', isAuthenticated, async (req, res) => {
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
        // Start the stream process. It's now asynchronous due to the buffer initialization wait.
        // We catch initialization errors here and send a failure response if the buffer fails.
        await startStream(url); 
        res.json({ message: 'Stream process initiated successfully' });
    } catch (error) {
        console.error(`[API] Failed to start stream process: ${error.message}`);
        // Ensure everything is cleaned up if initialization fails mid-way
        stopAllStreamProcesses(); 
        res.status(500).json({ error: 'Failed to start stream process, check server logs.', details: error.message });
    }
});


app.post('/api/stop', isAuthenticated, (req, res) => {
    console.log('[API] Received /api/stop request.');
    stopAllStreamProcesses(); 

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
        // Non-fatal, just log
    }

    fs.readFile(HLS_LOG_PATH, 'utf8', (err, data) => {
        if (err) {
            return res.json([]);
        }

        const lines = data.split('\n').filter(line => line.trim() !== '');
        const viewers = new Map();
        const now = Date.now();
        const logRegex = /([\d\.:a-f]+) - \[[^\]]+\]/g; // Updated regex to better match IP

        for (const line of lines) {
            const match = line.match(logRegex);
            if (!match) continue;

            const ip = match[1];
            // Get timestamp string (the content between the first pair of [])
            const timestampMatch = line.match(/\[([^\]]+)\]/);
            if (!timestampMatch) continue;
            
            // Format example: 20/Sep/2025:20:20:59 +0000 -> 20/Sep/2025 20:20:59 +0000
            const timestampStr = timestampMatch[1].replace(':', ' ').replace(/\//g, ' ');
            const timestamp = Date.parse(timestampStr.replace(' ', ',')); // Crude parsing fix
            
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
        if (readErr && readErr.code !== 'ENOENT') return res.status(500).json({ error: 'Failed to read blocklist' });
        
        let blocklistContent = data || '';
        if (blocklistContent.includes(`deny ${ip};`)) {
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
