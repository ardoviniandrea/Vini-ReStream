const express = require('express');
const { spawn, exec } = require('child_process');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
// --- NEW: Auth & DB Imports ---
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());
// Serve static files from 'public' directory (serves index.html, which is now our login/app page)
app.use(express.static(path.join(__dirname, 'public')));

// --- NEW: DB Setup ---
const DB_PATH = '/data/restream.db';
let db;
try {
    // Ensure the /data directory exists (Docker should handle this, but good to be safe)
    if (!fs.existsSync('/data')) {
        fs.mkdirSync('/data');
    }
    
    db = new sqlite3.Database(DB_PATH, (err) => {
        if (err) {
            console.error('Error opening database:', err.message);
        } else {
            console.log('Connected to the SQLite database at /data/restream.db');
            // Create users table if it doesn't exist
            db.run(`CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL
            )`, (err) => {
                if (err) {
                    console.error("Error creating users table:", err.message);
                } else {
                    console.log("'users' table is ready.");
                }
            });
        }
    });
} catch (dirErr) {
    console.error("Failed to create or access /data directory.", dirErr);
    console.error("Please ensure the /data volume is mounted and writeable by the 'node' user.");
    process.exit(1); // Exit if we can't access the persistent storage
}


// --- NEW: Session Setup ---
const SESSION_SECRET = process.env.SESSION_SECRET || 'supersecretkeyforrestream';

app.use(session({
    store: new SQLiteStore({
        db: 'restream.db', // The name of the db file
        dir: '/data',      // The directory to store it in
        table: 'sessions'
    }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false, // Don't create session until something stored
    cookie: {
        maxAge: 1000 * 60 * 60 * 24 * 7, // 1 week
        httpOnly: true, // Prevent client-side JS from accessing cookie
        secure: false // Set to true if using HTTPS
    }
}));

// --- NEW: Auth Middleware ---
// This middleware will protect routes
const isAuthenticated = (req, res, next) => {
    if (req.session.userId) {
        next(); // User is logged in, proceed
    } else {
        res.status(401).json({ error: 'Unauthorized. Please log in.' });
    }
};


// --- Stream State & Constants ---
let ffmpegProcess = null;
let currentStreamUrl = "";
const HLS_LOG_PATH = '/var/log/nginx/hls_access.log';
const BLOCKLIST_PATH = '/etc/nginx/blocklist.conf';
const VIEWER_TIMEOUT_MS = 15 * 1000; // 15 seconds

// --- Stream Helper Functions ---

// Function to start ffmpeg
function startStream(streamUrl) {
    // (This function's content is identical to your original, so it is collapsed for brevity)
    if (ffmpegProcess) {
        console.log('Killing existing ffmpeg process...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
    }

    console.log(`Starting stream from: ${streamUrl}`);
    
    const args = [
        '-reconnect', '1',
        '-reconnect_streamed', '1',
        '-reconnect_delay_max', '5',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        '-i', streamUrl,
        '-c', 'copy', 
        '-f', 'hls',
        '-hls_time', '4',
        '-hls_list_size', '10',
        '-hls_flags', 'delete_segments+discont_start+omit_endlist', 
        '-hls_segment_filename', '/var/www/hls/segment_%03d.ts',
        '/var/www/hls/live.m3u8'
    ];

    ffmpegProcess = spawn('ffmpeg', args);
    currentStreamUrl = streamUrl;

    ffmpegProcess.stdout.on('data', (data) => {
        // console.log(`ffmpeg stdout: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
        console.error(`ffmpeg stderr: ${data}`);
    });

    ffmpegProcess.on('close', (code) => {
        console.log(`ffmpeg process exited with code ${code}`);
        if (ffmpegProcess) {
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
    // (This function's content is identical to your original)
    exec('supervisorctl -c /etc/supervisor/conf.d/supervisord.conf signal HUP nginx', (err, stdout, stderr) => {
        if (err) {
            console.error('Failed to reload nginx:', stderr);
        } else {
            console.log('Nginx reloaded successfully.');
        }
    });
}


// --- NEW: Auth API Endpoints ---

// Check auth status and if any users exist (for first-run)
app.get('/api/auth/check', (req, res) => {
    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (err) {
            console.error("Auth check DB error:", err);
            return res.status(500).json({ error: 'Database error' });
        }
        
        const hasUsers = row.count > 0;
        if (req.session.userId) {
            // User is logged in
            res.json({
                loggedIn: true,
                username: req.session.username,
                hasUsers: hasUsers
            });
        } else {
            // User is not logged in
            res.json({
                loggedIn: false,
                hasUsers: hasUsers
            });
        }
    });
});

// Register a user
// 1. If NO users exist, anyone can register (first-run setup).
// 2. If users *do* exist, only an authenticated user can register (admin-only).
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
        
        // If users exist, check if the person *making* this request is logged in
        if (hasUsers && !req.session.userId) {
            return res.status(403).json({ error: 'Only an admin can create new users.' });
        }

        // We can proceed. Hash the password
        try {
            const hashedPassword = await bcrypt.hash(password, 10);
            
            db.run("INSERT INTO users (username, password) VALUES (?, ?)", [username, hashedPassword], function(err) {
                if (err) {
                    if (err.message.includes('UNIQUE constraint failed')) {
                        return res.status(409).json({ error: 'Username already taken' });
                    }
                    console.error("Error creating user:", err);
                    return res.status(500).json({ error: 'Error creating user' });
                }
                
                const newUserId = this.lastID;
                
                // If this was the first user, log them in immediately
                if (!hasUsers) {
                    req.session.userId = newUserId;
                    req.session.username = username;
                }
                
                res.status(201).json({ 
                    message: 'User created successfully',
                    id: newUserId,
                    username: username
                });
            });
        } catch (hashErr) {
            console.error("Error hashing password:", hashErr);
            res.status(500).json({ error: 'Error hashing password' });
        }
    });
});

// Login
app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password are required' });
    }

    db.get("SELECT * FROM users WHERE username = ?", [username], async (err, user) => {
        if (err) {
            return res.status(500).json({ error: 'Database error' });
        }
        if (!user) {
            return res.status(401).json({ error: 'Invalid username or password' });
        }

        // User found, check password
        try {
            const isMatch = await bcrypt.compare(password, user.password);
            if (isMatch) {
                // Passwords match! Create session.
                req.session.userId = user.id;
                req.session.username = user.username;
                res.json({ 
                    message: 'Login successful',
                    username: user.username 
                });
            } else {
                // Passwords don't match
                return res.status(401).json({ error: 'Invalid username or password' });
            }
        } catch (compareErr) {
            console.error("Bcrypt compare error:", compareErr);
            return res.status(500).json({ error: "Server error during login" });
        }
    });
});

// Logout
app.post('/api/auth/logout', (req, res) => {
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({ error: 'Failed to log out' });
        }
        res.clearCookie('connect.sid'); // Clear the session cookie
        res.json({ message: 'Logout successful' });
    });
});


// --- NEW: User Management API Endpoints (Protected) ---

// Get all users (omit passwords)
app.get('/api/users', isAuthenticated, (req, res) => {
    db.all("SELECT id, username FROM users ORDER BY username", (err, rows) => {
        if (err) {
            return res.status(500).json({ error: 'Database error fetching users' });
        }
        // Send back all users *except* the currently logged-in one (can't delete self)
        const otherUsers = rows.filter(u => u.id !== req.session.userId);
        res.json(otherUsers);
    });
});

// Delete a user
app.delete('/api/users/:id', isAuthenticated, (req, res) => {
    const userIdToDelete = parseInt(req.params.id, 10);
    
    // Protect against deleting yourself
    if (req.session.userId === userIdToDelete) {
         return res.status(400).json({ error: 'You cannot delete yourself.' });
    }

    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (err) {
            return res.status(500).json({ error: 'Database error' });
        }
        if (row.count <= 1) {
            return res.status(400).json({ error: 'Cannot delete the last user.' });
        }

        // Proceed with deletion
        db.run("DELETE FROM users WHERE id = ?", [userIdToDelete], function(err) {
            if (err) {
                return res.status(500).json({ error: 'Failed to delete user' });
            }
            if (this.changes === 0) {
                return res.status(404).json({ error: 'User not found' });
            }
            res.json({ message: 'User deleted successfully' });
        });
    });
});


// --- Stream API Endpoints (NOW PROTECTED) ---

app.post('/api/start', isAuthenticated, (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'Missing "url" in request body' });
    }

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

app.post('/api/stop', isAuthenticated, (req, res) => {
    if (ffmpegProcess) {
        console.log('Stopping stream via API request...');
        ffmpegProcess.kill('SIGKILL');
        ffmpegProcess = null;
        currentStreamUrl = "";

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

app.get('/api/status', isAuthenticated, (req, res) => {
    // Report if the process is running and what URL it's using
    res.json({ 
        running: (ffmpegProcess !== null),
        url: currentStreamUrl 
    });
});

app.get('/api/viewers', isAuthenticated, (req, res) => {
    // (This function's content is identical to your original, so it is collapsed for brevity)
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
            const timestampStr = match[2].replace('/', ' ').replace('/', ' ').replace(':', ' ');
            const timestamp = Date.parse(timestampStr);

            if (isNaN(timestamp)) {
                console.warn(`Could not parse timestamp: ${match[2]}`);
                continue;
            }

            const viewer = viewers.get(ip) || { 
                ip, 
                firstSeen: timestamp, 
                lastSeen: timestamp,
                isBlocked: blockedIps.has(ip)
            };

            if (timestamp > viewer.lastSeen) {
                viewer.lastSeen = timestamp;
            }
            if (timestamp < viewer.firstSeen) {
                viewer.firstSeen = timestamp;
            }
            viewer.isBlocked = blockedIps.has(ip); 
            viewers.set(ip, viewer);
        }

        const activeViewers = Array.from(viewers.values()).filter(v => 
            (now - v.lastSeen) < VIEWER_TIMEOUT_MS
        );

        activeViewers.sort((a, b) => b.lastSeen - a.lastSeen);

        res.json(activeViewers);
    });
});

app.post('/api/terminate', isAuthenticated, (req, res) => {
    // (This function's content is identical to your original, so it is collapsed for brevity)
    const { ip } = req.body;
    if (!ip) {
        return res.status(400).json({ error: 'Missing "ip" in request body' });
    }

    fs.readFile(BLOCKLIST_PATH, 'utf8', (readErr, data) => {
        if (readErr) {
            console.error('Failed to read blocklist:', readErr);
            return res.status(500).json({ error: 'Failed to read blocklist' });
        }

        if (data.includes(`deny ${ip};`)) {
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


// Serve the index.html for the root route (handled by express.static)
// The original file had this, so we keep it as a fallback.
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, '127.0.0.1', () => {
    // Listens on localhost only, Nginx will handle public traffic
    console.log(`Stream control API listening on port ${port}`);
});
