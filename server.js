const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const crypto = require('crypto');
const { Client } = require('pg');

// Database client and in-memory cache. If DATABASE_URL is provided,
// data will be loaded from and persisted to PostgreSQL instead of the
// local JSON file. This allows the application to survive instance
// restarts and prevents data loss when the server sleeps.
let dbClient = null;
let dbData = null;

/**
 * Initialise a PostgreSQL connection and load the application state
 * from the database. When using a database, all subsequent reads and
 * writes go through the dbData object to keep an in-memory copy in sync.
 */
async function initDb() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    // No database configured – fallback to file-based storage
    return;
  }
  dbClient = new Client({
    connectionString: databaseUrl,
    ssl: { rejectUnauthorized: false }
  });
  try {
    await dbClient.connect();
    // Ensure table exists
    await dbClient.query(`
      CREATE TABLE IF NOT EXISTS app_state (
        id INTEGER PRIMARY KEY,
        data JSONB NOT NULL
      );
    `);
    // Load existing state or insert a default row
    const result = await dbClient.query('SELECT data FROM app_state WHERE id = 1');
    if (result.rows && result.rows.length > 0) {
      dbData = result.rows[0].data;
    } else {
      // If no row exists, read from file and insert into DB
      const fileData = readDataFromFile();
      dbData = fileData;
      await dbClient.query('INSERT INTO app_state (id, data) VALUES (1, $1::jsonb)', [JSON.stringify(fileData)]);
    }
    console.log('Database initialised – using persistent storage');
  } catch (err) {
    console.error('Failed to initialise database:', err);
    // On failure, fall back to file-based storage
    dbClient = null;
    dbData = null;
  }
}

/**
 * Helper to read the data file directly. This bypasses the database logic
 * and should only be used during initialisation or when no database is
 * configured.
 */
function readDataFromFile() {
  if (!fs.existsSync(DATA_FILE)) {
    return {
      users: [],
      nextUserId: 1,
      nextSellerNumber: 1,
      appointments: [],
      nextAppointmentId: 1,
      bookings: [],
      nextBookingId: 1
    };
  }
  const raw = fs.readFileSync(DATA_FILE, 'utf8');
  try {
    return JSON.parse(raw);
  } catch (err) {
    console.error('Failed to parse data file:', err);
    return {
      users: [],
      nextUserId: 1,
      nextSellerNumber: 1,
      appointments: [],
      nextAppointmentId: 1,
      bookings: [],
      nextBookingId: 1
    };
  }
}

/*
 * This server implements a simple appointment booking system for two types of
 * users: buyers and sellers.  Buyers and sellers can register accounts,
 * authenticate, reset their passwords and manage appointments/bookings.  All
 * data is persisted to a JSON file on disk (data.json) for ease of setup and
 * portability.  The interface is delivered as standard HTML pages with a
 * minimal amount of client‑side JavaScript to handle real–time updates via
 * server–sent events (SSE).  No external dependencies are used – everything
 * relies on Node's built‑in modules so that the application can be run
 * anywhere without installing additional packages.
 */

const DATA_FILE = path.join(__dirname, 'data.json');
const SESSION_TIMEOUT_MS = 1000 * 60 * 60 * 24; // 24 hours

// Default advertisement HTML. This will be used whenever no custom advert
// has been saved yet. Admins can change the advert via the new
// /admin/advert page. The content is stored alongside the other app state
// in the database (if configured) or in the JSON file.
const DEFAULT_ADVERT_HTML =
  '<div style="background-color:#e8e8e8;width:100%;height:100%;display:flex;flex-direction:column;justify-content:center;align-items:center;">' +
  '<h2>Werbefläche</h2>' +
  '<p>Hier könnte Ihre Werbung stehen.</p>' +
  '</div>';

// In‑memory session store.  When the server restarts sessions will be
// invalidated; persistent sessions could be stored in the data file if
// necessary.
const sessions = {};

/**
 * Remove expired appointments and related bookings.
 *
 * An appointment is considered expired when its end time (or start time
 * if no end is provided) plus 24 hours lies in the past relative to
 * the current system time.  All expired appointments are removed from
 * the data store along with any associated bookings.  Bookings with
 * a non‑active status (e.g. cancelled) are also purged.  Returns
 * true if any records were removed, otherwise false.
 *
 * @param {Object} data The application state object
 * @returns {boolean} Whether any data was removed
 */
function cleanupExpired(data) {
  let changed = false;
  const now = Date.now();
  // Remove expired appointments
  if (Array.isArray(data.appointments)) {
    const remainingAppointments = [];
    data.appointments.forEach(app => {
      // Determine the end timestamp of the appointment.  For all‑day
      // appointments we treat the end as the start plus one day.
      let endTs = null;
      try {
        if (app.allDay) {
          const s = new Date(app.start || app.datetime);
          endTs = s.getTime() + 24 * 60 * 60 * 1000;
        } else if (app.end) {
          endTs = new Date(app.end).getTime();
        } else if (app.start || app.datetime) {
          endTs = new Date(app.start || app.datetime).getTime();
        }
      } catch (e) {
        endTs = null;
      }
      // Add 24h grace period after end
      const expiry = endTs ? (endTs + 24 * 60 * 60 * 1000) : null;
      const isExpired = expiry && expiry < now;
      if (isExpired) {
        changed = true;
        // Remove any bookings tied to this appointment
        if (Array.isArray(data.bookings)) {
          data.bookings = data.bookings.filter(b => b.appointmentId !== app.id);
        }
      } else {
        remainingAppointments.push(app);
      }
    });
    data.appointments = remainingAppointments;
  }
  // Purge any bookings that are not active (e.g. cancelled)
  if (Array.isArray(data.bookings)) {
    const remainingBookings = [];
    data.bookings.forEach(b => {
      if (b.status && b.status !== 'active') {
        changed = true;
      } else {
        remainingBookings.push(b);
      }
    });
    data.bookings = remainingBookings;
  }
  return changed;
}

// SSE event clients keyed by userId.  Each entry is an array of
// { id, res } objects where id is a unique identifier for the connection
// and res is the ServerResponse object.  When data changes, events are
// broadcast to the appropriate users.
const sseClients = {};

/**
 * Read the persistent data file.  If it doesn't exist yet, create a
 * reasonable starting structure.  All file reads/writes are synchronous
 * because they happen infrequently relative to user interactions.
 */
function readData() {
  // If a DB client is available, return the cached state. The cache is kept
  // in sync with the database by writeData().
  if (dbClient) {
    return dbData || {
      users: [],
      nextUserId: 1,
      nextSellerNumber: 1,
      appointments: [],
      nextAppointmentId: 1,
      bookings: [],
      nextBookingId: 1
    };
  }
  // Otherwise fall back to reading from the JSON file.
  return readDataFromFile();
}

/**
 * Persist data back to disk.  Writes synchronously to ensure that the
 * server state on disk matches the in‑memory state after each mutating
 * operation.
 */
function writeData(data) {
  if (dbClient) {
    // Update in-memory cache
    dbData = data;
    // Persist asynchronously to the database. Errors are logged but do not
    // block the main thread.
    dbClient
      .query('UPDATE app_state SET data = $1::jsonb WHERE id = 1', [JSON.stringify(data)])
      .catch(err => {
        console.error('Failed to persist data to database:', err);
      });
  } else {
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  }
}

/**
 * Helper to generate a random identifier.  This is used for session IDs,
 * password reset tokens and SSE connection IDs.  A cryptographically
 * secure random number generator is used for maximum unpredictability.
 */
function generateId(length = 24) {
  return crypto.randomBytes(length).toString('hex');
}

/**
 * Hash a plain text password using SHA‑256.  While bcrypt or scrypt
 * provide stronger protection, SHA‑256 is sufficient for demonstration
 * purposes and requires no external dependencies.
 */
function hashPassword(password) {
  return crypto.createHash('sha256').update(password).digest('hex');
}

/**
 * Compare a plain password with a hashed password.  Timing safe
 * comparison prevents certain types of side channel attacks.
 */
function comparePassword(plain, hashed) {
  const hashedPlain = hashPassword(plain);
  return crypto.timingSafeEqual(Buffer.from(hashedPlain), Buffer.from(hashed));
}

/**
 * Get or create a session for an incoming request.  Sessions are stored
 * in memory only; each session has an expiry timestamp attached.  A
 * cookie called `sessionId` is used to reference the session.
 */
function getSession(req, res) {
  const cookies = parseCookies(req);
  let sid = cookies.sessionId;
  let session;
  if (sid && sessions[sid]) {
    session = sessions[sid];
    // Expire old sessions
    if (Date.now() > session.expires) {
      delete sessions[sid];
      sid = null;
      session = null;
    }
  }
  if (!session) {
    sid = generateId(16);
    session = { id: sid, userId: null, expires: Date.now() + SESSION_TIMEOUT_MS };
    sessions[sid] = session;
    setCookie(res, 'sessionId', sid, { httpOnly: true, path: '/' });
  }
  return session;
}

/**
 * Parse cookies from the request header into an object.
 */
function parseCookies(req) {
  const header = req.headers.cookie;
  const cookies = {};
  if (!header) return cookies;
  const parts = header.split(';');
  parts.forEach(part => {
    const [name, ...rest] = part.trim().split('=');
    cookies[name] = decodeURIComponent(rest.join('='));
  });
  return cookies;
}

/**
 * Set a cookie on the response.  Options include path, maxAge and httpOnly.
 */
function setCookie(res, name, value, options = {}) {
  let cookie = `${name}=${encodeURIComponent(value)}`;
  if (options.maxAge) {
    cookie += `; Max-Age=${options.maxAge}`;
  }
  if (options.path) {
    cookie += `; Path=${options.path}`;
  }
  if (options.httpOnly) {
    cookie += `; HttpOnly`;
  }
  res.setHeader('Set-Cookie', cookie);
}

/**
 * Serve a static file from the public directory.  If the file is not
 * found then return false.  Otherwise write the contents and return true.
 */
function serveStatic(req, res, pathname) {
  const filePath = path.join(__dirname, 'public', pathname);
  if (!filePath.startsWith(path.join(__dirname, 'public'))) {
    return false; // protect against directory traversal
  }
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes = {
      '.html': 'text/html; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.svg': 'image/svg+xml',
      '.ico': 'image/x-icon'
    };
    const mime = mimeTypes[ext] || 'application/octet-stream';
    const content = fs.readFileSync(filePath);
    res.writeHead(200, { 'Content-Type': mime });
    res.end(content);
    return true;
  }
  return false;
}

/**
 * Render an HTML template with a simple interpolation.  Templates live
 * in the views directory and have access to a `data` object for
 * substitution.  The format is {{key}} where key can reference nested
 * properties (e.g. {{user.name}}).  For more complex templating a
 * library like EJS would be appropriate, but implementing a basic
 * interpolation avoids external dependencies here.
 */
function renderTemplate(name, data = {}) {
  const file = path.join(__dirname, 'views', `${name}.html`);
  let template = fs.readFileSync(file, 'utf8');
  // First handle triple braces {{{var}}} for unescaped insertion
  template = template.replace(/\{\{\{\s*([\w.]+)\s*\}\}\}/g, (match, key) => {
    const parts = key.split('.');
    let value = data;
    for (const part of parts) {
      if (value && Object.prototype.hasOwnProperty.call(value, part)) {
        value = value[part];
      } else {
        value = '';
        break;
      }
    }
    return String(value);
  });
  // Then handle double braces with escaping
  template = template.replace(/\{\{\s*([\w.]+)\s*\}\}/g, (match, key) => {
    const parts = key.split('.');
    let value = data;
    for (const part of parts) {
      if (value && Object.prototype.hasOwnProperty.call(value, part)) {
        value = value[part];
      } else {
        value = '';
        break;
      }
    }
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  });
  return template;
}

/**
 * Broadcast an event to SSE clients.  The `targets` argument can be a
 * single userId or an array of userIds.  Clients listening on /events
 * will receive the provided message.  This is used to update dashboards
 * when bookings are created, updated or cancelled.
 */
/* New helpers: approvals and products */
function findSellerProducts(data, sellerId){ return data.products ? data.products.filter(p => p.sellerId === sellerId) : []; }
/**
 * Create a new product for a seller.  Accepts an optional stock value.
 * Stock should be a number (or null) representing available units.  If no stock
 * is provided, the property is omitted to maintain backwards compatibility.
 */
function createProduct(data, sellerId, name, stock = null){
  const id = generateId(12);
  data.products = data.products||[];
  const product = {
    id,
    sellerId,
    name,
    createdAt: Date.now(),
    prices: {}
  };
  // If a stock value is supplied, ensure it's a number and attach it to product.
  if (stock !== null && stock !== undefined && stock !== '') {
    const s = Number(stock);
    if (!isNaN(s) && s >= 0) {
      product.stock = s;
    }
  }
  data.products.push(product);
  return id;
}
// Helpers for buyer/seller approval.  Always convert IDs to numbers to avoid
// inconsistent string/number comparisons.  Approvals are stored in
// `data.approvals` with numeric sellerId and buyerId.  Status can be
// 'pending', 'approved' or 'rejected'.
function getApproval(data, sellerId, buyerId){
  data.approvals = data.approvals || [];
  const sid = Number(sellerId);
  const bid = Number(buyerId);
  return data.approvals.find(a => Number(a.sellerId) === sid && Number(a.buyerId) === bid) || null;
}
function setApproval(data, sellerId, buyerId, status){
  const sid = Number(sellerId);
  const bid = Number(buyerId);
  data.approvals = data.approvals || [];
  let a = getApproval(data, sid, bid);
  if(!a){
    a = { id: generateId(10), sellerId: sid, buyerId: bid, status, createdAt: Date.now() };
    data.approvals.push(a);
  } else {
    a.status = status;
  }
  return a;
}
function isApproved(data, sellerId, buyerId){
  const a = getApproval(data, sellerId, buyerId);
  return !!(a && a.status === 'approved');
}

function setBuyerTiersForProduct(product, buyerId, tiers){ product.prices = product.prices || {}; product.prices[String(buyerId)] = tiers; }
function getBuyerTiersForProduct(product, buyerId){
  if(!product || !product.prices) return null;
  const t = product.prices[String(buyerId)];
  if(!t) return null;
  if(Array.isArray(t)) return t;
  const v = Number(t); if(!isNaN(v) && v>0) return [{minAmount:0, unitPrice:v}];
  return null;
}
function resolveUnitPrice(tiers, amount){
  if(!tiers || !tiers.length) return null;
  const a = Number(amount)||0; let price = null;
  tiers.forEach(t=>{ if(a>=Number(t.minAmount)) price=Number(t.unitPrice); });
  return price;
}

function now(){ return Date.now(); }
function isSellerActive(u){ if(!u || u.type!=='seller') return false; if(u.accessUntil && now()>u.accessUntil) return false; return true; }

function broadcastEvent(targets, event, data) {
  const ids = Array.isArray(targets) ? targets : [targets];
  const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  ids.forEach(uid => {
    const clients = sseClients[uid];
    if (!clients) return;
    clients.forEach(client => {
      client.res.write(message);
    });
  });
}

/**
 * Main request handler.  Routes are defined here with simple pattern
 * matching on the request method and pathname.  Because there are no
 * external routing libraries, the handlers are defined directly in
 * this function for clarity.
 */
function handleRequest(req, res) {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const data = readData();
  // Ensure new collections exist so they persist across requests.  Messages store
  // simple communication objects, and codeRequests collects seller requests for
  // new activation keys.  Initialise them if they are missing so that other
  // code can safely push into these arrays without additional guards.
  data.messages = data.messages || [];
  data.codeRequests = data.codeRequests || [];
  // Periodically clean up expired appointments and stale bookings.  If
  // any data is removed, persist the updated state.  Doing this at
  // request time ensures that stale records disappear at most a few
  // minutes after they expire.
  const expiredRemoved = cleanupExpired(data);
  if (expiredRemoved) {
    writeData(data);
  }
  // Ensure a default advert exists. If no advert has been defined yet,
  // initialise it with the default HTML. Persist the change so it
  // survives restarts. We perform this check here because readData()
  // may return a persisted object without an advertHtml property.
  if (!data.advertHtml) {
    data.advertHtml = DEFAULT_ADVERT_HTML;
    writeData(data);
  }
  // Ensure a default admin account exists.  If no admin is defined in
  // the data store, initialize one with the default credentials.  The
  // password is stored as a SHA‑256 hash to avoid persisting plain text.
  if (!data.admin) {
    data.admin = { username: 'admin', password: hashPassword('admin1234!') };
    writeData(data);
  }
  // Ensure the codeRequests list exists for tracking seller code
  // requests.  Without this property old installs will not have a
  // codeRequests array.
  if (!Array.isArray(data.codeRequests)) {
    data.codeRequests = [];
    writeData(data);
  }
  const session = getSession(req, res);
  const user = data.users.find(u => u.id === session.userId) || null;

  // Static files
  if (pathname.startsWith('/static/')) {
    if (serveStatic(req, res, pathname.substring(8))) return;
    res.writeHead(404);
    return res.end('Not found');
  }

  // Server‑sent events endpoint
  if (pathname === '/events') {
    if (!user) {
      res.writeHead(403);
      return res.end('Forbidden');
    }
    // Keep connection open
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write('\n');
    const clientId = generateId(8);
    if (!sseClients[user.id]) sseClients[user.id] = [];
    sseClients[user.id].push({ id: clientId, res });
    req.on('close', () => {
      // Remove client on disconnect
      const idx = sseClients[user.id]?.findIndex(c => c.id === clientId);
      if (idx >= 0) {
        sseClients[user.id].splice(idx, 1);
      }
    });
    return;
  }

  // Public advert endpoint. This serves the current advertisement HTML
  // directly. It is used by the embedded iframes on the home and admin
  // pages. Respond with plain HTML so that the iframe content is
  // rendered correctly.
  if (pathname === '/advert' && req.method === 'GET') {
    const advertContent = data.advertHtml || DEFAULT_ADVERT_HTML;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(advertContent);
  }

  // Helper for sending plain text or HTML responses
  function send(status, body, headers = {}) {
    res.writeHead(status, Object.assign({ 'Content-Type': 'text/html; charset=utf-8' }, headers));
    res.end(body);
  }

  // Helper for redirecting
  function redirect(location) {
    res.writeHead(302, { Location: location });
    res.end();
  }

  // Routing table
  // Home page
  if (pathname === '/') {
    if (user) {
      // Redirect buyers and sellers to their dashboards
      if (user.type === 'seller') return redirect('/seller/dashboard');
      if (user.type === 'buyer') return redirect('/buyer/dashboard');
    }
    const html = renderTemplate('home', {});
    return send(200, html);
  }

  

/* Admin routes */
// Deprecated admin login endpoint.  Admins now log in via /login using the
// username "admin".  Redirect any requests to the unified login page.
if (pathname === '/admin/login' && (req.method === 'GET' || req.method === 'POST')) {
  return redirect('/login');
}
if (pathname === '/admin/logout') { session.isAdmin = false; return redirect('/'); }
function requireAdmin(){ return session.isAdmin === true; }

// Admin backup of user data.  Returns a JSON document containing
// all registered users.  Only accessible to admins.
if (pathname === '/admin/backup/users' && req.method === 'GET') {
  if (!requireAdmin()) return redirect('/admin/login');
  const content = JSON.stringify(data.users || [], null, 2);
  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Content-Disposition': 'attachment; filename="users_backup.json"'
  });
  return res.end(content);
}

// Admin key management page.  Displays form to create new keys and
// lists existing keys.  Accessible only to admins.
if (pathname === '/admin/keys') {
  if (!requireAdmin()) return redirect('/admin/login');
  const keys = data.keys || [];
  const html = renderTemplate('admin_keys', {
    keys: JSON.stringify(keys)
  });
  return send(200, html);
}

// Admin sellers management page.  Lists all sellers with editing links.
if (pathname === '/admin/sellers') {
  if (!requireAdmin()) return redirect('/admin/login');
  const sellers = data.users.filter(u => u.type === 'seller');
  const html = renderTemplate('admin_sellers', {
    sellers: JSON.stringify(sellers)
  });
  return send(200, html);
}

// Admin buyers management page.  Lists all buyers with editing links.
if (pathname === '/admin/buyers') {
  if (!requireAdmin()) return redirect('/admin/login');
  const buyers = data.users.filter(u => u.type === 'buyer');
  const html = renderTemplate('admin_buyers', {
    buyers: JSON.stringify(buyers)
  });
  return send(200, html);
}

// Admin user edit page (GET).  Displays a form to edit a user's details.
if (pathname.startsWith('/admin/users/edit/') && req.method === 'GET') {
  if (!requireAdmin()) return redirect('/admin/login');
  const idStr = pathname.split('/').pop();
  const uid = parseInt(idStr, 10);
  const usr = data.users.find(u => u.id === uid);
  if (!usr) {
    return send(404, 'Benutzer nicht gefunden');
  }
  const html = renderTemplate('admin_edit_user', {
    user: JSON.stringify(usr),
    returnPath: usr.type === 'seller' ? '/admin/sellers' : '/admin/buyers'
  });
  return send(200, html);
}

// Admin user edit submission (POST).  Updates the specified user.
if (pathname.startsWith('/admin/users/edit/') && req.method === 'POST') {
  if (!requireAdmin()) return redirect('/admin/login');
  const idStr = pathname.split('/').pop();
  const uid = parseInt(idStr, 10);
  const usr = data.users.find(u => u.id === uid);
  if (!usr) {
    return send(404, 'Benutzer nicht gefunden');
  }
  collectPostData(req, body => {
    const name = (body.name || '').trim();
    const phone = (body.phone || '').trim();
    const email = (body.email || '').trim();
    const password = body.password || '';
    if (name) usr.name = name;
    if (phone) usr.phone = phone;
    if (email) {
      // Ensure no other user is using this email
      const existing = data.users.find(u => u.id !== uid && u.email.toLowerCase() === email.toLowerCase());
      if (existing) {
        return send(400, 'E‑Mail bereits vergeben');
      }
      usr.email = email;
    }
    if (password) {
      usr.password = hashPassword(password);
    }
    writeData(data);
    // Redirect back to appropriate management page based on user type
    const redirectPath = usr.type === 'seller' ? '/admin/sellers' : '/admin/buyers';
    return redirect(redirectPath);
  });
  return;
}

// Default admin dashboard redirects to key management page.  Placed after
// more specific admin routes so that those are matched first.
if (pathname === '/admin') {
  return redirect('/admin/keys');
}
if (pathname === '/admin/keys/create' && req.method === 'POST') {
  if (!requireAdmin()) return redirect('/admin/login');
  collectPostData(req, body => {
    const days = parseInt(body.days || '0', 10);
    const code = (body.code || generateId(8)).toUpperCase();
    const durationMs = Math.max(days, 1) * 24 * 60 * 60 * 1000;
    // Read the maximum number of users allowed for this key.  Default to 1 if not provided or invalid.
    const maxUsesRaw = body.maxUses;
    let maxUses = parseInt(maxUsesRaw, 10);
    if (!maxUses || maxUses < 1) maxUses = 1;
    const key = {
      id: generateId(10),
      code,
      createdAt: Date.now(),
      expiresAt: Date.now() + durationMs,
      status: 'unused',
      maxUses: maxUses,
      usedBy: []
    };
    data.keys = data.keys || [];
    data.keys.push(key);
    writeData(data);
    return redirect('/admin/keys');
  }); return;
}
if (pathname === '/admin/password' && req.method === 'POST') {
  if (!requireAdmin()) return redirect('/admin/login');
  collectPostData(req, body => {
    const { oldpass, newpass } = body;
    if (data.admin.password !== hashPassword(oldpass)) return send(400,'Altes Passwort falsch');
    data.admin.password = hashPassword(newpass);
    writeData(data);
    return redirect('/admin/keys');
  }); return;
}

 // Admin advert management. Allows the admin to view and edit the current
 // advertisement HTML. Only accessible to logged-in admins. When the
 // admin submits new content, it is saved in the persistent data and the
 // user is redirected back to the dashboard.
 if (pathname === '/admin/advert' && req.method === 'GET') {
   if (!requireAdmin()) return redirect('/admin/login');
   const html = renderTemplate('admin_advert', { advert: data.advertHtml || DEFAULT_ADVERT_HTML });
   return send(200, html);
 }
 if (pathname === '/admin/advert' && req.method === 'POST') {
   if (!requireAdmin()) return redirect('/admin/login');
   collectPostData(req, body => {
     const content = body.content || '';
     // Persist the new advertisement. If empty, revert to default.
     data.advertHtml = content.trim() ? content : DEFAULT_ADVERT_HTML;
     writeData(data);
     return redirect('/admin');
   });
   return;
 }

 // Admin messages management.  Displays all messages and allows sending new ones to users.
 if (pathname === '/admin/messages') {
   if (!requireAdmin()) return redirect('/login');
   const msgs = data.messages || [];
   const html = renderTemplate('admin_messages', {
     messages: JSON.stringify(msgs),
     users: JSON.stringify(data.users || [])
   });
   return send(200, html);
 }
 // Admin send message.  Admin can target everyone, all buyers, all sellers or an individual.
 if (pathname === '/admin/messages/send' && req.method === 'POST') {
   if (!requireAdmin()) return redirect('/login');
   collectPostData(req, body => {
     const target = (body.to || '').trim();
     const content = (body.message || '').trim();
     if (!content) {
       return send(400, 'Nachricht darf nicht leer sein');
     }
     let recipients = [];
     if (target === 'all') {
       recipients = (data.users || []).map(u => u.id);
     } else if (target === 'buyers') {
       recipients = (data.users || []).filter(u => u.type === 'buyer').map(u => u.id);
     } else if (target === 'sellers') {
       recipients = (data.users || []).filter(u => u.type === 'seller').map(u => u.id);
     } else {
       const uid = parseInt(target, 10);
       if (!isNaN(uid)) recipients.push(uid);
     }
     recipients.forEach(rid => {
       const msg = {
         id: generateId(12),
         from: 'admin',
         to: rid,
         toRole: null,
         body: content,
         createdAt: Date.now(),
         read: false
       };
       data.messages.push(msg);
     });
     writeData(data);
     return redirect('/admin/messages');
   });
   return;
 }
// Registration page
  if (pathname === '/register' && req.method === 'GET') {
    const html = renderTemplate('register', {});
    return send(200, html);
  }
  if (pathname === '/register' && req.method === 'POST') {
    collectPostData(req, body => {
      const { type, name, phone, email, password, sellerCode } = body;
      if (!type || !name || !phone || !email || !password || !['buyer', 'seller'].includes(type)) {
        return send(400, 'Ungültige Registrierungsdaten');
      }
      if (data.users.some(u => u.email.toLowerCase() === email.toLowerCase())) {
        return send(400, 'E-Mail ist bereits registriert');
      }
      // Seller key check
let accessUntil = null;
if (type === 'seller') {
  const key = (data.keys || []).find(k => k.code === String(sellerCode || '').trim());
  if (!key) {
    return send(400, 'Ungültiger Schlüssel');
  }
  // Determine how many times the key has been used.  For backwards compatibility,
  // handle both array and single-value formats for usedBy.
  let usedCount = 0;
  if (Array.isArray(key.usedBy)) {
    usedCount = key.usedBy.length;
  } else if (key.usedBy) {
    usedCount = 1;
  }
  const allowed = key.maxUses || 1;
  // Reject if the key has reached its maximum usage count.
  if (usedCount >= allowed) {
    return send(400, 'Schlüssel hat die maximale Anzahl an Nutzungen erreicht');
  }
  // For older keys that only allowed one user, ensure they are not reused.
  if (key.status && key.status !== 'unused' && !Array.isArray(key.usedBy)) {
    return send(400, 'Schlüssel bereits verwendet');
  }
  if (key.expiresAt && Date.now() > key.expiresAt) {
    return send(400, 'Schlüssel abgelaufen');
  }
  accessUntil = key.expiresAt;
}
const newUser = {
        id: data.nextUserId++,
        type,
        name,
        phone,
        email,
        password: hashPassword(password),
        resetToken: null
      };
      if (type === 'seller') {
        newUser.sellerNumber = data.nextSellerNumber++;
      }
      data.users.push(newUser);
      if (type === 'seller') {
        const key = (data.keys || []).find(k => k.code === String(sellerCode || '').trim());
        if (key) {
          // Ensure usedBy is an array for compatibility with the new multi-use logic.
          if (!Array.isArray(key.usedBy)) {
            key.usedBy = key.usedBy ? [key.usedBy] : [];
          }
          key.usedBy.push(newUser.id);
          key.usedAt = Date.now();
          const allowed = key.maxUses || 1;
          if (key.usedBy.length >= allowed) {
            key.status = 'used';
          } else {
            // When not fully used, show that some uses remain
            key.status = 'partial';
          }
        }
      }
      writeData(data);
      return redirect('/login');
    });
    return;
  }

  // Login
  // Unified login for buyers, sellers and the admin.  Admins log in via the
  // same form using the username "admin" in the E‑Mail field.  An optional
  // error parameter on the query string triggers an error message.
  if (pathname === '/login' && req.method === 'GET') {
    let errorMsg = '';
    if (parsed.query.error) {
      errorMsg = '<p class="error">E‑Mail oder Passwort falsch.</p>';
    }
    const html = renderTemplate('login', { errorMessage: errorMsg });
    return send(200, html);
  }
  if (pathname === '/login' && req.method === 'POST') {
    collectPostData(req, body => {
      const { email, password } = body;
      const loginName = (email || '').trim().toLowerCase();
      // Admin credentials: use the email/username field to log in as admin
      if (data.admin && loginName === (data.admin.username || 'admin').toLowerCase() && comparePassword(password || '', data.admin.password)) {
        session.isAdmin = true;
        // Admin sessions do not set userId, leaving userId undefined
        return redirect('/admin/keys');
      }
      // Regular user authentication
      const found = data.users.find(u => u.email.toLowerCase() === loginName);
      if (!found || !comparePassword(password || '', found.password)) {
        return redirect('/login?error=1');
      }
      session.userId = found.id;
      session.expires = Date.now() + SESSION_TIMEOUT_MS;
      return redirect('/');
    });
    return;
  }

  // Logout
  if (pathname === '/logout') {
    session.userId = null;
    return redirect('/');
  }

  // Password reset request
  if (pathname === '/password-reset' && req.method === 'GET') {
    const html = renderTemplate('password_reset_request', {});
    return send(200, html);
  }
  if (pathname === '/password-reset' && req.method === 'POST') {
    collectPostData(req, body => {
      const { email } = body;
      const found = data.users.find(u => u.email.toLowerCase() === (email || '').toLowerCase());
      if (!found) {
        // Do not reveal whether an email exists for security
        const message = '';
        return send(200, renderTemplate('password_reset_sent', { message }));
      }
      const token = generateId(16);
      found.resetToken = token;
      writeData(data);
      const link = `/reset?token=${token}`;
      const message = `\n<p>Zum Testen klicken Sie auf folgenden Link, um Ihr Passwort zurückzusetzen: <a href="${link}">${link}</a></p>`;
      return send(200, renderTemplate('password_reset_sent', { message }));
    });
    return;
  }

  // Password reset form
  if (pathname === '/reset' && req.method === 'GET') {
    const token = parsed.query.token;
    const found = data.users.find(u => u.resetToken === token);
    if (!found) {
      return send(400, 'Ungültiger oder abgelaufener Token');
    }
    const html = renderTemplate('password_reset_form', { token });
    return send(200, html);
  }
  // Password reset submission
  if (pathname === '/reset' && req.method === 'POST') {
    collectPostData(req, body => {
      const { token, password } = body;
      const found = data.users.find(u => u.resetToken === token);
      if (!found) {
        return send(400, 'Ungültiger oder abgelaufener Token');
      }
      found.password = hashPassword(password);
      found.resetToken = null;
      writeData(data);
      return redirect('/login');
    });
    return;
  }

  
// Products management
if (user && user.type === 'seller' && pathname === '/seller/products' && req.method === 'GET') {
  const products = findSellerProducts(data, user.id);
  const html = renderTemplate('seller_products', { products: JSON.stringify(products) });
  return send(200, html);
}
if (user && user.type === 'seller' && pathname === '/seller/products' && req.method === 'POST') {
  collectPostData(req, body => {
    const name = (body.name||'').trim();
    // Parse stock from form; if provided, ensure it is a non-negative number
    const stockRaw = body.stock;
    let stockVal = null;
    if (stockRaw !== undefined && stockRaw !== null && stockRaw !== '') {
      const s = parseFloat(stockRaw);
      if (isNaN(s) || s < 0) {
        return send(400, 'Ungültiger Bestand');
      }
      stockVal = s;
    }
    if(!name) {
      return send(400,'Produktname erforderlich');
    }
    createProduct(data, user.id, name, stockVal);
    writeData(data);
    return redirect('/seller/products');
  }); return;
}
if (user && user.type === 'seller' && pathname.startsWith('/seller/products/delete/') && req.method === 'POST') {
  const id = pathname.split('/').pop();
  const idx = (data.products||[]).findIndex(p=>p.id===id && p.sellerId===user.id);
  if(idx>=0){ data.products.splice(idx,1); writeData(data); }
  return redirect('/seller/products');
}

  // Seller key management page: display current expiry and allow entering a new key
  if (user && user.type === 'seller' && pathname === '/seller/key') {
    if (req.method === 'GET') {
      const exp = user.accessUntil || null;
      const html = renderTemplate('seller_key', { expiry: exp });
      return send(200, html);
    }
    if (req.method === 'POST') {
      collectPostData(req, body => {
        const codeInput = (body.code || '').trim().toUpperCase();
        if (!codeInput) {
          return send(400, 'Schlüssel erforderlich');
        }
        const key = (data.keys || []).find(k => k.code === codeInput);
        if (!key) {
          return send(400, 'Ungültiger Schlüssel');
        }
        // Check key usage limits
        let usedCount = 0;
        if (Array.isArray(key.usedBy)) usedCount = key.usedBy.length;
        else if (key.usedBy) usedCount = 1;
        const allowed = key.maxUses || 1;
        if (usedCount >= allowed) {
          return send(400, 'Schlüssel hat die maximale Anzahl an Nutzungen erreicht');
        }
        if (key.expiresAt && Date.now() > key.expiresAt) {
          return send(400, 'Schlüssel abgelaufen');
        }
        // Ensure usedBy is an array
        if (!Array.isArray(key.usedBy)) {
          key.usedBy = key.usedBy ? [key.usedBy] : [];
        }
        if (!key.usedBy.includes(user.id)) {
          key.usedBy.push(user.id);
        }
        // Update status
        if (key.usedBy.length >= allowed) key.status = 'used';
        else key.status = 'partial';
        // Update seller's accessUntil
        user.accessUntil = key.expiresAt;
        writeData(data);
        return redirect('/seller/dashboard');
      });
      return;
    }
  }

  // Seller messages page: list messages and send new ones
  if (user && user.type === 'seller' && pathname === '/seller/messages') {
    if (req.method === 'GET') {
      // Filter messages addressed to this seller or sent by them, or broadcast to all sellers
      const msgs = (data.messages || []).filter(m => {
        const toMatch = (String(m.to) === String(user.id)) || (m.to === 'all' && (!m.toRole || m.toRole === 'seller'));
        const fromMatch = String(m.from) === String(user.id);
        return toMatch || fromMatch;
      });
      // Determine approved buyers for this seller
      const approved = (data.approvals || []).filter(a => a.sellerId === user.id && a.status === 'approved');
      const buyersList = data.users.filter(u => u.type === 'buyer' && approved.some(a => a.buyerId === u.id));
      const html = renderTemplate('seller_messages', {
        messages: JSON.stringify(msgs),
        buyers: JSON.stringify(buyersList)
      });
      return send(200, html);
    }
    if (req.method === 'POST') {
      collectPostData(req, body => {
        const toId = body.to || '';
        const content = (body.message || '').trim();
        if (!content) {
          return send(400, 'Nachricht darf nicht leer sein');
        }
        // Determine recipients: all approved buyers or a specific buyer
        const recipients = [];
        if (toId === 'all') {
          const approved = (data.approvals || []).filter(a => a.sellerId === user.id && a.status === 'approved');
          approved.forEach(a => recipients.push(a.buyerId));
        } else {
          const rid = parseInt(toId, 10);
          if (!isNaN(rid)) recipients.push(rid);
        }
        recipients.forEach(rid => {
          const msg = {
            id: generateId(12),
            from: user.id,
            to: rid,
            toRole: 'buyer',
            body: content,
            createdAt: Date.now(),
            read: false
          };
          data.messages.push(msg);
        });
        writeData(data);
        return redirect('/seller/messages');
      });
      return;
    }
  }

    // Update product stock.  Accepts POST to /seller/products/stock/:id with a
    // 'stock' field.  Only the owning seller may update the stock.  After
    // updating, broadcasts a 'stock' SSE event to the seller and approved
    // buyers.
    if (user && user.type === 'seller' && pathname.startsWith('/seller/products/stock/') && req.method === 'POST') {
      // Extract product ID from path
      const parts = pathname.split('/');
      const prodId = parts[parts.length - 1];
      const product = (data.products || []).find(p => p.id === prodId && p.sellerId === user.id);
      if (!product) {
        return send(404, 'Produkt nicht gefunden');
      }
      collectPostData(req, body => {
        const stockRaw = body.stock;
        if (stockRaw === undefined || stockRaw === null || stockRaw === '') {
          return send(400, 'Bestand erforderlich');
        }
        const s = parseFloat(stockRaw);
        if (isNaN(s) || s < 0) {
          return send(400, 'Ungültiger Bestand');
        }
        product.stock = s;
        writeData(data);
        // Broadcast updated stock to seller and approved buyers for this seller
        // Send to seller
        broadcastEvent(user.id, 'stock', { productId: product.id, stock: product.stock });
        // Also send to each approved buyer
        const approved = (data.approvals || []).filter(a => a.sellerId === user.id && a.status === 'approved');
        const buyerIds = approved.map(a => a.buyerId);
        if (buyerIds.length > 0) {
          broadcastEvent(buyerIds, 'stock', { productId: product.id, stock: product.stock });
        }
        return redirect('/seller/products');
      });
      return;
    }

/* Seller routes */
  // Buyers management
  if (user && user.type === 'seller' && pathname === '/seller/buyers' && req.method === 'GET') {
    const pending = (data.approvals||[]).filter(a=>a.sellerId===user.id && a.status==='pending');
    const approved = (data.approvals||[]).filter(a=>a.sellerId===user.id && a.status==='approved');
    const buyers = data.users.filter(u=>u.type==='buyer');
    const products = findSellerProducts(data, user.id);
    const model = { pending: JSON.stringify(pending), approved: JSON.stringify(approved), buyers: JSON.stringify(buyers), products: JSON.stringify(products) };
    const html = renderTemplate('seller_buyers', model);
    return send(200, html);
  }
  if (user && user.type === 'seller' && pathname.startsWith('/seller/buyers/approve/') && req.method === 'POST') {
    const buyerId = pathname.split('/').pop(); setApproval(data, user.id, buyerId, 'approved'); writeData(data); return redirect('/seller/buyers');
  }
  if (user && user.type === 'seller' && pathname.startsWith('/seller/buyers/reject/') && req.method === 'POST') {
    const buyerId = pathname.split('/').pop(); setApproval(data, user.id, buyerId, 'rejected'); writeData(data); return redirect('/seller/buyers');
  }
  if (user && user.type === 'seller' && pathname === '/seller/buyers/tiers' && req.method === 'POST') {
    collectPostData(req, body => {
      const buyerId = body.buyerId, productId = body.productId, raw = (body.tiers||'').trim();
      const product = (data.products||[]).find(p=>p.id===productId && p.sellerId===user.id);
      if(!product) return send(400, 'Produkt ungültig');
      const tiers = []; raw.split(/\r?\n/).forEach(line=>{ const parts=line.split(/[,;:\s]+/).filter(Boolean); if(parts.length>=2){const min=parseFloat(parts[0]), pr=parseFloat(parts[1]); if(!isNaN(min)&&!isNaN(pr)&&pr>0) tiers.push({minAmount:min, unitPrice:pr});}});
      if(!tiers.length) return send(400,'Keine gültige Staffel');
      tiers.sort((a,b)=>a.minAmount-b.minAmount); setBuyerTiersForProduct(product, buyerId, tiers); writeData(data); return redirect('/seller/buyers');
    }); return;
  }

  // Show details for a specific buyer, including current price tiers for each product.
  // Only accessible to sellers for buyers that have already been approved.  Renders a
  // page that displays the buyer's contact info and provides forms to adjust
  // per-product price tiers.  Route: /seller/buyer/:id
  if (user && user.type === 'seller' && pathname.startsWith('/seller/buyer/') && req.method === 'GET') {
    const buyerIdStr = pathname.split('/').pop();
    const buyerIdNum = parseInt(buyerIdStr, 10);
    const buyer = data.users.find(u => u.id === buyerIdNum && u.type === 'buyer');
    if (!buyer) {
      return send(404, 'Käufer nicht gefunden');
    }
    // Check approval
    const approvedEntry = (data.approvals || []).find(a => Number(a.sellerId) === user.id && Number(a.buyerId) === buyer.id && a.status === 'approved');
    if (!approvedEntry) {
      return send(403, 'Freischaltung erforderlich');
    }
    // Gather seller's products and tiers for this buyer
    const products = findSellerProducts(data, user.id);
    const tiersByProduct = {};
    products.forEach(p => {
      const tiers = getBuyerTiersForProduct(p, buyer.id);
      tiersByProduct[p.id] = tiers || [];
    });
    const html = renderTemplate('seller_buyer_details', {
      buyer: JSON.stringify(buyer),
      products: JSON.stringify(products),
      tiers: JSON.stringify(tiersByProduct)
    });
    return send(200, html);
  }

  if (user && user.type === 'seller' && isSellerActive(user)) {
    // Seller dashboard
    if (pathname === '/seller/dashboard') {
      // Aggregate appointments and bookings for this seller
      const appointments = data.appointments.filter(a => a.sellerId === user.id);
      const bookingsRaw = data.bookings.filter(b => appointments.some(a => a.id === b.appointmentId) && b.status === 'active');
      const bookings = bookingsRaw.map(b => { const bu = data.users.find(u => u.id === b.buyerId); return Object.assign({}, b, { buyerName: bu ? bu.name : 'Unbekannt' }); });
      const total = bookings.reduce((sum, b) => sum + b.amount, 0);
      // Provide both the full user object and separate name/number fields.
      // Without the `user` property the seller dashboard template tries to
      // parse an empty string and fails.  Passing a JSON stringified user
      // ensures JSON.parse works in the template.
      const html = renderTemplate('seller_dashboard', {
        user: JSON.stringify(user),
        userName: user.name,
        sellerNumber: user.sellerNumber,
        appointments: JSON.stringify(appointments),
        bookings: JSON.stringify(bookings),
        total: total
      });
      return send(200, html);
    }
    // Page to create new appointment
    if (pathname === '/seller/appointments/new' && req.method === 'GET') {
      const html = renderTemplate('seller_new_appointment', {});
      return send(200, html);
    }
    if (pathname === '/seller/appointments/new' && req.method === 'POST') {
      collectPostData(req, body => {
        // Accept start, optional end and allDay flags for new appointments.  A
        // start (datetime-local) and location are required.  If allDay is
        // checked, the end field will be ignored.
        const start = body.start || body.datetime;
        const end = body.end;
        const allDay = body.allDay ? true : false;
        const location = body.location;
        const days = body.days; // may be undefined, string or array
        if (!start || !location) {
          return send(400, 'Datum/Zeit und Ort erforderlich');
        }
        // Validate that end, if provided, is after start
        if (!allDay && end) {
          const sDate = new Date(start);
          const eDate = new Date(end);
          if (isNaN(sDate.getTime()) || isNaN(eDate.getTime()) || eDate <= sDate) {
            return send(400, 'Endzeit muss nach Startzeit liegen');
          }
        }
        // Helper to create and store an appointment
        function addAppointment(s, e, ad) {
          const app = {
            id: data.nextAppointmentId++,
            sellerId: user.id,
            datetime: s,
            start: s,
            end: ad ? null : (e || null),
            allDay: ad,
            location,
            booked: false,
            bookingId: null
          };
          data.appointments.push(app);
          broadcastEvent(user.id, 'appointment', { action: 'created', appointment: app });
        }
        const startDate = new Date(start);
        const endDateObj = end ? new Date(end) : null;
        // Determine selected weekdays from the form.  Accept both string and array
        let dayList = [];
        if (days) {
          if (Array.isArray(days)) dayList = days;
          else dayList = [days];
        }
        if (dayList.length === 0) {
          // Single appointment on the specified start date/time
          addAppointment(start, end, allDay);
        } else {
          // Map abbreviations to JavaScript day numbers (0 = Sunday, 1 = Monday, ...)
          const dayMap = { mo: 1, di: 2, mi: 3, do: 4, fr: 5, sa: 6, so: 0 };
          const baseDay = startDate.getDay();
          dayList.forEach(d => {
            const key = String(d).toLowerCase();
            const targetDay = dayMap[key];
            if (targetDay === undefined) return;
            let diff = targetDay - baseDay;
            if (diff < 0) diff += 7;
            const newStartDate = new Date(startDate.getTime());
            newStartDate.setDate(startDate.getDate() + diff);
            // Format as local datetime-local string (YYYY-MM-DDTHH:MM)
            const pad = n => String(n).padStart(2, '0');
            const toInput = (dt) => {
              const y = dt.getFullYear();
              const m = pad(dt.getMonth() + 1);
              const dstr = pad(dt.getDate());
              const hr = pad(dt.getHours());
              const mn = pad(dt.getMinutes());
              return `${y}-${m}-${dstr}T${hr}:${mn}`;
            };
            const newStartStr = toInput(newStartDate);
            let newEndStr = null;
            if (!allDay && endDateObj) {
              const newEndDate = new Date(endDateObj.getTime());
              newEndDate.setDate(endDateObj.getDate() + diff);
              newEndStr = toInput(newEndDate);
            }
            addAppointment(newStartStr, newEndStr, allDay);
          });
        }
        writeData(data);
        return redirect('/seller/dashboard');
      });
      return;
    }
    // Cancel booking by seller
    if (pathname.startsWith('/seller/bookings/cancel/') && req.method === 'POST') {
      const bookingId = parseInt(pathname.split('/').pop(), 10);
      const booking = data.bookings.find(b => b.id === bookingId);
      if (!booking) {
        return send(404, 'Buchung nicht gefunden');
      }
      // Check that booking belongs to this seller
      const appointment = data.appointments.find(a => a.id === booking.appointmentId);
      if (!appointment || appointment.sellerId !== user.id) {
        return send(403, 'Keine Berechtigung');
      }
      // Cancel booking
      booking.status = 'cancelled';
      appointment.booked = false;
      appointment.bookingId = null;
      writeData(data);
      // Notify buyer and seller
      broadcastEvent([booking.buyerId, user.id], 'booking', { action: 'cancelled', bookingId: booking.id });
      return redirect('/seller/dashboard');
    }

  // Seller requests a freischaltcode.  When a seller visits this page,
  // they can send a request to the admin to obtain a new key.  The
  // request will be stored in data.codeRequests and can later be
  // processed by an admin.  Only logged‑in sellers may access this.
  if (pathname === '/seller/request-code') {
    if (!user || user.type !== 'seller') {
      return redirect('/login');
    }
    if (req.method === 'GET') {
      const html = renderTemplate('seller_request_code', {});
      return send(200, html);
    }
    if (req.method === 'POST') {
      // Ensure codeRequests array exists
      data.codeRequests = data.codeRequests || [];
      // Avoid duplicate pending requests from the same seller
      const existing = data.codeRequests.find(r => r.sellerId === user.id && r.status === 'pending');
      if (!existing) {
        data.codeRequests.push({ id: generateId(10), sellerId: user.id, createdAt: Date.now(), status: 'pending' });
        writeData(data);
      }
      return redirect('/seller/dashboard');
    }
  }

  // Seller edits an existing appointment.  Show the edit form.
  if (pathname.startsWith('/seller/appointments/edit/') && req.method === 'GET') {
    if (!user || user.type !== 'seller') {
      return redirect('/login');
    }
    const idStr = pathname.split('/').pop();
    const aid = parseInt(idStr, 10);
    const appointment = data.appointments.find(a => a.id === aid);
    if (!appointment || appointment.sellerId !== user.id) {
      return send(404, 'Termin nicht gefunden');
    }
    // Render edit form with appointment data
    const html = renderTemplate('seller_edit_appointment', {
      appointment: JSON.stringify(appointment)
    });
    return send(200, html);
  }
  // Seller updates an appointment
  if (pathname.startsWith('/seller/appointments/edit/') && req.method === 'POST') {
    if (!user || user.type !== 'seller') {
      return redirect('/login');
    }
    const idStr = pathname.split('/').pop();
    const aid = parseInt(idStr, 10);
    const appointment = data.appointments.find(a => a.id === aid);
    if (!appointment || appointment.sellerId !== user.id) {
      return send(404, 'Termin nicht gefunden');
    }
    // Prevent editing if appointment is booked
    if (appointment.booked) {
      return send(400, 'Gebuchte Termine können nicht geändert werden');
    }
    collectPostData(req, body => {
      const start = body.start || body.datetime;
      const end = body.end;
      const allDay = body.allDay ? true : false;
      const location = body.location;
      if (!start || !location) {
        return send(400, 'Datum/Zeit und Ort erforderlich');
      }
      if (!allDay && end) {
        const startDate = new Date(start);
        const endDate = new Date(end);
        if (isNaN(startDate.getTime()) || isNaN(endDate.getTime()) || endDate <= startDate) {
          return send(400, 'Endzeit muss nach Startzeit liegen');
        }
      }
      appointment.start = start;
      appointment.datetime = start;
      appointment.end = allDay ? null : (end || null);
      appointment.allDay = allDay;
      appointment.location = location;
      writeData(data);
      // Notify seller dashboard of updated appointment.  Use SSE event
      broadcastEvent(user.id, 'appointment', { action: 'updated', appointment });
      return redirect('/seller/dashboard');
    });
    return;
  }
  // Seller deletes an appointment that has not been booked
  if (pathname.startsWith('/seller/appointments/delete/') && req.method === 'POST') {
    if (!user || user.type !== 'seller') {
      return redirect('/login');
    }
    const idStr = pathname.split('/').pop();
    const aid = parseInt(idStr, 10);
    const idx = data.appointments.findIndex(a => a.id === aid && a.sellerId === user.id);
    if (idx < 0) {
      return send(404, 'Termin nicht gefunden');
    }
    const appointment = data.appointments[idx];
    if (appointment.booked) {
      return send(400, 'Gebuchte Termine können nicht gelöscht werden');
    }
    data.appointments.splice(idx, 1);
    writeData(data);
    broadcastEvent(user.id, 'appointment', { action: 'deleted', appointmentId: aid });
    return redirect('/seller/dashboard');
  }
  }

  /* Buyer routes */
  if (user && user.type === 'buyer') {
    // Buyer dashboard
    if (pathname === '/buyer/dashboard') {
      const html = renderTemplate('buyer_dashboard', {});
      return send(200, html);
    }
    // Form to lookup a seller's appointments
    if (pathname === '/buyer/lookup' && req.method === 'GET') {
      const html = renderTemplate('buyer_lookup', {});
      return send(200, html);
    }
    if (pathname === '/buyer/lookup' && req.method === 'POST') {
      collectPostData(req, body => {
        const { sellerNumber } = body;
        const seller = data.users.find(u => u.type === 'seller' && String(u.sellerNumber) === String(sellerNumber));
        if (!seller) {
          return send(404, 'Verkäufer nicht gefunden');
        }
        return redirect(`/buyer/seller/${seller.sellerNumber}`);
      });
      return;
    }

    // Buyer requests approval from a seller.  When invoked, the buyer's
    // approval status for the seller is set to 'pending'.  After this,
    // the seller can approve or reject in their dashboard.  The buyer
    // will be redirected back to the seller page.
    if (pathname.startsWith('/buyer/request-approval/') && req.method === 'POST') {
      const sellerNumber = pathname.split('/').pop();
      const seller = data.users.find(u => u.type === 'seller' && String(u.sellerNumber) === sellerNumber);
      if (!seller) {
        return send(404, 'Verkäufer nicht gefunden');
      }
      // Mark approval as pending
      setApproval(data, seller.id, user.id, 'pending');
      writeData(data);
      return redirect(`/buyer/seller/${seller.sellerNumber}`);
    }
    // View seller's available appointments
    if (pathname.startsWith('/buyer/seller/') && req.method === 'GET') {
      const sellerNumber = pathname.split('/').pop();
      const seller = data.users.find(u => u.type === 'seller' && String(u.sellerNumber) === sellerNumber);
      if (!seller) {
        return send(404, 'Verkäufer nicht gefunden');
      }
      // Gather appointments for this seller
      const appointments = data.appointments.filter(a => a.sellerId === seller.id);
      // Find seller's products and buyer tiers
      const products = findSellerProducts(data, seller.id);
      const buyerId = user ? user.id : null;
      const tiersByProduct = Object.fromEntries(products.map(p => [p.id, getBuyerTiersForProduct(p, buyerId)]));
      // Determine approval status
      const approvedFlag = user ? (isApproved(data, seller.id, user.id)) : false;
      // Determine if approval has been requested but not yet approved
      const approvalEntry = (data.approvals || []).find(a => a.sellerId === seller.id && a.buyerId === user.id);
      const requestedFlag = approvalEntry && approvalEntry.status === 'pending';
      const model = {
        sellerName: seller.name,
        sellerNumber: seller.sellerNumber,
        seller: JSON.stringify(seller),
        appointments: JSON.stringify(appointments),
        products: JSON.stringify(products),
        tiers: JSON.stringify(tiersByProduct),
        approved: approvedFlag ? 'true' : 'false',
        requested: requestedFlag ? 'true' : 'false'
      };
      const html = renderTemplate('buyer_view_seller', model);
      return send(200, html);
    }
    // Book an appointment
    if (pathname === '/buyer/book' && req.method === 'POST') {
      collectPostData(req, body => {
        const { appointmentId, amount, productId } = body;
        // Look up appointment and ensure it exists and is available
        const appointment = data.appointments.find(a => a.id === parseInt(appointmentId, 10));
        const amt = parseInt(amount, 10);
        if (!appointment || appointment.booked) {
          return send(400, 'Termin nicht verfügbar');
        }
        if (!amt || amt < 5 || amt > 500 || amt % 5 !== 0) {
          return send(400, 'Ungültiger Betrag');
        }
        // Determine seller from appointment
        const seller = data.users.find(u => u.id === appointment.sellerId);
        if (!seller) {
          return send(404, 'Verkäufer nicht gefunden');
        }
        // Check buyer approval for this seller
        if (!isApproved(data, seller.id, user.id)) {
          return send(403, 'Freischaltung erforderlich');
        }
        // Find the selected product and resolve tiers
        const product = (data.products || []).find(p => p.id === productId && p.sellerId === seller.id);
        if (!product) {
          return send(400, 'Produkt erforderlich');
        }
        const tiers = getBuyerTiersForProduct(product, user.id);
        const unitPrice = resolveUnitPrice(tiers, amt);
        if (!unitPrice) {
          return send(400, 'Preis/Staffel nicht gesetzt oder Betrag zu niedrig');
        }
        const quantity = Math.floor((amt / unitPrice) * 100) / 100;
        // Check available stock for the selected product.  If stock is defined
        // and the requested quantity exceeds it, reject the booking.
        if (product.stock !== undefined && product.stock !== null) {
          const available = Number(product.stock);
          if (quantity > available) {
            return send(400, 'Nicht genügend Bestand verfügbar');
          }
        }
        // Reduce stock by the quantity booked.  If no stock property exists,
        // skip this step (legacy products may not have stock defined).
        if (product.stock !== undefined && product.stock !== null) {
          product.stock = Number(product.stock) - quantity;
          if (product.stock < 0) product.stock = 0;
        }
        // Create booking record with productId and quantity.  Quantity
        // is calculated here so it can be displayed later without
        // recomputing tiers on the client.
        const booking = {
          id: data.nextBookingId++,
          appointmentId: appointment.id,
          buyerId: user.id,
          productId: product.id,
          amount: amt,
          quantity,
          status: 'active'
        };
        // Mark appointment as booked
        appointment.booked = true;
        appointment.bookingId = booking.id;
        data.bookings.push(booking);
        writeData(data);
        // Notify seller and buyer of booking
        broadcastEvent([appointment.sellerId, user.id], 'booking', { action: 'created', booking: Object.assign({}, booking, { buyerName: user.name }) });
        // Broadcast updated stock to the seller and buyer so their interfaces can refresh
        if (product.stock !== undefined && product.stock !== null) {
          broadcastEvent([appointment.sellerId, user.id], 'stock', { productId: product.id, stock: product.stock });
        }
        return redirect('/buyer/bookings');
      });
      return;
    }
    // List buyer's bookings
    if (pathname === '/buyer/bookings' && req.method === 'GET') {
      const bookings = data.bookings.filter(b => b.buyerId === user.id && b.status === 'active');
      const appointments = data.appointments;
      const sellers = data.users.filter(u => u.type === 'seller');
      const html = renderTemplate('buyer_bookings', {
        bookings: JSON.stringify(bookings),
        appointments: JSON.stringify(appointments),
        sellers: JSON.stringify(sellers)
      });
      return send(200, html);
    }
    // Edit a booking amount
    if (pathname.startsWith('/buyer/bookings/edit/') && req.method === 'GET') {
      const bid = parseInt(pathname.split('/').pop(), 10);
      const booking = data.bookings.find(b => b.id === bid && b.buyerId === user.id && b.status === 'active');
      if (!booking) return send(404, 'Buchung nicht gefunden');
      // Build HTML options for amounts in 5‑euro steps
      let optionsHtml = '';
      for (let amt = 5; amt <= 500; amt += 5) {
        const selected = amt === booking.amount ? 'selected' : '';
        optionsHtml += `<option value="${amt}" ${selected}>${amt} €</option>`;
      }
      const html = renderTemplate('buyer_edit_booking', {
        bookingId: booking.id,
        optionsHtml
      });
      return send(200, html);
    }
    if (pathname.startsWith('/buyer/bookings/edit/') && req.method === 'POST') {
      collectPostData(req, body => {
        const bid = parseInt(pathname.split('/').pop(), 10);
        const booking = data.bookings.find(b => b.id === bid && b.buyerId === user.id && b.status === 'active');
        if (!booking) return send(404, 'Buchung nicht gefunden');
        const amt = parseInt(body.amount, 10);
        if (!amt || amt < 5 || amt > 500 || amt % 5 !== 0) {
          return send(400, 'Ungültiger Betrag');
        }
        booking.amount = amt;
        // Recalculate quantity based on tiers for this booking's product
        // Determine seller from appointment and product
        const app = data.appointments.find(a => a.id === booking.appointmentId);
        const sellerUser = app ? data.users.find(u => u.id === app.sellerId) : null;
        const product = sellerUser ? (data.products || []).find(p => p.id === booking.productId && p.sellerId === sellerUser.id) : null;
        if (product) {
          const ts = getBuyerTiersForProduct(product, user.id);
          const unitPrice = resolveUnitPrice(ts, amt);
          if (unitPrice) {
            booking.quantity = Math.floor((amt / unitPrice) * 100) / 100;
          }
        }
        writeData(data);
        // Notify seller and buyer
        broadcastEvent([data.appointments.find(a => a.id === booking.appointmentId).sellerId, user.id], 'booking', { action: 'updated', booking });
        return redirect('/buyer/bookings');
      });
      return;
    }
    // Cancel a booking
    if (pathname.startsWith('/buyer/bookings/cancel/') && req.method === 'POST') {
      const bid = parseInt(pathname.split('/').pop(), 10);
      const booking = data.bookings.find(b => b.id === bid && b.buyerId === user.id && b.status === 'active');
      if (!booking) return send(404, 'Buchung nicht gefunden');
      booking.status = 'cancelled';
      const appointment = data.appointments.find(a => a.id === booking.appointmentId);
      if (appointment) {
        appointment.booked = false;
        appointment.bookingId = null;
      }
      writeData(data);
      broadcastEvent([appointment.sellerId, user.id], 'booking', { action: 'cancelled', bookingId: booking.id });
      return redirect('/buyer/bookings');
    }
    // End of buyer routes
  }

  // Fallback 404 for unmatched routes
  return send(404, 'Seite nicht gefunden');
}

/**
 * Collect POST data from a request.  Supports application/x-www-form-urlencoded
 * and application/json bodies.  Calls the callback with the parsed data
 * when complete.
 */
function collectPostData(req, callback) {
  let body = '';
  req.on('data', chunk => {
    body += chunk.toString();
  });
  req.on('end', () => {
    const contentType = req.headers['content-type'] || '';
    let parsed;
    if (contentType.includes('application/json')) {
      try {
        parsed = JSON.parse(body);
      } catch (e) {
        parsed = {};
      }
    } else {
      // parse x-www-form-urlencoded
      parsed = {};
      body.split('&').forEach(pair => {
        const [key, value] = pair.split('=');
        if (key) parsed[decodeURIComponent(key)] = decodeURIComponent(value || '');
      });
    }
    callback(parsed);
  });
}

// Create HTTP server
const server = http.createServer(handleRequest);

// Start listening. When the script is run directly (not imported), initialise
// the database (if configured) and then start the HTTP server. Initialising
// the database first ensures that the in-memory state is loaded from a
// persistent store before handling any requests.
if (require.main === module) {
  initDb()
    .then(() => {
      const PORT = process.env.PORT || 3000;
      server.listen(PORT, () => {
        console.log(`Server started on http://localhost:${PORT}`);
      });
    })
    .catch(err => {
      console.error('Fatal error during startup:', err);
      process.exit(1);
    });
}

module.exports = server;