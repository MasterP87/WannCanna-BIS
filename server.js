const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const crypto = require('crypto');

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

// In‑memory session store.  When the server restarts sessions will be
// invalidated; persistent sessions could be stored in the data file if
// necessary.
const sessions = {};

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

/**
 * Persist data back to disk.  Writes synchronously to ensure that the
 * server state on disk matches the in‑memory state after each mutating
 * operation.
 */
function writeData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
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
function createProduct(data, sellerId, name){ const id = generateId(12); data.products = data.products||[]; data.products.push({ id, sellerId, name, createdAt: Date.now(), prices: {} }); return id; }
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
  // Ensure a default admin account exists.  If no admin is defined in
  // the data store, initialize one with the default credentials.  The
  // password is stored as a SHA‑256 hash to avoid persisting plain text.
  if (!data.admin) {
    data.admin = { username: 'admin', password: hashPassword('admin1234!') };
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
if (pathname === '/admin/login' && req.method === 'GET') {
  const html = renderTemplate('admin_login', {}); return send(200, html);
}
if (pathname === '/admin/login' && req.method === 'POST') {
  collectPostData(req, body => {
    const { username, password } = body;
    if (data.admin && username === data.admin.username && data.admin.password === hashPassword(password)) {
      session.isAdmin = true; return redirect('/admin');
    }
    return send(401, 'Login fehlgeschlagen');
  }); return;
}
if (pathname === '/admin/logout') { session.isAdmin = false; return redirect('/'); }
function requireAdmin(){ return session.isAdmin === true; }

if (pathname === '/admin') {
  if (!requireAdmin()) return redirect('/admin/login');
  const keys = data.keys || [];
  const sellers = data.users.filter(u=>u.type==='seller');
  const html = renderTemplate('admin_dashboard', { keys: JSON.stringify(keys), sellers: JSON.stringify(sellers) });
  return send(200, html);
}
if (pathname === '/admin/keys/create' && req.method === 'POST') {
  if (!requireAdmin()) return redirect('/admin/login');
    collectPostData(req, body => {
      const days = parseInt(body.days || '0', 10);
      const code = (body.code || generateId(8)).toUpperCase();
      const durationMs = Math.max(days, 1) * 24 * 60 * 60 * 1000;
      // Lese maximale Nutzung aus dem Formular. Wenn kein Wert angegeben ist, verwende 1.
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
      return redirect('/admin');
    });
    return;
}
if (pathname === '/admin/password' && req.method === 'POST') {
  if (!requireAdmin()) return redirect('/admin/login');
  collectPostData(req, body => {
    const { oldpass, newpass } = body;
    if (data.admin.password !== hashPassword(oldpass)) return send(400,'Altes Passwort falsch');
    data.admin.password = hashPassword(newpass); writeData(data); return redirect('/admin');
  }); return;
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
  if (!key) return send(400, 'Ungültiger Schlüssel');
  // Prüfe, ob der Schlüssel noch nutzbar ist. Ist maxUses erreicht, lehne die Registrierung ab.
  const usedCount = Array.isArray(key.usedBy) ? key.usedBy.length : (key.usedBy ? 1 : 0);
  const allowed = key.maxUses || 1;
  if (usedCount >= allowed) {
    return send(400, 'Schlüssel hat die maximale Anzahl an Nutzungen erreicht');
  }
  // Wenn der Schlüssel einen Status hat, berücksichtigen, dass alte Versionen nur einen Nutzer erlauben.
  if (key.status && key.status !== 'unused' && !Array.isArray(key.usedBy)) {
    // Wenn usedBy kein Array ist, war der Schlüssel bereits genutzt.
    return send(400, 'Schlüssel bereits verwendet');
  }
  if (key.expiresAt && Date.now() > key.expiresAt) return send(400, 'Schlüssel abgelaufen');
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
          // Initialisiere usedBy als Array, falls noch nicht vorhanden oder falls altes Format verwendet wurde
          if (!Array.isArray(key.usedBy)) {
            key.usedBy = key.usedBy ? [key.usedBy] : [];
          }
          key.usedBy.push(newUser.id);
          key.usedAt = Date.now();
          // Wenn die maximale Nutzung erreicht ist, setze Status auf 'used'
          const allowed = key.maxUses || 1;
          if (key.usedBy.length >= allowed) {
            key.status = 'used';
          } else {
            // Zwischenzustand anzeigen
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
      const found = data.users.find(u => u.email.toLowerCase() === (email || '').toLowerCase());
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
    if(!name) return send(400,'Produktname erforderlich');
    createProduct(data, user.id, name); writeData(data);
    return redirect('/seller/products');
  }); return;
}
if (user && user.type === 'seller' && pathname.startsWith('/seller/products/delete/') && req.method === 'POST') {
  const id = pathname.split('/').pop();
  const idx = (data.products||[]).findIndex(p=>p.id===id && p.sellerId===user.id);
  if(idx>=0){ data.products.splice(idx,1); writeData(data); }
  return redirect('/seller/products');
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
        const { datetime, location } = body;
        if (!datetime || !location) {
          return send(400, 'Datum/Zeit und Ort erforderlich');
        }
        const appointment = {
          id: data.nextAppointmentId++,
          sellerId: user.id,
          datetime,
          location,
          booked: false,
          bookingId: null
        };
        data.appointments.push(appointment);
        writeData(data);
        // Notify seller dashboard of new appointment
        broadcastEvent(user.id, 'appointment', { action: 'created', appointment });
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
        // Notify seller and buyer
        broadcastEvent([appointment.sellerId, user.id], 'booking', { action: 'created', booking: Object.assign({}, booking, { buyerName: user.name }) });
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

// Start listening.  When the script is run directly (not imported), the
// server will start.  This makes it possible to import server.js in
// testing scenarios without automatically starting the server.
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => {
    console.log(`Server started on http://localhost:${PORT}`);
  });
}

module.exports = server;