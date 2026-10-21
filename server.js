// Couple Date - signaling server (version 2)
// - Pairs two people in a room and passes messages between them.
// - Audio / video never pass through here: they go phone-to-phone (or via the TURN relay).
// - NOTHING is saved. Rooms live in memory only, and disappear when people leave
//   (or about 10 minutes after a phone loses connection and does not come back).
// - GET /ice gives the app the relay (TURN) settings, read from environment variables.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const GRACE_MS = Number(process.env.GRACE_MS || 600000); // how long a dropped phone (for example a locked one) can come back: 10 minutes
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I

// Message types that are passed straight to the other person.
const RELAY = new Set([
  'chat', 'ack', 'read', 'profile', 'react', 'miss', 'cam-state',
  'call-request', 'call-accept', 'call-decline', 'call-end',
  'offer', 'answer', 'ice',
  'screen-offer', 'screen-answer', 'screen-ice', 'screen-stop', 'screen-busy',
]);

const rooms = new Map(); // code -> { code, pin, host: slot, guest: slot|null }
// slot = { id, ws, timer }

// ---------- relay (TURN) settings ----------
let iceCache = null;
let iceCacheAt = 0;
let cfCache = null;
let cfCacheAt = 0;
const CF_BASE = process.env.CF_API_BASE || 'https://rtc.live.cloudflare.com';

// Cloudflare gives short-lived relay passwords. Needs two settings on Render:
//   CF_TURN_KEY_ID  and  CF_TURN_API_TOKEN
// The passwords are kept in memory for 6 hours (they are valid for 24 hours).
async function cloudflareIce() {
  const id = process.env.CF_TURN_KEY_ID;
  const token = process.env.CF_TURN_API_TOKEN;
  if (!id || !token) return [];
  if (cfCache && Date.now() - cfCacheAt < 6 * 60 * 60 * 1000) return cfCache;
  try {
    const r = await fetch(`${CF_BASE}/v1/turn/keys/${encodeURIComponent(id)}/credentials/generate-ice-servers`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ ttl: 86400 }),
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    let list = j && j.iceServers;
    if (list && !Array.isArray(list)) list = [list];
    if (Array.isArray(list) && list.length) {
      cfCache = list;
      cfCacheAt = Date.now();
      return list;
    }
    throw new Error('no iceServers in reply');
  } catch (e) {
    console.log('Cloudflare relay settings failed:', e.message);
    return cfCache || []; // an old answer is better than none
  }
}

async function buildIceServers() {
  const servers = [{ urls: 'stun:stun.l.google.com:19302' }];
  servers.push(...(await cloudflareIce()));
  const urls = (process.env.TURN_URLS || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (urls.length && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    servers.push({
      urls,
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL,
    });
  }
  if (process.env.METERED_URL) {
    // Optional: a provider that hands out fresh credentials from a web address.
    try {
      if (!iceCache || Date.now() - iceCacheAt > 10 * 60 * 1000) {
        const r = await fetch(process.env.METERED_URL);
        const list = await r.json();
        if (Array.isArray(list)) { iceCache = list; iceCacheAt = Date.now(); }
      }
      if (iceCache) servers.push(...iceCache);
    } catch (e) {
      console.log('relay provider fetch failed:', e.message);
    }
  }
  return servers;
}

// ---------- web page (health check + /ice) ----------
const server = http.createServer(async (req, res) => {
  const path = (req.url || '/').split('?')[0];
  if (path === '/ice') {
    const iceServers = await buildIceServers();
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ iceServers }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Couple Date server is running');
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

// ---------- helpers ----------
function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function genCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => ALPHABET[Math.floor(Math.random() * ALPHABET.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function otherRole(role) { return role === 'host' ? 'guest' : 'host'; }

function attach(room, role, ws, deviceId) {
  const slot = room[role];
  if (slot) {
    if (slot.timer) clearTimeout(slot.timer);
    slot.timer = null;
    slot.id = deviceId;
    slot.ws = ws;
  } else {
    room[role] = { id: deviceId, ws, timer: null };
  }
  ws.room = room.code;
  ws.role = role;
  ws.deviceId = deviceId;
}

// Remove one person from the room for good.
function drop(room, role) {
  const slot = room[role];
  if (!slot) return;
  if (slot.timer) clearTimeout(slot.timer);
  const other = room[otherRole(role)];
  if (role === 'host') {
    // The host leaving closes the room.
    if (other) {
      send(other.ws, { type: 'peer-left' });
      if (other.timer) clearTimeout(other.timer);
      if (other.ws) { other.ws.room = null; other.ws.role = null; }
    }
    if (slot.ws) { slot.ws.room = null; slot.ws.role = null; }
    rooms.delete(room.code);
  } else {
    if (slot.ws) { slot.ws.room = null; slot.ws.role = null; }
    room.guest = null;
    if (other) send(other.ws, { type: 'peer-left' });
  }
}

function validDevice(id) {
  return typeof id === 'string' && /^[A-Za-z0-9_-]{8,64}$/.test(id);
}

function tooManyTries(ws) {
  ws.badTries = (ws.badTries || 0) + 1;
  if (ws.badTries >= 6) { ws.close(); return true; }
  return false;
}

// ---------- connections ----------
wss.on('connection', (ws) => {
  ws.room = null;
  ws.role = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // ----- are you still there? (the app checks after waking up) -----
    if (msg.type === 'ping') return send(ws, { type: 'pong' });

    // ----- create a new room with a random code -----
    if (msg.type === 'create') {
      if (ws.room) return;
      if (!validDevice(msg.deviceId)) return send(ws, { type: 'error', message: 'Update the app.' });
      const code = genCode();
      const room = { code, pin: null, host: null, guest: null };
      rooms.set(code, room);
      attach(room, 'host', ws, msg.deviceId);
      return send(ws, { type: 'created', code, role: 'host' });
    }

    // ----- "Our room": your own fixed code + PIN, created the first time someone opens it -----
    if (msg.type === 'ourroom') {
      if (ws.room) return;
      if (!validDevice(msg.deviceId)) return send(ws, { type: 'error', message: 'Update the app.' });
      const code = String(msg.code || '').toUpperCase().replace(/\s+/g, '');
      const pin = String(msg.pin || '');
      if (!/^[A-Z0-9]{4,24}$/.test(code)) return send(ws, { type: 'error', message: 'Room code: 4 to 24 letters or numbers.' });
      if (!/^[A-Za-z0-9]{3,12}$/.test(pin)) return send(ws, { type: 'error', message: 'PIN: 3 to 12 letters or numbers.' });
      let room = rooms.get(code);
      if (!room) {
        room = { code, pin, host: null, guest: null };
        rooms.set(code, room);
        attach(room, 'host', ws, msg.deviceId);
        return send(ws, { type: 'created', code, role: 'host' });
      }
      if (room.pin !== pin) {
        if (tooManyTries(ws)) return;
        return send(ws, { type: 'error', message: 'This code is in use, or the PIN is wrong.' });
      }
      if (room.host && room.host.id === msg.deviceId) {
        attach(room, 'host', ws, msg.deviceId);
        send(ws, { type: 'created', code, role: 'host' });
        if (room.guest) send(room.guest.ws, { type: 'peer-back' });
        return;
      }
      if (room.guest && room.guest.id === msg.deviceId) {
        attach(room, 'guest', ws, msg.deviceId);
        send(ws, { type: 'joined', code, role: 'guest', hostOnline: !!(room.host && room.host.ws) });
        return send(room.host.ws, { type: 'peer-back' });
      }
      if (room.guest) return send(ws, { type: 'error', message: 'This room already has two people.' });
      attach(room, 'guest', ws, msg.deviceId);
      send(ws, { type: 'joined', code, role: 'guest', hostOnline: !!(room.host && room.host.ws) });
      return send(room.host.ws, { type: 'peer-joined' });
    }

    // ----- join with a code -----
    if (msg.type === 'join') {
      if (ws.room) return;
      if (!validDevice(msg.deviceId)) return send(ws, { type: 'error', message: 'Update the app.' });
      const code = String(msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) {
        if (tooManyTries(ws)) return;
        return send(ws, { type: 'error', message: 'Room not found. Check the code.' });
      }
      if (room.pin) return send(ws, { type: 'error', message: 'This is a private room. Use the "Our room" button.' });
      if (room.guest && room.guest.id !== msg.deviceId) {
        return send(ws, { type: 'error', message: 'This room already has two people.' });
      }
      if (room.host && room.host.id === msg.deviceId) {
        return send(ws, { type: 'error', message: 'You made this room. Share the code with your partner.' });
      }
      attach(room, 'guest', ws, msg.deviceId);
      send(ws, { type: 'joined', code, role: 'guest', hostOnline: !!(room.host && room.host.ws) });
      return send(room.host.ws, { type: 'peer-joined' });
    }

    // ----- come back to the same room after a dropped connection -----
    if (msg.type === 'rejoin') {
      if (ws.room) return;
      const code = String(msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      let role = null;
      if (room && validDevice(msg.deviceId)) {
        if (room.host && room.host.id === msg.deviceId) role = 'host';
        else if (room.guest && room.guest.id === msg.deviceId) role = 'guest';
      }
      if (!role) return send(ws, { type: 'rejoin-failed' });
      const old = room[role].ws;
      attach(room, role, ws, msg.deviceId);
      if (old && old !== ws) { old.room = null; old.role = null; try { old.close(); } catch {} }
      const other = room[otherRole(role)];
      send(ws, { type: 'rejoined', code, role, peerOnline: !!(other && other.ws) });
      if (other) send(other.ws, { type: 'peer-back' });
      return;
    }

    // ----- leaving on purpose -----
    if (msg.type === 'leave') {
      const room = rooms.get(ws.room);
      if (room && ws.role) drop(room, ws.role);
      return;
    }

    // ----- everything else is passed to the other person -----
    if (!RELAY.has(msg.type)) return;
    const room = rooms.get(ws.room);
    if (!room) return;
    if (msg.type === 'chat' && typeof msg.text === 'string' && msg.text.length > 2000) {
      msg.text = msg.text.slice(0, 2000);
    }
    const other = room[otherRole(ws.role)];
    if (other) send(other.ws, msg);
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);
    if (!room || !ws.role) return;
    const slot = room[ws.role];
    if (!slot || slot.ws !== ws) return; // an older connection that was already replaced
    slot.ws = null;
    const role = ws.role;
    const other = room[otherRole(role)];
    if (other) send(other.ws, { type: 'peer-offline' });
    slot.timer = setTimeout(() => drop(room, role), GRACE_MS);
  });
});

// Drop dead connections (phone lost signal, etc.)
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log(`Couple Date server running on port ${PORT}`));
