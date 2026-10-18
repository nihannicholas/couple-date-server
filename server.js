// Couple Date - signaling server (step 1)
// Creates rooms, pairs two people, and relays chat + call setup messages.
// It never touches audio/video: those go directly between the phones.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I
const RELAY = new Set([
  'chat',
  'call-request', 'call-accept', 'call-decline', 'call-end',
  'offer', 'answer', 'ice',
  'screen-offer', 'screen-answer', 'screen-ice', 'screen-stop', 'screen-busy',
]);

const rooms = new Map(); // code -> { host, guest }

function genCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => ALPHABET[Math.floor(Math.random() * ALPHABET.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// A tiny web page so hosting services can see the server is alive.
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Couple Date server is running');
});
const wss = new WebSocketServer({ server });
server.listen(PORT, () => console.log(`Couple Date server running on port ${PORT}`));

wss.on('connection', (ws) => {
  ws.room = null;
  ws.role = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'create') {
      if (ws.room) return;
      const code = genCode();
      rooms.set(code, { host: ws, guest: null });
      ws.room = code;
      ws.role = 'host';
      return send(ws, { type: 'created', code });
    }

    if (msg.type === 'join') {
      if (ws.room) return;
      const code = String(msg.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'Room not found. Check the code.' });
      if (room.guest) return send(ws, { type: 'error', message: 'This room already has two people.' });
      room.guest = ws;
      ws.room = code;
      ws.role = 'guest';
      send(ws, { type: 'joined', code });
      return send(room.host, { type: 'peer-joined' });
    }

    if (!RELAY.has(msg.type)) return;
    const room = rooms.get(ws.room);
    if (!room) return;
    send(ws.role === 'host' ? room.guest : room.host, msg);
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    if (ws.role === 'host') {
      send(room.guest, { type: 'peer-left' });
      if (room.guest) room.guest.close();
      rooms.delete(ws.room);
    } else {
      room.guest = null;
      send(room.host, { type: 'peer-left' });
    }
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

