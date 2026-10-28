// Servidor de salas en tiempo real para los juegos pixel.
// Reenvía mensajes y presencia entre los jugadores de cada sala.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_BYTES = 16 * 1024;
const ROOM_RE = /^[a-z0-9][a-z0-9-]{0,59}$/;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Servidor de juegos pixel: OK');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_BYTES });
const rooms = new Map(); // nombre de sala -> Set de conexiones
let siguienteId = 1;

function enviar(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function listaPeers(set, visor) {
  return [...set].map(c => ({
    peer: c.id,
    presence: c.presence || {},
    isMe: c === visor
  }));
}

function difundirPeers(nombre) {
  const set = rooms.get(nombre);
  if (!set) return;
  for (const c of set) enviar(c, { k: 'peers', room: nombre, peers: listaPeers(set, c) });
}

function salirDeSala(ws, nombre) {
  const set = rooms.get(nombre);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) rooms.delete(nombre);
  else difundirPeers(nombre);
}

wss.on('connection', ws => {
  ws.id = 'p' + (siguienteId++);
  ws.salas = new Set();
  ws.presence = {};

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.room !== 'string' || !ROOM_RE.test(m.room)) return;

    if (m.k === 'join') {
      ws.salas.add(m.room);
      if (!rooms.has(m.room)) rooms.set(m.room, new Set());
      rooms.get(m.room).add(ws);
      enviar(ws, { k: 'joined', room: m.room });
      difundirPeers(m.room);
      return;
    }
    if (!ws.salas.has(m.room)) return;

    if (m.k === 'leave') {
      ws.salas.delete(m.room);
      salirDeSala(ws, m.room);
    } else if (m.k === 'presence') {
      ws.presence = m.data && typeof m.data === 'object' ? m.data : {};
      difundirPeers(m.room);
    } else if (m.k === 'emit' && typeof m.topic === 'string') {
      for (const c of rooms.get(m.room) || []) {
        enviar(c, {
          k: 'msg', room: m.room, topic: m.topic, data: m.data,
          peer: ws.id, sameTab: c === ws
        });
      }
    }
  });

  ws.on('close', () => {
    for (const nombre of ws.salas) salirDeSala(ws, nombre);
    ws.salas.clear();
  });
});

server.listen(PORT, () => console.log('Servidor escuchando en el puerto ' + PORT));
