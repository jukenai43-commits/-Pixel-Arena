const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_BYTES = 16 * 1024;
const ROOM_RE = /^[a-z0-9][a-z0-9-]{0,59}$/;
const MAX_PLAYERS = 8;

const rooms = new Map();
let siguienteId = 1;

function responder(res, status, data) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(data));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    return res.end();
  }

  if (req.method === 'GET' && url.pathname === '/rooms') {
    const list = [];

    for (const [room, peers] of rooms.entries()) {
      if (!room.startsWith('arena-') || peers.size === 0) continue;
      const names = [...peers]
        .map(client => client.presence && client.presence.n)
        .filter(Boolean);
      list.push({
        room,
        code: room.slice('arena-'.length),
        players: peers.size,
        maxPlayers: MAX_PLAYERS,
        names
      });
    }

    list.sort((a, b) => b.players - a.players);
    return responder(res, 200, { rooms: list });
  }

  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Access-Control-Allow-Origin': '*'
  });
  res.end('Servidor de juegos pixel: OK');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_BYTES });

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
  for (const c of set) {
    enviar(c, { k: 'peers', room: nombre, peers: listaPeers(set, c) });
  }
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
      let set = rooms.get(m.room);
      if (!set) {
        set = new Set();
        rooms.set(m.room, set);
      }

      if (!set.has(ws) && set.size >= MAX_PLAYERS) {
        enviar(ws, { k: 'join_error', room: m.room, error: 'SALA_LLENA' });
        return;
      }

      ws.salas.add(m.room);
      set.add(ws);
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
          k: 'msg',
          room: m.room,
          topic: m.topic,
          data: m.data,
          peer: ws.id,
          sameTab: c === ws
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
