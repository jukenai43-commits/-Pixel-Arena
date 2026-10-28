// Adaptador: ofrece window.claude.use('room') con la misma API que en claude.ai,
// pero conectándose a tu servidor propio. Dentro de claude.ai no hace nada.
(function () {
  if (window.claude) return;

  const SERVIDOR = window.SERVIDOR_JUEGOS || 'wss://TU-SERVIDOR.onrender.com';

  function unirse(nombre) {
    return new Promise((resolver, rechazar) => {
      const manejadores = {};
      const escuchas = [];
      const cola = [];
      let peers = [];
      let presencia = {};
      let listo = false;
      let cerradoPorUsuario = false;
      let ws = null;

      function enviar(obj) {
        const s = JSON.stringify(obj);
        if (ws && ws.readyState === 1) ws.send(s);
        else cola.push(s);
      }

      function avisarPeers() { escuchas.forEach(fn => fn(peers)); }

      const sala = {
        emit(topic, data) { enviar({ k: 'emit', room: nombre, topic, data }); },
        on(topic, fn) {
          (manejadores[topic] = manejadores[topic] || []).push(fn);
          return () => { manejadores[topic] = manejadores[topic].filter(f => f !== fn); };
        },
        onPeers(fn) { escuchas.push(fn); },
        presence(datos) {
          presencia = Object.assign({}, presencia, datos);
          enviar({ k: 'presence', room: nombre, data: presencia });
        },
        peers() { return peers; },
        leave() {
          cerradoPorUsuario = true;
          enviar({ k: 'leave', room: nombre });
          if (ws) ws.close();
        }
      };

      function conectar() {
        ws = new WebSocket(SERVIDOR);

        ws.onopen = () => {
          ws.send(JSON.stringify({ k: 'join', room: nombre }));
          if (Object.keys(presencia).length) ws.send(JSON.stringify({ k: 'presence', room: nombre, data: presencia }));
          while (cola.length) ws.send(cola.shift());
        };

        ws.onmessage = ev => {
          let m;
          try { m = JSON.parse(ev.data); } catch (e) { return; }
          if (m.k === 'joined' && !listo) { listo = true; resolver(sala); }
          else if (m.k === 'msg') {
            (manejadores[m.topic] || []).forEach(fn => fn({
              data: m.data, sameTab: !!m.sameTab, isMe: !!m.sameTab, peer: m.peer
            }));
          }
          else if (m.k === 'peers') {
            peers = (m.peers || []).map(p => ({
              peer: p.peer, presence: p.presence || {}, isMe: !!p.isMe, sameTab: !!p.isMe
            }));
            avisarPeers();
          }
        };

        ws.onerror = () => { if (!listo) rechazar(new Error('sin conexión')); };

        ws.onclose = () => {
          if (cerradoPorUsuario) return;
          // Reintenta sin que el jugador tenga que volver a entrar
          setTimeout(conectar, 2000);
        };
      }

      conectar();
    });
  }

  window.claude = {
    use(nombre) {
      return Promise.resolve(nombre === 'room' ? { join: unirse } : null);
    }
  };
})();
