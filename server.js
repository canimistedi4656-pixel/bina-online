// Yüksek Bina ve Çatı — online aktarma sunucusu
// Oyun mantığı istemcide; sunucu yalnızca aynı odadakilere konum ve sohbet iletir.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const MAX_PLAYERS = 4;                 // oda başına
const MAX_PER_IP = 12;                 // IP başına bağlantı
const ORIGINS = (process.env.ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // boşsa herkese açık

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Yuksek Bina online sunucusu calisiyor. Odalar: ' + rooms.size);
});
const wss = new WebSocketServer({ server, maxPayload: 1024 });
const rooms = new Map();               // kod -> { clients:Set }
const perIp = new Map();
let nextId = 1;

const num = (v, lim = 5000) => (typeof v === 'number' && isFinite(v)) ? Math.max(-lim, Math.min(lim, v)) : 0;
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, n);
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (room, o, except) => { for (const c of room.clients) if (c !== except) send(c, o); };

wss.on('connection', (ws, req) => {
  if (ORIGINS.length && !ORIGINS.includes(req.headers.origin || '')) { ws.close(1008, 'origin'); return; }
  const ip = ((req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?';
  const n = (perIp.get(ip) || 0) + 1;
  if (n > MAX_PER_IP) { ws.close(1013, 'cok fazla baglanti'); return; }
  perIp.set(ip, n);

  ws.alive = true; ws.room = null; ws.lastP = 0; ws.lastC = 0;
  ws.on('pong', () => { ws.alive = true; });
  const joinTimer = setTimeout(() => { if (!ws.room) ws.close(); }, 10000);

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    const now = Date.now();

    if (!ws.room) {                                   // önce odaya katıl
      if (m.t !== 'join') return;
      const code = String(m.room || '').toUpperCase();
      if (!/^[A-Z0-9]{3,8}$/.test(code)) { send(ws, { t: 'err', msg: 'Oda kodu 3-8 harf veya rakam olmalı' }); ws.close(); return; }
      let r = rooms.get(code);
      if (!r) { r = { clients: new Set() }; rooms.set(code, r); }
      if (r.clients.size >= MAX_PLAYERS) { send(ws, { t: 'err', msg: 'Oda dolu (en fazla ' + MAX_PLAYERS + ' kişi)' }); ws.close(); return; }
      ws.id = nextId++; ws.name = clean(m.name, 16) || ('Oyuncu' + ws.id); ws.code = code; ws.room = r;
      clearTimeout(joinTimer);
      const players = [...r.clients].map(c => ({ id: c.id, name: c.name }));
      r.clients.add(ws);
      send(ws, { t: 'w', id: ws.id, players });
      bcast(r, { t: 'j', id: ws.id, name: ws.name }, ws);
      return;
    }

    if (m.t === 'p') {                                // konum (saniyede en fazla ~25)
      if (now - ws.lastP < 40) return; ws.lastP = now;
      bcast(ws.room, { t: 'p', id: ws.id, x: num(m.x), y: num(m.y, 500), z: num(m.z), f: num(m.f, 10) }, ws);
    } else if (m.t === 'c') {                         // sohbet (en fazla ~1/sn)
      if (now - ws.lastC < 800) return; ws.lastC = now;
      const text = clean(m.text, 80); if (!text) return;
      bcast(ws.room, { t: 'c', id: ws.id, text }, ws);
    }
  });

  ws.on('close', () => {
    clearTimeout(joinTimer);
    const k = (perIp.get(ip) || 1) - 1; if (k > 0) perIp.set(ip, k); else perIp.delete(ip);
    if (ws.room) {
      ws.room.clients.delete(ws);
      bcast(ws.room, { t: 'l', id: ws.id });
      if (!ws.room.clients.size) rooms.delete(ws.code);
    }
  });
  ws.on('error', () => {});
});

setInterval(() => {                                   // ölü bağlantıları temizle
  for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} }
}, 20000);

server.listen(PORT, () => console.log('Yuksek Bina online sunucusu :' + PORT));
