// 3D 무인도 타이쿤 온라인 — 한 사람이 섬 하나씩 가지고 함께 노는 서버 (정적 파일 + WebSocket)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const MAX_PLAYERS = 40;
const TICK_MS = 100;            // 1초에 10번 위치를 나눠 줌
const AFK_MS = Number(process.env.AFK_MS) || 15 * 60000; // 이만큼 아무것도 안 하면 내보냄
const EMOTES = 6;
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };

// ---------- 정적 파일 ----------
const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://x'); } catch (e) { res.writeHead(400); return res.end(); }
  if (url.pathname === '/healthz') { res.writeHead(200); return res.end('ok'); }
  let rel;
  try { rel = decodeURIComponent(url.pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (rel === '/') rel = '/index.html';
  const file = path.join(PUBLIC, path.normalize(rel));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    const ext = path.extname(file);
    res.writeHead(200, { 'Content-Type': TYPES[ext] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// ---------- 사람들 ----------
const players = new Map(); // id -> player
let nextId = 1;

const num = (v, lo, hi, d = 0) => (typeof v === 'number' && isFinite(v) ? Math.max(lo, Math.min(hi, v)) : d);
const r2 = (v) => Math.round(v * 100) / 100;
const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 10) || '무인도 주민';

function cleanIsland(o) {
  o = o && typeof o === 'object' ? o : {};
  return {
    e: !!o.e, n: !!o.n, h: !!o.h, lh: !!o.lh,
    lj: Math.round(num(o.lj, 0, 2)), mn: Math.round(num(o.mn, 0, 2)),
    lv: Math.round(num(o.lv, 1, 99, 1)),
  };
}

function freeSlot(want) {
  const used = new Set([...players.values()].filter((p) => p.slot != null).map((p) => p.slot));
  if (Number.isInteger(want) && want >= 0 && want < 200 && !used.has(want)) return want;
  for (let i = 0; ; i++) if (!used.has(i)) return i;
}

const info = (p) => ({ id: p.id, n: p.name, sl: p.slot, isl: p.isl });
function send(ws, m) { if (ws.readyState === 1) ws.send(JSON.stringify(m)); }
function broadcast(m, except) {
  const s = JSON.stringify(m);
  for (const p of players.values()) if (p.ready && p !== except && p.ws.readyState === 1) p.ws.send(s);
}

// 보내는 신호를 압축해 전송량을 줄임 (브라우저가 알아서 풂)
const wss = new WebSocketServer({ server, maxPayload: 4096 , perMessageDeflate: { threshold: 64, zlibDeflateOptions: { level: 3, memLevel: 7 }, concurrencyLimit: 8 } });

wss.on('connection', (ws) => {
  const p = { id: nextId++, ws, ready: false, slot: null, name: '', uid: '', isl: cleanIsland(), x: 0, z: 5, ry: 0, mv: 0, c: 0, w: [], last: Date.now(), lastEmo: 0 };

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'hello' && !p.ready) {
      p.uid = String(m.uid || '').slice(0, 40);
      // 같은 브라우저(같은 uid)로 또 들어오면 예전 창을 내보냄
      for (const o of players.values()) {
        if (o.ready && o.uid && o.uid === p.uid) {
          send(o.ws, { t: 'kick', why: '다른 창에서 같은 섬으로 다시 들어왔어요.' });
          o.ws.close();
          leave(o);
        }
      }
      if ([...players.values()].filter((o) => o.ready).length >= MAX_PLAYERS) {
        send(ws, { t: 'kick', why: `지금은 사람이 꽉 찼어요 (최대 ${MAX_PLAYERS}명). 조금 뒤에 다시 와 주세요!` });
        return ws.close();
      }
      p.name = cleanName(m.name);
      p.isl = cleanIsland(m.isl);
      p.slot = freeSlot(m.slot);
      p.ready = true;
      players.set(p.id, p);
      send(ws, { t: 'welcome', id: p.id, slot: p.slot, ps: [...players.values()].filter((o) => o.ready).map(info) });
      broadcast({ t: 'join', p: info(p) }, p);
      broadcast({ t: 'news', msg: `🏝️ ${p.name} 님의 섬이 바다 위에 나타났어요!` }, p);
      return;
    }
    if (!p.ready) return;

    if (m.t === 'p') {
      p.x = r2(num(m.x, -2000, 2000)); p.z = r2(num(m.z, -2000, 2000));
      p.ry = r2(num(m.ry, -10, 10)); p.mv = m.mv ? 1 : 0; p.c = Math.round(num(m.c, 0, 99));
      p.w = Array.isArray(m.w) ? m.w.slice(0, 4).map((a) => [a && a[0] === 1 ? 1 : 0, r2(num(a && a[1], -60, 60)), r2(num(a && a[2], -60, 60))]) : [];
      if (m.mv) p.last = Date.now();
    } else if (m.t === 'isl') {
      const before = p.isl;
      p.isl = cleanIsland(m.isl);
      broadcast({ t: 'info', p: info(p) });
      if (!before.lh && p.isl.lh) broadcast({ t: 'news', msg: `🗼 ${p.name} 님이 등대를 완공했어요! 축하해 주세요 🎉`, big: 1 });
      p.last = Date.now();
    } else if (m.t === 'emo') {
      const e = Math.round(num(m.e, 0, EMOTES - 1));
      if (Date.now() - p.lastEmo < 800) return;
      p.lastEmo = Date.now();
      broadcast({ t: 'emo', id: p.id, e });
      p.last = Date.now();
    } else if (m.t === 'visit') {
      const o = players.get(Math.round(num(m.id, 0, 1e9)));
      if (o && o !== p) send(o.ws, { t: 'news', msg: `👣 ${p.name} 님이 내 섬에 놀러 왔어요!` });
      p.last = Date.now();
    }
  });

  ws.on('close', () => leave(p));
  ws.on('error', () => {});
});

function leave(p) {
  if (!players.has(p.id)) return;
  players.delete(p.id);
  broadcast({ t: 'leave', id: p.id });
}

// 위치 나눠 주기
setInterval(() => {
  const now = Date.now();
  const ps = [];
  for (const p of players.values()) {
    if (!p.ready) continue;
    if (now - p.last > AFK_MS) {
      send(p.ws, { t: 'kick', why: '한동안 움직이지 않아서 잠시 쉬게 했어요. 새로고침하면 다시 들어올 수 있어요!' });
      p.ws.close();
      leave(p);
      continue;
    }
    ps.push([p.id, p.x, p.z, p.ry, p.mv, p.c, p.w]);
  }
  if (ps.length) broadcast({ t: 's', ps });
}, TICK_MS);

server.listen(PORT, () => console.log(`무인도 타이쿤 온라인: http://localhost:${PORT}`));
