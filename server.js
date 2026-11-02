const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') return res.end('ok');
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  fs.createReadStream(page).pipe(res);
});
const wss = new WebSocketServer({ server });
const games = new Map();

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const newPin = () => { let p; do p = String(Math.floor(10000 + Math.random() * 90000)); while (games.has(p)); return p; };
const list = g => [...g.players.values()];
const board = g => list(g).sort((a, b) => b.score - a.score);
const top = g => board(g).slice(0, 5).map(p => ({ name: p.name, score: p.score }));
const broadcast = (g, o) => { send(g.host, o); list(g).forEach(p => send(p.ws, o)); };

function clean(quiz) {
  if (!Array.isArray(quiz)) return [];
  return quiz.slice(0, 50).map(q => {
    const options = Array.isArray(q.options) ? q.options.slice(0, 4).map(o => String(o).slice(0, 100)) : [];
    return {
      text: String(q.text || '').slice(0, 200), options,
      correct: Number(q.correct), time: Math.min(60, Math.max(5, Number(q.time) || 20))
    };
  }).filter(q => q.text && q.options.length >= 2 && Number.isInteger(q.correct) && q.correct >= 0 && q.correct < q.options.length);
}

function ask(g) {
  const q = g.quiz[g.idx];
  g.state = 'question'; g.start = Date.now();
  list(g).forEach(p => { p.answer = null; });
  broadcast(g, { t: 'q', idx: g.idx, total: g.quiz.length, text: q.text, options: q.options, time: q.time });
  clearTimeout(g.timer);
  g.timer = setTimeout(() => reveal(g), q.time * 1000 + 300);
}

function reveal(g) {
  if (g.state !== 'question') return;
  clearTimeout(g.timer); g.state = 'reveal';
  const q = g.quiz[g.idx];
  const counts = q.options.map(() => 0);
  list(g).forEach(p => {
    p.pts = 0;
    if (!p.answer) return;
    counts[p.answer.i]++;
    if (p.answer.i === q.correct) {
      p.pts = Math.round(1000 * (1 - (p.answer.ms / (q.time * 1000)) / 2));
      p.score += p.pts;
    }
  });
  send(g.host, { t: 'reveal', correct: q.correct, counts, top: top(g), last: g.idx >= g.quiz.length - 1 });
  const b = board(g);
  b.forEach((p, r) => send(p.ws, {
    t: 'result', answered: !!p.answer, correct: !!p.answer && p.answer.i === q.correct,
    pts: p.pts, score: p.score, rank: r + 1, of: b.length
  }));
}

function finish(g) {
  g.state = 'end';
  const b = board(g);
  send(g.host, { t: 'end', top: top(g) });
  b.forEach((p, r) => send(p.ws, { t: 'end', rank: r + 1, score: p.score, of: b.length }));
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'host' && !ws.role) {
      const quiz = clean(m.quiz);
      if (!quiz.length) return send(ws, { t: 'err', msg: 'Kein gültiges Quiz gefunden.' });
      const g = { pin: newPin(), host: ws, quiz, players: new Map(), state: 'lobby', idx: 0 };
      games.set(g.pin, g); ws.game = g; ws.role = 'host';
      send(ws, { t: 'hosting', pin: g.pin });
    } else if (m.t === 'join' && !ws.role) {
      const g = games.get(String(m.pin).trim());
      if (!g) return send(ws, { t: 'err', msg: 'Diesen Code gibt es nicht.' });
      if (g.state !== 'lobby') return send(ws, { t: 'err', msg: 'Das Spiel läuft schon.' });
      const name = String(m.name || '').trim().slice(0, 16);
      if (!name) return send(ws, { t: 'err', msg: 'Bitte gib einen Namen ein.' });
      if (list(g).some(p => p.name.toLowerCase() === name.toLowerCase())) return send(ws, { t: 'err', msg: 'Dieser Name ist schon vergeben.' });
      g.players.set(ws, { ws, name, score: 0, answer: null, pts: 0 });
      ws.game = g; ws.role = 'player';
      send(ws, { t: 'joined' });
      send(g.host, { t: 'lobby', names: list(g).map(p => p.name) });
    } else if (ws.role === 'host') {
      const g = ws.game;
      if (m.t === 'start' && g.state === 'lobby' && g.players.size) { g.idx = 0; ask(g); }
      else if (m.t === 'skip') reveal(g);
      else if (m.t === 'next' && g.state === 'reveal') {
        if (g.idx >= g.quiz.length - 1) finish(g); else { g.idx++; ask(g); }
      }
    } else if (ws.role === 'player' && m.t === 'answer') {
      const g = ws.game, p = g.players.get(ws), q = g.quiz[g.idx], i = Number(m.i);
      if (g.state !== 'question' || p.answer || !Number.isInteger(i) || i < 0 || i >= q.options.length) return;
      p.answer = { i, ms: Math.min(Date.now() - g.start, q.time * 1000) };
      const n = list(g).filter(x => x.answer).length;
      send(g.host, { t: 'count', n, of: g.players.size });
      if (n === g.players.size) reveal(g);
    }
  });
  ws.on('close', () => {
    const g = ws.game; if (!g) return;
    if (ws.role === 'host') {
      clearTimeout(g.timer);
      list(g).forEach(p => send(p.ws, { t: 'err', msg: 'Der Host hat das Spiel beendet.' }));
      games.delete(g.pin);
    } else {
      g.players.delete(ws);
      if (g.state === 'lobby') send(g.host, { t: 'lobby', names: list(g).map(p => p.name) });
      else if (g.state === 'question' && g.players.size && list(g).every(p => p.answer)) reveal(g);
    }
  });
});

setInterval(() => wss.clients.forEach(ws => {
  if (!ws.isAlive) return ws.terminate();
  ws.isAlive = false; ws.ping();
}), 30000);

server.listen(PORT, () => console.log('Quizrunde läuft auf Port ' + PORT));
