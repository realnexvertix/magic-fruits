const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Pool } = require('pg');
const { WebSocketServer } = require('ws');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
const ADMIN_PASSWORD = 'wertik3636';

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ==================== POSTGRES ====================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('render.com')
    ? { rejectUnauthorized: false }
    : false
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password TEXT NOT NULL,
      data     JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  console.log('[db] Таблица users готова');
}

function hashPassword(p) {
  return crypto.createHash('sha256').update(p + '::magic_fruits_salt').digest('hex');
}

// ==================== REST API ====================
app.post('/api/register', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || username.length < 3) return res.json({ ok: false, error: 'Ник минимум 3 символа' });
    if (!password || password.length < 3) return res.json({ ok: false, error: 'Пароль минимум 3 символа' });
    const exists = await pool.query('SELECT username FROM users WHERE username=$1', [username]);
    if (exists.rows.length > 0) return res.json({ ok: false, error: 'Ник уже занят' });
    await pool.query(
      'INSERT INTO users (username, password, data) VALUES ($1, $2, $3)',
      [username, hashPassword(password), {}]
    );
    res.json({ ok: true, data: {} });
  } catch (e) {
    console.error('[register]', e);
    res.json({ ok: false, error: 'Ошибка сервера' });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) return res.json({ ok: false, error: 'Введи ник и пароль' });
    const result = await pool.query('SELECT username, password, data FROM users WHERE username=$1', [username]);
    if (result.rows.length === 0) return res.json({ ok: false, error: 'Игрок не найден' });
    const row = result.rows[0];
    if (row.password !== hashPassword(password)) return res.json({ ok: false, error: 'Неверный пароль' });
    let data = {};
    if (row.data) data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
    res.json({ ok: true, data });
  } catch (e) {
    console.error('[login]', e);
    res.json({ ok: false, error: 'Ошибка сервера' });
  }
});

app.post('/api/save', async (req, res) => {
  try {
    const { username, password, data } = req.body || {};
    if (!username || !password) return res.json({ ok: false, error: 'Не авторизован' });
    const check = await pool.query('SELECT password FROM users WHERE username=$1', [username]);
    if (check.rows.length === 0) return res.json({ ok: false, error: 'Не авторизован' });
    if (check.rows[0].password !== hashPassword(password)) return res.json({ ok: false, error: 'Не авторизован' });
    await pool.query('UPDATE users SET data=$1 WHERE username=$2', [data || {}, username]);
    res.json({ ok: true });
  } catch (e) {
    console.error('[save]', e);
    res.json({ ok: false, error: 'Ошибка сервера' });
  }
});

// ==================== WEBSOCKET ====================
const wss = new WebSocketServer({ server });
const players = new Map();

const COLORS = ['#ffb300','#ff5566','#66ff99','#aaddff','#ffaaee','#ffdd88','#88ddff','#dd88ff','#c8ff88','#ffcc66','#88ffcc','#ff8844'];
function pickColor(username) {
  let h = 0;
  for (let i = 0; i < username.length; i++) h = (h * 31 + username.charCodeAt(i)) | 0;
  return COLORS[Math.abs(h) % COLORS.length];
}

function broadcastAll(msg) {
  const raw = JSON.stringify(msg);
  for (const p of players.values()) {
    if (p.ws.readyState === 1) { try { p.ws.send(raw); } catch (e) {} }
  }
}
function broadcastExcept(msg, exceptUsername) {
  const raw = JSON.stringify(msg);
  for (const [u, p] of players) {
    if (u === exceptUsername) continue;
    if (p.ws.readyState === 1) { try { p.ws.send(raw); } catch (e) {} }
  }
}
function snapshotExcept(exceptUsername) {
  const list = [];
  for (const [u, p] of players) {
    if (u === exceptUsername) continue;
    list.push({
      u, x: Math.round(p.x), y: Math.round(p.y),
      lx: +p.lx.toFixed(2), ly: +p.ly.toFixed(2),
      hp: Math.round(p.hp), mhp: p.mhp,
      c: p.color, wf: p.waterForm,
      held: p.held || null
    });
  }
  return list;
}

wss.on('connection', (ws) => {
  let username = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    if (msg.type === 'hello') {
      if (typeof msg.username !== 'string' || typeof msg.password !== 'string') {
        ws.send(JSON.stringify({ type: 'hello_err', error: 'Неверные данные' }));
        try { ws.close(); } catch (e) {}
        return;
      }
      let row;
      try {
        const result = await pool.query('SELECT password FROM users WHERE username=$1', [msg.username]);
        row = result.rows[0];
      } catch (e) {
        console.error('[ws hello]', e);
        ws.send(JSON.stringify({ type: 'hello_err', error: 'Ошибка БД' }));
        try { ws.close(); } catch (err) {}
        return;
      }
      if (!row || row.password !== hashPassword(msg.password)) {
        ws.send(JSON.stringify({ type: 'hello_err', error: 'Авторизация не пройдена' }));
        try { ws.close(); } catch (e) {}
        return;
      }
      username = msg.username;
      const prev = players.get(username);
      if (prev && prev.ws !== ws) { try { prev.ws.close(); } catch (e) {} }

      const me = {
        ws, username,
        x: 0, y: 0, lx: 1, ly: 0,
        hp: 100, mhp: 100,
        color: pickColor(username),
        waterForm: false,
        held: null,
        invulnUntil: 0,
        lastMoveTime: Date.now(),
        kills: 0, deaths: 0,
        hitCount: 0, hitResetAt: 0
      };
      players.set(username, me);

      ws.send(JSON.stringify({
        type: 'hello_ok',
        you: { u: username, c: me.color },
        players: snapshotExcept(username)
      }));

      broadcastExcept({
        type: 'join', u: username,
        x: me.x, y: me.y, lx: me.lx, ly: me.ly,
        hp: me.hp, mhp: me.mhp, c: me.color, wf: me.waterForm,
        held: me.held
      }, username);

      console.log(`[ws] + ${username} (online ${players.size})`);
      return;
    }

    if (!username) return;
    const me = players.get(username);
    if (!me) return;

    // -------- АДМИН: SET LEVEL --------
    if (msg.type === 'admin_setlevel') {
      if (msg.password !== ADMIN_PASSWORD) {
        ws.send(JSON.stringify({ type: 'admin_setlevel_err', error: 'Неверный пароль' }));
        return;
      }
      const target = String(msg.target || '').trim();
      let level = Number(msg.level);
      if (!target) {
        ws.send(JSON.stringify({ type: 'admin_setlevel_err', error: 'Не указано имя игрока' }));
        return;
      }
      if (!Number.isFinite(level)) {
        ws.send(JSON.stringify({ type: 'admin_setlevel_err', error: 'Неверный уровень' }));
        return;
      }
      level = Math.max(1, Math.min(20, Math.floor(level)));

      try {
        const r = await pool.query('SELECT data FROM users WHERE username=$1', [target]);
        if (r.rows.length === 0) {
          ws.send(JSON.stringify({ type: 'admin_setlevel_err', error: 'Игрок не найден' }));
          return;
        }
        let data = r.rows[0].data || {};
        if (typeof data === 'string') data = JSON.parse(data);
        data.level = level;
        await pool.query('UPDATE users SET data=$1 WHERE username=$2', [data, target]);
        ws.send(JSON.stringify({ type: 'admin_setlevel_ok', target, level }));
        console.log(`[admin] ${username} → ${target}: level ${level}`);
        const targetPlayer = players.get(target);
        if (targetPlayer && targetPlayer.ws.readyState === 1) {
          try {
            targetPlayer.ws.send(JSON.stringify({ type: 'admin_setlevel_apply', level }));
          } catch (e) {}
        }
      } catch (e) {
        console.error('[admin_setlevel]', e);
        ws.send(JSON.stringify({ type: 'admin_setlevel_err', error: 'Ошибка БД' }));
      }
      return;
    }

    // -------- АДМИН: SET MASTERY --------
    // Формат: { type: 'admin_setmastery', password, target, fruit, mastery }
    // Пример: g mastery 5 акула Test1  →  target=Test1, fruit='Акула', mastery=5
    if (msg.type === 'admin_setmastery') {
      if (msg.password !== ADMIN_PASSWORD) {
        ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Неверный пароль' }));
        return;
      }
      const target = String(msg.target || '').trim();
      const fruitName = String(msg.fruit || '').trim();
      let masteryLvl = Number(msg.mastery);
      if (!target) {
        ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Не указано имя игрока' }));
        return;
      }
      if (!fruitName) {
        ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Не указан фрукт' }));
        return;
      }
      if (!Number.isFinite(masteryLvl)) {
        ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Неверный уровень mastery' }));
        return;
      }
      masteryLvl = Math.max(0, Math.min(100, Math.floor(masteryLvl)));

      try {
        const r = await pool.query('SELECT data FROM users WHERE username=$1', [target]);
        if (r.rows.length === 0) {
          ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Игрок не найден' }));
          return;
        }
        let data = r.rows[0].data || {};
        if (typeof data === 'string') data = JSON.parse(data);
        if (!data.powerMastery || typeof data.powerMastery !== 'object') data.powerMastery = {};
        data.powerMastery[fruitName] = { xp: 0, level: masteryLvl };
        await pool.query('UPDATE users SET data=$1 WHERE username=$2', [data, target]);
        ws.send(JSON.stringify({ type: 'admin_setmastery_ok', target, fruit: fruitName, mastery: masteryLvl }));
        console.log(`[admin] ${username} → ${target}: ${fruitName} mastery ${masteryLvl}`);

        const targetPlayer = players.get(target);
        if (targetPlayer && targetPlayer.ws.readyState === 1) {
          try {
            targetPlayer.ws.send(JSON.stringify({
              type: 'admin_setmastery_apply',
              fruit: fruitName,
              mastery: masteryLvl
            }));
          } catch (e) {}
        }
      } catch (e) {
        console.error('[admin_setmastery]', e);
        ws.send(JSON.stringify({ type: 'admin_setmastery_err', error: 'Ошибка БД' }));
      }
      return;
    }

    // -------- ДВИЖЕНИЕ --------
    if (msg.type === 'move') {
      const nx = Number(msg.x), ny = Number(msg.y);
      if (!Number.isFinite(nx) || !Number.isFinite(ny)) return;
      const now = Date.now();
      const dt = Math.max(0.01, (now - me.lastMoveTime) / 1000);
      me.lastMoveTime = now;
      const dx = nx - me.x, dy = ny - me.y;
      const dist = Math.hypot(dx, dy);
      const maxDist = 600 * dt + 60;
      if (dist > maxDist && dist > 0) {
        const f = maxDist / dist;
        me.x = me.x + dx * f;
        me.y = me.y + dy * f;
      } else {
        me.x = nx; me.y = ny;
      }
      if (typeof msg.lx === 'number' && Number.isFinite(msg.lx)) me.lx = msg.lx;
      if (typeof msg.ly === 'number' && Number.isFinite(msg.ly)) me.ly = msg.ly;
      if (typeof msg.hp === 'number' && Number.isFinite(msg.hp)) me.hp = Math.max(0, Math.min(msg.hp, me.mhp));
      if (typeof msg.mhp === 'number' && Number.isFinite(msg.mhp)) me.mhp = msg.mhp;
      if (typeof msg.wf === 'boolean') me.waterForm = msg.wf;
      if (msg.held === null || (typeof msg.held === 'string' && msg.held.length > 0)) me.held = msg.held;

      broadcastExcept({
        type: 'pos', u: username,
        x: Math.round(me.x), y: Math.round(me.y),
        lx: +me.lx.toFixed(2), ly: +me.ly.toFixed(2),
        hp: Math.round(me.hp), mhp: me.mhp,
        c: me.color, wf: me.waterForm,
        held: me.held
      }, username);
      return;
    }

    // -------- ВИЗУАЛ АТАКИ (рассылаем всем) --------
    if (msg.type === 'attack_use') {
      broadcastExcept({
        type: 'remote_attack',
        from: username,
        abilityId: String(msg.abilityId || ''),
        x: Number(msg.x) || 0,
        y: Number(msg.y) || 0,
        dirX: Number(msg.dirX) || 0,
        dirY: Number(msg.dirY) || 0
      }, username);
      return;
    }

    // -------- ФРУКТ В РУКЕ --------
    if (msg.type === 'held_change') {
      me.held = (msg.held === null || typeof msg.held === 'string') ? msg.held : me.held;
      broadcastExcept({
        type: 'pos', u: username,
        x: Math.round(me.x), y: Math.round(me.y),
        lx: +me.lx.toFixed(2), ly: +me.ly.toFixed(2),
        hp: Math.round(me.hp), mhp: me.mhp,
        c: me.color, wf: me.waterForm,
        held: me.held
      }, username);
      return;
    }

    // -------- БРОСОК ФРУКТА --------
    if (msg.type === 'drop_fruit') {
      broadcastAll({
        type: 'fruit_dropped',
        from: username,
        fruitId: String(msg.fruitId || ''),
        name: String(msg.name || ''),
        color: String(msg.color || '#888'),
        x: Number(msg.x) || me.x,
        y: Number(msg.y) || me.y
      });
      return;
    }

    // -------- ПОДБОР ФРУКТА (сообщаем всем, чтобы убрали у себя) --------
    if (msg.type === 'fruit_picked') {
      broadcastAll({
        type: 'fruit_picked',
        from: username,
        fruitId: String(msg.fruitId || '')
      });
      return;
    }

    // -------- PvP --------
    if (msg.type === 'hit') {
      const target = players.get(msg.target);
      if (!target || target.username === username) return;
      const now = Date.now();

      if (now < me.hitResetAt) {
        if (me.hitCount >= 10) return;
        me.hitCount++;
      } else {
        me.hitResetAt = now + 1000;
        me.hitCount = 1;
      }
      if (now < target.invulnUntil) return;

      let dmg = Number(msg.dmg);
      if (!Number.isFinite(dmg)) return;
      dmg = Math.max(1, Math.min(80, Math.round(dmg)));
      if (target.waterForm) dmg = Math.max(1, Math.floor(dmg * 0.8));

      const dist = Math.hypot(target.x - me.x, target.y - me.y);
      const kind = msg.kind === 'ranged' ? 'ranged' : 'melee';
      if (kind === 'melee' && dist > 260) return;
      if (kind === 'ranged' && dist > 1500) return;

      target.hp = Math.max(0, target.hp - dmg);
      target.invulnUntil = now + 400;

      if (target.ws.readyState === 1) {
        try {
          target.ws.send(JSON.stringify({
            type: 'hurt', from: username,
            dmg, hp: Math.round(target.hp)
          }));
        } catch (e) {}
      }

      broadcastAll({
        type: 'hitfx', from: username, target: target.username,
        dmg, x: Math.round(target.x), y: Math.round(target.y)
      });

      if (target.hp <= 0) {
        target.deaths++;
        me.kills++;
        target.hp = target.mhp;
        target.x = 0; target.y = 0;
        target.invulnUntil = now + 2000;
        if (target.ws.readyState === 1) {
          try {
            target.ws.send(JSON.stringify({
              type: 'respawn', x: 0, y: 0, hp: Math.round(target.hp)
            }));
          } catch (e) {}
        }
        broadcastAll({ type: 'kill', from: username, target: target.username });
      }
      return;
    }
  });

  ws.on('close', () => {
    if (username) {
      const p = players.get(username);
      if (p && p.ws === ws) {
        players.delete(username);
        broadcastAll({ type: 'leave', u: username });
        console.log(`[ws] - ${username} (online ${players.size})`);
      }
    }
  });

  ws.on('error', (e) => { console.error('[ws err]', e.message); });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

initDb().then(() => {
  server.listen(PORT, () => {
    console.log(`Magic Fruits listening on port ${PORT}`);
  });
}).catch(err => {
  console.error('[FATAL] Не удалось подключиться к БД:', err);
  server.listen(PORT, () => {
    console.log(`Magic Fruits listening on port ${PORT} (БД не подключена!)`);
  });
});
