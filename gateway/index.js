// NOVA UNIVERSE — шлюз WhatsApp (Baileys): сопряжение по коду, приём и отправка сообщений.
// Вся логика бота живёт в Python (wa_core.py); шлюз только переправляет события по localhost.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import * as B from '@whiskeysockets/baileys';
import pino from 'pino';
import { restoreAuth, startBackup, backupNow, clearRemote } from './persist.js';

const pick = (n) => B[n] ?? B.default?.[n];
const makeWASocket = typeof B.default === 'function' ? B.default : (B.default?.default ?? B.makeWASocket);
const useMultiFileAuthState = pick('useMultiFileAuthState');
const fetchLatestBaileysVersion = pick('fetchLatestBaileysVersion');
const downloadMediaMessage = pick('downloadMediaMessage');
const makeCacheableSignalKeyStore = pick('makeCacheableSignalKeyStore');
const Browsers = pick('Browsers');
const DisconnectReason = pick('DisconnectReason') || { loggedOut: 401, connectionReplaced: 440 };

const log = pino({ level: process.env.LOG_LEVEL || 'info' });
const quiet = pino({ level: 'silent' });
const digits = (s) => String(s || '').replace(/\D/g, '');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const PORT = Number(process.env.GATEWAY_PORT || 3001);
const INTERNAL = process.env.INTERNAL_KEY || '';
const PY_URL = `http://127.0.0.1:${process.env.PORT || 10000}`;
const PHONE = digits(process.env.WA_PHONE);
const ALLOWED = new Set((process.env.ALLOWED_NUMBERS || '').split(',').map(digits).filter(Boolean));
const ALLOW_ALL = process.env.ALLOW_ALL === '1';
const AUTH_DIR = path.join(process.env.DATA_DIR || 'data', 'wa_auth');
const MARK = '\u2063'; // невидимый маркер: так бот узнаёт собственные ответы в чате «Мне»

if (!INTERNAL) { log.fatal('INTERNAL_KEY не задан'); process.exit(1); }

let sock = null;
let status = 'starting';
let readyForPair = false;
let lastCode = null;
let codeAt = 0;
let stopBackup = () => {};
let connecting = false;

const ownIds = () => {
  const u = sock?.user || {};
  return new Set([digits(String(u.id || '').split(':')[0].split('@')[0]), digits(String(u.lid || '').split(':')[0].split('@')[0]), PHONE].filter(Boolean));
};

async function connect() {
  if (connecting) return;
  connecting = true;
  try {
    try { sock?.ev?.removeAllListeners?.('connection.update'); sock?.ev?.removeAllListeners?.('messages.upsert'); sock?.end?.(undefined); } catch { /* старый сокет */ }
    readyForPair = false;
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    let version;
    try { ({ version } = await fetchLatestBaileysVersion()); } catch { /* используем версию библиотеки */ }
    const keys = makeCacheableSignalKeyStore ? makeCacheableSignalKeyStore(state.keys, quiet) : state.keys;
    sock = makeWASocket({
      ...(version ? { version } : {}),
      auth: { creds: state.creds, keys },
      logger: quiet,
      printQRInTerminal: false,
      browser: Browsers ? Browsers.ubuntu('Chrome') : ['Ubuntu', 'Chrome', '22.04'],
      markOnlineOnConnect: false,
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
    });
    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('connection.update', onConnection);
    sock.ev.on('messages.upsert', onMessages);
  } finally {
    connecting = false;
  }
}

async function onConnection(update) {
  const { connection, lastDisconnect, qr } = update;
  if (qr) {
    readyForPair = true; // код сопряжения можно запрашивать только после события qr
    if (!sock.authState.creds.registered) status = 'awaiting_pairing';
  }
  if (connection === 'open') {
    status = 'connected';
    lastCode = null;
    log.info({ me: sock.user?.id }, 'WhatsApp подключён');
    backupNow(AUTH_DIR, true, log);
  }
  if (connection === 'close') {
    const code = lastDisconnect?.error?.output?.statusCode;
    readyForPair = false;
    if (code === DisconnectReason.loggedOut) {
      status = 'logged_out';
      log.warn('Сессия завершена на телефоне — нужно новое сопряжение');
      fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      await clearRemote();
      setTimeout(connect, 2000);
    } else if (code === DisconnectReason.connectionReplaced) {
      status = 'replaced';
      log.warn('Сессия открыта в другом месте (например, второй экземпляр при деплое). Повтор через минуту.');
      setTimeout(connect, 60_000);
    } else {
      status = 'reconnecting';
      log.warn({ code }, 'соединение закрыто, переподключаюсь');
      setTimeout(connect, code === 515 ? 500 : 3000); // 515 — штатный перезапуск сразу после сопряжения
    }
  }
}

async function requestCode() {
  if (!sock || sock.authState.creds.registered) return { status };
  if (!PHONE) return { status: 'no_phone', error: 'WA_PHONE не задан' };
  if (!readyForPair) return { status: 'not_ready' };
  if (lastCode && Date.now() - codeAt < 55_000) return { status, code: lastCode, age: Math.round((Date.now() - codeAt) / 1000) };
  try {
    const raw = await sock.requestPairingCode(PHONE);
    lastCode = raw.length === 8 ? `${raw.slice(0, 4)}-${raw.slice(4)}` : raw;
    codeAt = Date.now();
    return { status, code: lastCode, age: 0 };
  } catch (e) {
    return { status: 'not_ready', error: e.message };
  }
}

// ───── входящие сообщения → Python ─────
const unwrap = (m) => m?.ephemeralMessage?.message ?? m?.viewOnceMessage?.message ?? m?.viewOnceMessageV2?.message
  ?? m?.documentWithCaptionMessage?.message ?? m;

async function onMessages({ messages, type }) {
  if (type !== 'notify') return;
  for (const m of messages) {
    try { await handleMsg(m); } catch (e) { log.error({ err: e.message }, 'handleMsg'); }
  }
}

async function handleMsg(m) {
  if (!m.message) return;
  const key = m.key;
  const jid = key.remoteJid || '';
  if (!jid || jid === 'status@broadcast' || jid.endsWith('@g.us') || jid.endsWith('@broadcast') || jid.endsWith('@newsletter')) return;
  const msg = unwrap(m.message);
  const text = msg.conversation ?? msg.extendedTextMessage?.text ?? '';
  const own = ownIds();
  const alt = key.remoteJidAlt || key.participantAlt || '';
  const cands = [jid, alt].map((j) => digits(String(j).split(':')[0].split('@')[0])).filter(Boolean);
  const self = cands.some((d) => own.has(d));
  if (key.fromMe) {
    if (!self || text.endsWith(MARK)) return; // чужие чаты и собственные ответы бота не трогаем
  }
  const allowed = self || ALLOW_ALL || cands.some((d) => ALLOWED.has(d));
  if (!allowed) return;

  let type = 'text', caption = '', filename = '', mime = '', media = null;
  if (msg.imageMessage) { type = 'image'; caption = msg.imageMessage.caption || ''; mime = msg.imageMessage.mimetype || ''; }
  else if (msg.stickerMessage) { type = 'sticker'; mime = msg.stickerMessage.mimetype || 'image/webp'; }
  else if (msg.documentMessage) {
    type = 'document'; caption = msg.documentMessage.caption || '';
    filename = msg.documentMessage.fileName || 'file'; mime = msg.documentMessage.mimetype || '';
  } else if (msg.audioMessage) type = 'audio';
  else if (msg.videoMessage) type = 'video';
  else if (!text) return;

  const wantMedia = type === 'image' || type === 'sticker' || (type === 'document'
    && (mime.startsWith('image/') || mime.startsWith('text/') || /\.(txt|md|py|js|ts|json|csv|log|ya?ml|xml|html|css|sql|sh|ini|toml)$/i.test(filename)));
  if (wantMedia) {
    const inner = msg.imageMessage || msg.stickerMessage || msg.documentMessage;
    if (Number(inner?.fileLength || 0) > 15_000_000) { type = 'toolarge'; }
    else {
      try {
        const buf = await downloadMediaMessage(m, 'buffer', {}, { logger: quiet, reuploadRequest: sock.updateMediaMessage });
        media = Buffer.from(buf).toString('base64');
      } catch (e) { log.warn({ err: e.message }, 'не удалось скачать медиа'); }
    }
  }
  try {
    if (!key.fromMe) await sock.readMessages([key]);
    await sock.sendPresenceUpdate('composing', jid);
  } catch { /* не критично */ }

  const fromDigits = self ? ([...own][0] || cands[0]) : (cands.find((d) => d !== digits(jid)) || cands[0]);
  const payload = {
    id: key.id, chat: jid, from: fromDigits, name: m.pushName || '', self, allowed: true,
    type, text, caption, filename, mime, media_b64: media,
  };
  for (let i = 0; i < 5; i++) { // Python может ещё запускаться
    try {
      const r = await fetch(`${PY_URL}/incoming`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'x-internal': INTERNAL }, body: JSON.stringify(payload),
      });
      if (r.ok) return;
      log.warn({ status: r.status }, 'Python не принял событие');
    } catch (e) { log.warn({ err: e.message }, 'Python пока недоступен'); }
    await sleep(2000);
  }
}

// ───── отправка (Python → WhatsApp), с очередью и «человеческими» паузами ─────
let queue = Promise.resolve();
function enqueue(fn) {
  const p = queue.then(fn, fn);
  queue = p.catch(() => {});
  return p;
}

async function doSend(body) {
  if (!sock || status !== 'connected') throw Object.assign(new Error('not connected'), { code: 503 });
  const chat = body.chat;
  const len = (body.text || '').length;
  try { await sock.sendPresenceUpdate('composing', chat); } catch { /* ignore */ }
  await sleep(400 + Math.random() * 700 + Math.min(len * 6, 2500));
  if (body.type === 'document') {
    await sock.sendMessage(chat, { document: Buffer.from(body.b64, 'base64'), fileName: body.filename || 'file.txt', mimetype: body.mime || 'text/plain' });
  } else {
    await sock.sendMessage(chat, { text: `${body.text}${MARK}` });
  }
  try { await sock.sendPresenceUpdate('paused', chat); } catch { /* ignore */ }
}

// ───── внутренний HTTP (только 127.0.0.1) ─────
const server = http.createServer(async (req, res) => {
  const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (req.headers['x-internal'] !== INTERNAL) return send(403, { error: 'forbidden' });
  const url = new URL(req.url, 'http://x');
  try {
    if (req.method === 'GET' && url.pathname === '/status') {
      return send(200, { status, registered: Boolean(sock?.authState?.creds?.registered), phone: PHONE ? `+${PHONE.slice(0, 3)}***${PHONE.slice(-2)}` : null });
    }
    if (req.method === 'GET' && url.pathname === '/pair') return send(200, await requestCode());
    if (req.method === 'POST' && url.pathname === '/send') {
      let raw = '';
      for await (const c of req) { raw += c; if (raw.length > 30_000_000) return send(413, { error: 'too large' }); }
      await enqueue(() => doSend(JSON.parse(raw)));
      return send(200, { ok: true });
    }
    send(404, { error: 'not found' });
  } catch (e) {
    send(e.code === 503 ? 503 : 500, { error: e.message });
  }
});
server.listen(PORT, '127.0.0.1', () => log.info(`шлюз слушает 127.0.0.1:${PORT}`));

// ───── запуск и остановка ─────
async function shutdown() {
  log.info('остановка, финальный бэкап сессии…');
  stopBackup();
  await Promise.race([backupNow(AUTH_DIR, true, log), sleep(20_000)]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('unhandledRejection', (e) => log.error({ err: String(e) }, 'unhandledRejection'));

fs.mkdirSync(AUTH_DIR, { recursive: true });
await restoreAuth(AUTH_DIR, log);
stopBackup = startBackup(AUTH_DIR, log);
await connect();
