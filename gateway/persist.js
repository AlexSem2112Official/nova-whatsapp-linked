// Резервная копия сессии WhatsApp (папка авторизации Baileys) в приватный GitHub-репозиторий.
// Нужна на бесплатном Render: диск стирается при каждом рестарте, а без неё пришлось бы заново вводить код сопряжения.
// Папка → JSON → gzip → AES-256-GCM (ключ из ENCRYPTION_KEY) → asset релиза "backup" с префиксом auth-.
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import crypto from 'node:crypto';

const API = 'https://api.github.com';
const PREFIX = 'auth-';
const TOKEN = (process.env.BACKUP_GITHUB_TOKEN || '').trim();
const REPO = (process.env.BACKUP_REPO || '').trim();
const SECRET = process.env.ENCRYPTION_KEY || '';
export const enabled = Boolean(TOKEN && REPO && SECRET);
const KEY = crypto.createHash('sha256').update(SECRET).digest();
let lastHash = null;

const H = (accept = 'application/vnd.github+json') => ({
  Authorization: `Bearer ${TOKEN}`, Accept: accept, 'User-Agent': 'nova-wa', 'X-GitHub-Api-Version': '2022-11-28',
});

export function encrypt(buf) {
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv('aes-256-gcm', KEY, iv);
  const enc = Buffer.concat([c.update(zlib.gzipSync(buf, { level: 6 })), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), enc]);
}

export function decrypt(blob) {
  const d = crypto.createDecipheriv('aes-256-gcm', KEY, blob.subarray(0, 12));
  d.setAuthTag(blob.subarray(12, 28));
  return zlib.gunzipSync(Buffer.concat([d.update(blob.subarray(28)), d.final()]));
}

function snapshot(dir) {
  if (!fs.existsSync(dir)) return null;
  const files = {};
  for (const name of fs.readdirSync(dir).sort()) {
    const p = path.join(dir, name);
    if (fs.statSync(p).isFile()) files[name] = fs.readFileSync(p).toString('base64');
  }
  return Object.keys(files).length ? Buffer.from(JSON.stringify(files)) : null;
}

async function releaseId(create) {
  let r = await fetch(`${API}/repos/${REPO}/releases/tags/backup`, { headers: H() });
  if (r.ok) return (await r.json()).id;
  if (!create) return null;
  r = await fetch(`${API}/repos/${REPO}/releases`, {
    method: 'POST', headers: H(),
    body: JSON.stringify({ tag_name: 'backup', name: 'NOVA backup', prerelease: true, body: 'Encrypted snapshots. Do not delete.' }),
  });
  return r.status === 201 ? (await r.json()).id : null;
}

async function assets(rid) {
  const r = await fetch(`${API}/repos/${REPO}/releases/${rid}/assets?per_page=100`, { headers: H() });
  return r.ok ? await r.json() : [];
}

export async function restoreAuth(dir, log = console) {
  if (!enabled) return false;
  if (fs.existsSync(path.join(dir, 'creds.json'))) return false;
  try {
    const rid = await releaseId(false);
    if (!rid) return false;
    const list = (await assets(rid)).filter((a) => a.name.startsWith(PREFIX)).sort((a, b) => (a.name < b.name ? 1 : -1));
    if (!list.length) return false;
    let r = await fetch(`${API}/repos/${REPO}/releases/assets/${list[0].id}`, { headers: H('application/octet-stream'), redirect: 'manual' });
    if (r.status >= 300 && r.status < 400) r = await fetch(r.headers.get('location')); // на хранилище токен не передаём
    const files = JSON.parse(decrypt(Buffer.from(await r.arrayBuffer())).toString());
    fs.mkdirSync(dir, { recursive: true });
    for (const [name, b64] of Object.entries(files)) fs.writeFileSync(path.join(dir, path.basename(name)), Buffer.from(b64, 'base64'));
    lastHash = crypto.createHash('sha256').update(JSON.stringify(files)).digest('hex');
    log.info?.('сессия WhatsApp восстановлена из резервной копии');
    return true;
  } catch (e) {
    log.error?.(`restore failed: ${e.message}`);
    return false;
  }
}

export async function backupNow(dir, force = false, log = console) {
  if (!enabled) return;
  try {
    const snap = snapshot(dir);
    if (!snap) return;
    const h = crypto.createHash('sha256').update(snap).digest('hex');
    if (h === lastHash && !force) return;
    const rid = await releaseId(true);
    if (!rid) return;
    const name = `${PREFIX}${Date.now()}.bin`;
    const up = await fetch(`https://uploads.github.com/repos/${REPO}/releases/${rid}/assets?name=${name}`, {
      method: 'POST', headers: { ...H(), 'Content-Type': 'application/octet-stream' }, body: encrypt(snap),
    });
    if (!up.ok) { log.warn?.(`backup upload failed: HTTP ${up.status}`); return; }
    lastHash = h;
    for (const a of await assets(rid)) {
      if (a.name.startsWith(PREFIX) && a.name !== name) await fetch(`${API}/repos/${REPO}/releases/assets/${a.id}`, { method: 'DELETE', headers: H() });
    }
  } catch (e) {
    log.error?.(`backup failed: ${e.message}`);
  }
}

export async function clearRemote() {
  if (!enabled) return;
  try {
    const rid = await releaseId(false);
    if (!rid) return;
    for (const a of await assets(rid)) {
      if (a.name.startsWith(PREFIX)) await fetch(`${API}/repos/${REPO}/releases/assets/${a.id}`, { method: 'DELETE', headers: H() });
    }
    lastHash = null;
  } catch { /* ignore */ }
}

export function startBackup(dir, log = console, everyMs = 60_000) {
  if (!enabled) return () => {};
  const t = setInterval(() => backupNow(dir, false, log), everyMs);
  return () => clearInterval(t);
}
