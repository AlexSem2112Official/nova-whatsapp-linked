"""NOVA UNIVERSE — ядро WhatsApp-бота (Python). Подключение к аккаунту WhatsApp (сопряжение по коду)
выполняет Node-шлюз на Baileys (папка gateway/); сюда он присылает входящие сообщения по localhost,
а ответы забирает обратно. Здесь: провайдеры, сессии, память, навыки, vision, формы ключей.
"""
import asyncio
import base64
import hashlib
import hmac
import html
import io
import ipaddress
import json
import logging
import math
import os
import re
import secrets
import signal
import socket
import time
from collections import OrderedDict
from urllib.parse import urlparse

import aiohttp
import aiosqlite
from aiohttp import web
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from PIL import Image

from persist import Backup

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nova-wa")

# ───────────────────────── Конфиг ─────────────────────────
INTERNAL_KEY = os.getenv("INTERNAL_KEY", "").strip()      # общий секрет шлюза и ядра (задаёт start.sh)
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://127.0.0.1:3001")
PAIR_KEY = os.getenv("PAIR_KEY", "").strip()              # ключ для /pair и /status (код сопряжения)
PUBLIC_URL = (os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
DEFAULT_OR_KEY = os.getenv("DEFAULT_OPENROUTER_KEY", "").strip()
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "NOVA-Spark-5")
ALLOW_PRIVATE = os.getenv("ALLOW_PRIVATE_URLS", "0") == "1"
HISTORY_CHARS = int(os.getenv("HISTORY_CHARS", "60000"))
HISTORY_MSGS = 80
SESSION_CAP = 400
Image.MAX_IMAGE_PIXELS = 50_000_000
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "nova.db")
WA_STYLE = ("\n\nТы отвечаешь в WhatsApp. Формат: *жирный*, _курсив_, ```код```; без таблиц и заголовков с #. "
            "Старайся уложиться примерно в 3500 символов.")


# ───────────────────────── Общее ядро (БД, провайдеры, vision, сессии) ─────────────────────────


def load_fernet() -> Fernet:
    k = os.getenv("ENCRYPTION_KEY", "").strip()
    if not k:
        p = os.path.join(DATA_DIR, "secret.key")
        if os.path.exists(p):
            k = open(p).read().strip()
        else:
            k = Fernet.generate_key().decode()
            with open(p, "w") as f:
                f.write(k)
            os.chmod(p, 0o600)
    return Fernet(k.encode())


fernet = load_fernet()


BUILTIN = {
    "openrouter": ("OpenRouter", "https://openrouter.ai/api/v1"),
    "google": ("Google AI Studio", "https://generativelanguage.googleapis.com/v1beta/openai"),
    "mistral": ("Mistral AI", "https://api.mistral.ai/v1"),
    "groq": ("Groq", "https://api.groq.com/openai/v1"),
    "deepseek": ("DeepSeek", "https://api.deepseek.com/v1"),
    "openai": ("OpenAI", "https://api.openai.com/v1"),
    "anthropic": ("Claude (Anthropic)", "https://api.anthropic.com/v1"),
    "xai": ("xAI (Grok)", "https://api.x.ai/v1"),
    "together": ("Together AI", "https://api.together.xyz/v1"),
    "perplexity": ("Perplexity", "https://api.perplexity.ai"),
    "cerebras": ("Cerebras", "https://api.cerebras.ai/v1"),
    "fireworks": ("Fireworks AI", "https://api.fireworks.ai/inference/v1"),
    "deepinfra": ("DeepInfra", "https://api.deepinfra.com/v1/openai"),
    "sambanova": ("SambaNova", "https://api.sambanova.ai/v1"),
    "novita": ("Novita AI", "https://api.novita.ai/v3/openai"),
    "nvidia": ("NVIDIA NIM", "https://integrate.api.nvidia.com/v1"),
    "huggingface": ("Hugging Face", "https://router.huggingface.co/v1"),
}


PROVIDER_DEFAULT_MODEL = {
    "openrouter": DEFAULT_MODEL, "google": "gemini-2.0-flash", "mistral": "mistral-small-latest",
    "groq": "llama-3.3-70b-versatile", "deepseek": "deepseek-chat", "openai": "gpt-4o-mini",
}


BASE_RULES = "Отвечай на языке пользователя. Будь точным; не выдумывай факты."


MODES = {
    "normal": ("💬 Обычный", "Ты — NOVA, дружелюбный и полезный ИИ-ассистент. " + BASE_RULES),
    "expert": ("🎓 Эксперт", "Ты — NOVA в режиме эксперта. Давай глубокие, структурированные ответы: "
               "сначала вывод, затем аргументы, нюансы и ограничения. " + BASE_RULES),
    "code": ("💻 NOVA Code", "Ты — NOVA Code, старший инженер. Сначала уточни структуру и контекст, "
             "предлагай минимальные патчи, показывай код блоками, объясняй риски. Финальный файл "
             "собирай только после согласования. " + BASE_RULES),
    "tutor": ("📚 Репетитор", "Ты — NOVA Репетитор. Перед началом задай 2–3 вопроса об уровне ученика. "
              "Разбирай тему по шагам, давай практику и домашнее задание, проверяй знания, не выдавай "
              "готовый ответ сразу, а веди к нему. " + BASE_RULES),
    "klyro": ("🧠 Klyro", "Ты — NOVA в режиме Klyro Engine. На сложные вопросы отвечай структурой: "
              "Решение; Уверенность (%); Причины; Риски и противоречия; Альтернативы; Следующий шаг. "
              "На простые вопросы отвечай коротко. " + BASE_RULES),
}


db: aiosqlite.Connection


session: aiohttp.ClientSession


class ProviderError(Exception):
    def __init__(self, msg, status=0):
        super().__init__(msg)
        self.status = status


VMETA: dict[str, dict] = {}


VISION_RE = re.compile(
    r"(gpt-4o|gpt-4\.1|gpt-4-turbo|gpt-5|o4-mini|gemini|claude|llava|vision|-vl\b|-vl-|/vl|pixtral|"
    r"llama-?3\.2-(11|90)b|llama-4|grok-(4|vision|2-vision)|qwen.*vl|internvl|minicpm-v|gemma-?3|"
    r"mistral-(small|medium)|molmo)", re.I)


async def ensure_column(table, col, decl):
    cols = [r["name"] for r in await fetch(f"PRAGMA table_info({table})")]
    if col not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.executescript("""
    CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY, provider TEXT, model TEXT,
        mode TEXT, temperature REAL, sys TEXT);
    CREATE TABLE IF NOT EXISTS keys(user_id INTEGER, provider TEXT, key_enc TEXT,
        PRIMARY KEY(user_id, provider));
    CREATE TABLE IF NOT EXISTS custom_providers(user_id INTEGER, name TEXT, base_url TEXT, model TEXT,
        PRIMARY KEY(user_id, name));
    CREATE TABLE IF NOT EXISTS memory(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, text TEXT);
    CREATE TABLE IF NOT EXISTS skills(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
        name TEXT, text TEXT);
    CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
        role TEXT, content TEXT);
    CREATE TABLE IF NOT EXISTS sessions(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
        title TEXT, created REAL, updated REAL);
    """)
    await ensure_column("users", "session_id", "INTEGER")
    await ensure_column("history", "session_id", "INTEGER")
    await ensure_column("history", "img", "TEXT")
    await ensure_column("custom_providers", "model", "TEXT")
    await db.commit()


async def fetch(sql, args=()):
    async with db.execute(sql, args) as c:
        return await c.fetchall()


async def run(sql, args=()):
    await db.execute(sql, args)
    await db.commit()


async def get_user(uid):
    rows = await fetch("SELECT * FROM users WHERE user_id=?", (uid,))
    if not rows:
        await run("INSERT INTO users(user_id,provider,model,mode,temperature,sys) VALUES(?,?,?,?,?,?)",
                  (uid, "openrouter", DEFAULT_MODEL, "normal", 0.7, ""))
        rows = await fetch("SELECT * FROM users WHERE user_id=?", (uid,))
    return rows[0]


async def upd(uid, **kw):
    await get_user(uid)
    cols = ", ".join(f"{k}=?" for k in kw)
    await run(f"UPDATE users SET {cols} WHERE user_id=?", (*kw.values(), uid))


async def set_key(uid, pid, key):
    await run("INSERT OR REPLACE INTO keys VALUES(?,?,?)", (uid, pid, fernet.encrypt(key.encode()).decode()))


async def get_key(uid, pid):
    rows = await fetch("SELECT key_enc FROM keys WHERE user_id=? AND provider=?", (uid, pid))
    if not rows:
        return None
    try:
        return fernet.decrypt(rows[0]["key_enc"].encode()).decode()
    except Exception:
        return None


async def check_url(url: str) -> str:
    """Защита от SSRF: только https и только публичные адреса."""
    p = urlparse(url.strip())
    if p.scheme not in ("https", "http") or not p.hostname:
        raise ValueError("Нужен адрес вида https://api.example.com/v1")
    if not ALLOW_PRIVATE:
        if p.scheme != "https":
            raise ValueError("Разрешён только https.")
        try:
            infos = await asyncio.to_thread(socket.getaddrinfo, p.hostname, p.port or 443)
        except socket.gaierror:
            raise ValueError("Не удалось найти такой домен.")
        for info in infos:
            ip = ipaddress.ip_address(info[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                raise ValueError("Адреса внутренней сети запрещены.")
    return url.strip().rstrip("/")


async def all_providers(uid):
    res = [(pid, name, base) for pid, (name, base) in BUILTIN.items()]
    for r in await fetch("SELECT name, base_url FROM custom_providers WHERE user_id=?", (uid,)):
        res.append((f"c:{r['name']}", f"{r['name']} (свой)", r["base_url"]))
    return res


async def resolve(uid, pid):
    if pid in BUILTIN:
        name, base = BUILTIN[pid]
        key = await get_key(uid, pid)
        if not key and pid == "openrouter":
            key = DEFAULT_OR_KEY or None
        return dict(id=pid, name=name, base=base, key=key, custom=False)
    if pid.startswith("c:"):
        rows = await fetch("SELECT base_url FROM custom_providers WHERE user_id=? AND name=?", (uid, pid[2:]))
        if rows:
            base = await check_url(rows[0]["base_url"])
            return dict(id=pid, name=pid[2:], base=base, key=await get_key(uid, pid), custom=True)
    return None


async def resolve_name(uid, pid):
    if pid in BUILTIN:
        return BUILTIN[pid][0]
    return pid[2:] + " (свой)"


def auth_headers(base, key):
    h = {"Content-Type": "application/json"}
    if key:
        h["Authorization"] = f"Bearer {key}"
    if "openrouter.ai" in base:
        h["X-Title"] = "NOVA UNIVERSE Bot"
    return h


def model_has_vision(m):
    """True/False, если провайдер сообщает модальности модели; None — если данных нет."""
    if not isinstance(m, dict):
        return None
    arch = m.get("architecture") or {}
    mods = arch.get("input_modalities")
    if isinstance(mods, list):
        return "image" in mods
    mod = arch.get("modality")
    if isinstance(mod, str):
        return "image" in mod.split("->")[0]
    caps = m.get("capabilities")
    if isinstance(caps, dict) and "vision" in caps:
        return bool(caps["vision"])
    return None


async def fetch_models(base, key):
    """→ (список id, множество id с vision, есть ли метаданные о vision)"""
    async with session.get(base + "/models", headers=auth_headers(base, key), allow_redirects=False,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            raise ProviderError(f"HTTP {r.status}", r.status)
        d = await r.json(content_type=None)
    items = d.get("data", []) if isinstance(d, dict) else d
    ids, vis, meta = [], set(), False
    for m in items:
        if not m:
            continue
        mid = m["id"] if isinstance(m, dict) else str(m)
        ids.append(mid)
        v = model_has_vision(m)
        if v is not None:
            meta = True
            if v:
                vis.add(mid)
    ids = sorted(set(ids))
    if meta:
        VMETA[base] = {"all": set(ids), "vis": vis, "ts": time.time()}
    return ids, vis, meta


async def vision_status(prov, model):
    """True — модель видит картинки, False — точно нет, None — неизвестно."""
    base = prov["base"]
    stale = base not in VMETA or time.time() - VMETA[base]["ts"] > 3600
    if "openrouter.ai" in base and stale:
        try:
            await fetch_models(base, prov["key"])
        except Exception:
            pass
    meta = VMETA.get(base)
    if meta and model in meta["all"]:
        return model in meta["vis"]
    if VISION_RE.search(model or ""):
        return True
    return None


def prep_image(data: bytes) -> str:
    """Любую картинку (jpeg/png/webp-стикер…) → JPEG ≤1568px в base64."""
    im = Image.open(io.BytesIO(data))
    im.load()
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        bg = Image.new("RGB", im.size, (255, 255, 255))
        bg.paste(im, mask=im.split()[-1])
        im = bg
    else:
        im = im.convert("RGB")
    im.thumbnail((1568, 1568))
    out = io.BytesIO()
    im.save(out, "JPEG", quality=85)
    return base64.b64encode(out.getvalue()).decode()


def image_content(prompt, img):
    if not img:
        return prompt
    return [{"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + img}}]


def split_text(t: str, n: int = 4000):
    out = []
    while len(t) > n:
        cut = t.rfind("\n", 0, n)
        cut = cut if cut > n // 2 else n
        out.append(t[:cut])
        t = t[cut:].lstrip("\n")
    return out + [t]


async def new_session(uid, title="Новый чат"):
    now = time.time()
    cur = await db.execute("INSERT INTO sessions(user_id,title,created,updated) VALUES(?,?,?,?)",
                           (uid, title[:60], now, now))
    await db.commit()
    sid = cur.lastrowid
    await run("UPDATE history SET session_id=? WHERE user_id=? AND session_id IS NULL", (sid, uid))  # старая история
    await upd(uid, session_id=sid)
    return sid


async def own_session(uid, sid):
    rows = await fetch("SELECT * FROM sessions WHERE id=? AND user_id=?", (sid, uid))
    return rows[0] if rows else None


async def active_session(uid):
    u = await get_user(uid)
    if u["session_id"] and await own_session(uid, u["session_id"]):
        return u["session_id"]
    return await new_session(uid)


async def msg_count(sid):
    return (await fetch("SELECT COUNT(*) c FROM history WHERE session_id=?", (sid,)))[0]["c"]


async def delete_session(uid, sid):
    await run("DELETE FROM history WHERE session_id=?", (sid,))
    await run("DELETE FROM sessions WHERE id=? AND user_id=?", (sid, uid))
    u = await get_user(uid)
    if u["session_id"] == sid:
        rows = await fetch("SELECT id FROM sessions WHERE user_id=? ORDER BY updated DESC LIMIT 1", (uid,))
        if rows:
            await upd(uid, session_id=rows[0]["id"])
        else:
            await new_session(uid)


async def load_history(sid, vision):
    rows = await fetch("SELECT role, content, img FROM history WHERE session_id=? ORDER BY id DESC LIMIT ?",
                       (sid, HISTORY_MSGS))
    out, total = [], 0
    for r in rows:
        total += len(r["content"])
        if total > HISTORY_CHARS and out:
            break
        c = r["content"]
        if r["img"] and r["role"] == "user" and vision is not False:
            c = image_content(c, r["img"])
        out.append({"role": r["role"], "content": c})
    out.reverse()
    while out and out[0]["role"] != "user":
        out.pop(0)
    return out


# ───────────────────────── Персона, NOVA-Spark-5, лимиты ─────────────────────────
SPARK_NAME = "NOVA-Spark-5"          # то, что видит пользователь
SPARK_ROUTER = "openrouter/free"     # скрытая реальная модель (Free Models Router на OpenRouter)
LIMIT_MSG = "⚠️ Лимит закончился. Попробуйте позже."
RATE_PER_HOUR = int(os.getenv("RATE_LIMIT_PER_HOUR", "40"))   # защита публичного номера от флуда
RATE: dict[int, list] = {}


def load_persona():
    """Системный промпт NOVA-Spark-5 из persona.txt: добавляется к любой модели."""
    p = os.getenv("PERSONA_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "persona.txt")
    try:
        with open(p, encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        log.warning("persona.txt не найден: системный промпт NOVA не будет добавлен")
        return ""


PERSONA = load_persona()
MODE_NOTES = {
    "normal": "",
    "expert": "Режим «Эксперт»: давай глубокие структурированные ответы (вывод, аргументы, нюансы), здесь можно подробнее обычного.",
    "code": "Режим «NOVA Code»: ты старший инженер. Предлагай минимальные патчи, код давай блоками, объясняй риски; финальный файл собирай после согласования.",
    "tutor": "Режим «Репетитор»: сначала задай 2–3 вопроса об уровне, разбирай тему по шагам, давай практику, не выдавай ответ сразу, а веди к нему.",
    "klyro": "Режим «Klyro»: на сложные вопросы отвечай структурой: Решение; Уверенность (%); Причины; Риски; Альтернативы; Следующий шаг. На простые отвечай коротко.",
}

_vision_status_orig = vision_status


async def vision_status(prov, model):
    """NOVA-Spark-5 понимает изображения (роутер сам выбирает подходящую бесплатную модель)."""
    if model == SPARK_NAME:
        return True
    return await _vision_status_orig(prov, model)


def rate_ok(uid):
    now = time.time()
    q = [t for t in RATE.get(uid, []) if now - t < 3600]
    ok = len(q) < RATE_PER_HOUR
    if ok:
        q.append(now)
    RATE[uid] = q
    return ok


# ───────────────────────── Отправка через шлюз WhatsApp ─────────────────────────
CHATS: dict[int, str] = {}        # user_id -> JID чата, куда отвечать
PENDING: dict[str, dict] = {}     # chat -> текущее числовое меню


def trunc(s, n):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


async def gw(path, payload):
    try:
        async with session.post(f"{GATEWAY_URL}{path}", json=payload, headers={"X-Internal": INTERNAL_KEY},
                                timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status >= 300:
                log.warning("шлюз %s: HTTP %s %s", path, r.status, (await r.text())[:200])
            return r.status < 300
    except Exception as e:
        log.warning("шлюз %s: %s", path, type(e).__name__)
        return False


async def send_text(to, text):
    for chunk in split_text((text or "").strip() or "…", 4000):
        await gw("/send", {"chat": to, "type": "text", "text": chunk})


async def send_menu(to, body, rows, hint="Ответьте номером пункта."):
    """Кнопки WhatsApp недоступны для обычных аккаунтов, поэтому меню — пронумерованный список."""
    lines, opts = [body, ""], {}
    for i, r in enumerate(rows, 1):
        opts[str(i)] = r[0]
        lines.append(f"{i}. {r[1]}" + (f" — {r[2]}" if len(r) > 2 and r[2] else ""))
    lines += ["", hint]
    PENDING[to] = {"opts": opts, "exp": time.time() + 900}
    await send_text(to, "\n".join(lines))


async def send_buttons(to, body, buttons):
    await send_menu(to, body, [(i, t) for i, t in buttons])


async def send_list(to, body, button, rows):
    await send_menu(to, body, rows)


async def send_document(to, data: bytes, filename: str):
    await gw("/send", {"chat": to, "type": "document", "filename": filename, "mime": "text/plain",
                       "b64": base64.b64encode(data).decode()})


def to_wa(t):
    """Markdown модели → форматирование WhatsApp."""
    t = re.sub(r"^\s*[\*\-]\s+", "• ", t, flags=re.M)
    t = re.sub(r"\*\*(.+?)\*\*", r"*\1*", t, flags=re.S)
    t = re.sub(r"^#{1,6}\s*(.+)$", r"*\1*", t, flags=re.M)
    return re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"\1 (\2)", t)


# ───────────────────────── Одноразовые веб-формы (ключи и провайдеры) ─────────────────────────
FORM: dict[str, dict] = {}

PAGE = """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer">
<title>NOVA UNIVERSE</title><style>
:root{--bg:#f5f6fa;--card:#fff;--tx:#14161f;--mut:#667;--ac:#5b5bf0;--bd:#d9dbe6}
@media(prefers-color-scheme:dark){:root{--bg:#0d0f17;--card:#171a26;--tx:#eceef7;--mut:#99a;--ac:#8a8aff;--bd:#2a2e42}}
body{margin:0;font:16px system-ui,sans-serif;background:var(--bg);color:var(--tx);display:grid;place-items:center;min-height:100vh;padding:16px;box-sizing:border-box}
main{background:var(--card);border:1px solid var(--bd);border-radius:16px;padding:24px;max-width:420px;width:100%}
h1{font-size:20px;margin:0 0 4px}p{color:var(--mut);margin:0 0 16px;font-size:14px}
label{display:block;font-size:14px;margin:12px 0 4px}
input{width:100%;box-sizing:border-box;padding:12px;border-radius:10px;border:1px solid var(--bd);background:transparent;color:var(--tx);font-size:16px}
button{margin-top:18px;width:100%;padding:12px;border:0;border-radius:10px;background:var(--ac);color:#fff;font-size:16px;font-weight:600}
.err{color:#d33;font-size:14px;margin-top:10px}.ok{font-size:18px}
</style></head><body><main>%%BODY%%</main></body></html>"""


def page(body, status=200):
    return web.Response(text=PAGE.replace("%%BODY%%", body), status=status, content_type="text/html",
                        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})


def make_token(uid, kind, pid=""):
    now = time.time()
    for k in [k for k, v in FORM.items() if v["exp"] < now]:
        FORM.pop(k, None)
    t = secrets.token_urlsafe(24)
    FORM[t] = dict(uid=uid, kind=kind, pid=pid, exp=now + 900)
    return t


def form_html(info, name, err=""):
    t = html.escape(info["t"])
    e = f'<div class="err">{html.escape(err)}</div>' if err else ""
    if info["kind"] == "key":
        return (f"<h1>🔑 API-ключ</h1><p>Провайдер: {html.escape(name)}. Ключ сохраняется в зашифрованном виде.</p>"
                f'<form method="post" action="/setup"><input type="hidden" name="t" value="{t}">'
                '<label>API-ключ</label><input type="password" name="key" required autocomplete="off">'
                f"<button>Сохранить</button></form>{e}")
    return ("<h1>➕ Свой провайдер</h1><p>Любой OpenAI-совместимый API.</p>"
            f'<form method="post" action="/setup"><input type="hidden" name="t" value="{t}">'
            '<label>Короткое имя (латиница, цифры, - и _)</label><input name="name" required maxlength="20">'
            '<label>Базовый адрес API (без /chat/completions)</label>'
            '<input name="url" required placeholder="https://api.example.com/v1">'
            '<label>Модель по умолчанию (можно пусто)</label><input name="model">'
            '<label>API-ключ (если не нужен — пусто)</label><input type="password" name="key" autocomplete="off">'
            f"<button>Добавить</button></form>{e}")


async def setup_get(req):
    t = req.query.get("t", "")
    info = FORM.get(t)
    if not info or info["exp"] < time.time():
        return page("<h1>Ссылка устарела</h1><p>Запросите новую командой /key или /addprovider в WhatsApp.</p>", 410)
    name = await resolve_name(info["uid"], info["pid"]) if info["pid"] else ""
    return page(form_html({**info, "t": t}, name))


async def setup_post(req):
    d = await req.post()
    t = d.get("t", "")
    info = FORM.get(t)
    if not info or info["exp"] < time.time():
        return page("<h1>Ссылка устарела</h1><p>Запросите новую в WhatsApp.</p>", 410)
    uid, key = info["uid"], (d.get("key") or "").strip()
    try:
        if info["kind"] == "key":
            if not key:
                raise ValueError("Введите ключ.")
            await set_key(uid, info["pid"], key)
            msg = "Ключ сохранён."
        else:
            name = (d.get("name") or "").strip()
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,20}", name):
                raise ValueError("Имя: латиница, цифры, - и _, до 20 символов.")
            if name in BUILTIN:
                raise ValueError("Такое имя занято встроенным провайдером.")
            url = await check_url(d.get("url") or "")
            model = (d.get("model") or "").strip()[:200]
            await run("INSERT OR REPLACE INTO custom_providers VALUES(?,?,?,?)", (uid, name, url, model))
            if key:
                await set_key(uid, f"c:{name}", key)
            await upd(uid, provider=f"c:{name}", model=model)
            msg = f"Провайдер «{name}» добавлен и выбран."
    except ValueError as e:
        return page(form_html({**info, "t": t}, "", str(e)), 400)
    FORM.pop(t, None)
    await send_text(CHATS.get(uid, f"{uid}@s.whatsapp.net"), f"✅ {msg}" + (" Теперь выберите модель: /models" if info["kind"] == "add" else ""))
    return page(f'<h1 class="ok">✅ Готово</h1><p>{html.escape(msg)} Можно вернуться в WhatsApp.</p>')


async def send_key_link(to, uid, pid):
    if not PUBLIC_URL:
        return await send_text(to, "⚠️ Не задан PUBLIC_URL, не могу выдать ссылку. Запасной вариант: /key <провайдер> <ключ>")
    name = await resolve_name(uid, pid)
    url = f"{PUBLIC_URL}/setup?t={make_token(uid, 'key', pid)}"
    await send_text(to, f"🔑 Ключ для {name}. Откройте ссылку (одноразовая, 15 минут):\n{url}\n\n"
                        "Ключ вводится на странице, а не в чате, поэтому в переписке не остаётся.")


async def send_add_link(to, uid):
    if not PUBLIC_URL:
        return await send_text(to, "⚠️ Не задан PUBLIC_URL, форма недоступна.")
    url = f"{PUBLIC_URL}/setup?t={make_token(uid, 'add')}"
    await send_text(to, f"➕ Свой провайдер: откройте форму (одноразовая, 15 минут):\n{url}")


# ───────────────────────── Экраны ─────────────────────────
HELP = ("🌌 NOVA UNIVERSE — ИИ-ассистент в WhatsApp\n\n"
        "Просто пишите сообщения. Можно присылать фото, стикеры и текстовые файлы (для картинок нужна модель с 👁). "
        "Меню приходят пронумерованным списком: ответьте цифрой.\n\n"
        "*Основное*\n/menu — меню\n/models — выбрать модель (/models текст — поиск, /models vision — только vision)\n"
        "/model <id> — задать модель вручную\n/providers — провайдеры и ключи\n/key — ссылка для ввода ключа\n"
        "/addprovider — добавить свой провайдер\n/mode — режим\n/status — состояние\n\n"
        "*Сессии* (у каждой своя история)\n/sessions, /new [название], /session, /rename <название>, "
        "/history, /export, /clear\n\n"
        "*Память и навыки*\n/memory, /remember <факт>, /forget <№>\n"
        "/skills, /addskill <название> | <инструкция>, /delskill <№>\n\n"
        "*Настройки*\n/settings, /temp 0.7, /system <текст> (- сбросить), /vision")

MC: dict[int, dict] = {}
PS = 8
VISION_TXT = {True: "👁 да, видит изображения", False: "нет, только текст", None: "неизвестно, попробуйте прислать картинку"}


async def send_main(to, uid):
    u = await get_user(uid)
    sid = await active_session(uid)
    title = (await own_session(uid, sid))["title"]
    body = (f"🌌 NOVA UNIVERSE\nПровайдер: {await resolve_name(uid, u['provider'])}\n"
            f"Модель: {u['model'] or '— не выбрана'}\nРежим: {MODES[u['mode']][0]}\nСессия: {title}")
    rows = [("m:model", "🤖 Модель"), ("m:prov", "🔑 Провайдеры и ключи"), ("m:sess", "🗂 Сессии"),
            ("m:mode", "🎭 Режим"), ("m:mem", "🧠 Память"), ("m:skills", "🧩 Навыки"),
            ("m:set", "⚙️ Настройки"), ("m:status", "📊 Статус"), ("m:new", "🆕 Новая сессия")]
    await send_list(to, body, "Меню", rows)


def apply_filter(c):
    q = c.get("q", "")
    c["flt"] = [x for x in c["all"] if q in x.lower() and (not c.get("vf") or x in c["vis"])]
    c["page"] = 0


async def load_models(to, uid, force=False):
    u = await get_user(uid)
    c = MC.get(uid)
    if c and not force and c.get("pid") == u["provider"]:
        return c
    try:
        prov = await resolve(uid, u["provider"])
        if not prov:
            raise ProviderError("провайдер не найден")
        ids, vis, meta = await fetch_models(prov["base"], prov["key"])
        if not ids:
            raise ProviderError("пустой список")
        if u["provider"] == "openrouter":  # скрытый роутер показываем под именем NOVA-Spark-5
            ids = [SPARK_NAME] + [x for x in ids if x != SPARK_ROUTER]
            vis, meta = set(vis) | {SPARK_NAME}, True
    except Exception as e:
        await send_text(to, f"⚠️ Не удалось получить список моделей ({e}).\n"
                            "Задайте модель вручную: /model <id> или проверьте ключ: /providers")
        return None
    c = dict(all=ids, flt=ids, q="", page=0, vis=vis, meta=meta, vf=False, pid=u["provider"])
    MC[uid] = c
    return c


async def models_cmd(to, uid, arg=""):
    c = await load_models(to, uid, force=not arg)
    if not c:
        return
    a = arg.strip().lower()
    if a == "reset":
        c["q"], c["vf"] = "", False
    elif a in ("vision", "vis"):
        if not c["meta"]:
            await send_text(to, "У этого провайдера нет данных о vision-моделях. Попробуйте прислать картинку.")
        c["vf"] = not c["vf"]
    elif a:
        c["q"] = a
    apply_filter(c)
    await show_models(to, uid, 0)


async def show_models(to, uid, page=0):
    c, u = MC.get(uid), await get_user(uid)
    if not c:
        return await models_cmd(to, uid, "")
    flt = c["flt"]
    pages = max(1, math.ceil(len(flt) / PS))
    page = max(0, min(page, pages - 1))
    c["page"] = page
    rows = []
    for i, mid in enumerate(flt[page * PS:(page + 1) * PS]):
        mark = ("✅ " if mid == u["model"] else "") + ("👁 " if mid in c["vis"] else "")
        rows.append((f"mm:{page * PS + i}", mark + mid.split("/")[-1], mid))
    if not rows:
        return await send_text(to, "Ничего не найдено. /models reset — сбросить фильтр.")
    if page > 0:
        rows.append((f"mp:{page - 1}", "⬅️ Назад"))
    if page < pages - 1:
        rows.append((f"mp:{page + 1}", "➡️ Дальше"))
    flt_txt = (f"\nФильтр: {c['q']}" if c["q"] else "") + ("\nТолько vision-модели" if c["vf"] else "")
    legend = "\n👁 — понимает изображения" if c["meta"] else ""
    await send_list(to, f"🤖 Модели: {len(flt)} шт., стр. {page + 1}/{pages}\nТекущая: {u['model'] or '—'}"
                       f"{flt_txt}{legend}\nПоиск: /models текст", "Выбрать модель", rows)


async def set_model(to, uid, model):
    await upd(uid, model=model[:200])
    u = await get_user(uid)
    prov = None
    try:
        prov = await resolve(uid, u["provider"])
    except ValueError:
        pass
    v = await vision_status(prov, model) if prov else None
    await send_text(to, f"✅ Модель: {model}\nПонимает изображения: {VISION_TXT[v]}")


async def show_providers(to, uid, page=0):
    u = await get_user(uid)
    provs = await all_providers(uid)
    pages = max(1, math.ceil(len(provs) / PS))
    page = max(0, min(page, pages - 1))
    rows = []
    for pid, name, base in provs[page * PS:(page + 1) * PS]:
        has = bool(await get_key(uid, pid)) or (pid == "openrouter" and DEFAULT_OR_KEY)
        rows.append((f"p:{pid}", ("▶️ " if pid == u["provider"] else "🔑 " if has else "") + name, base))
    if page > 0:
        rows.append((f"pp:{page - 1}", "⬅️ Назад"))
    if page < pages - 1:
        rows.append((f"pp:{page + 1}", "➡️ Дальше"))
    await send_list(to, "🔑 Провайдеры\n▶️ активный · 🔑 есть ключ\nСвой провайдер: /addprovider", "Провайдеры", rows)


async def provider_card(to, uid, pid):
    provs = {p[0]: p for p in await all_providers(uid)}
    if pid not in provs:
        return await show_providers(to, uid)
    _, name, base = provs[pid]
    u = await get_user(uid)
    own = await get_key(uid, pid)
    ks = ("ваш ключ сохранён" if own else "используется общий ключ бота" if pid == "openrouter" and DEFAULT_OR_KEY
          else "нет, вставьте ключ")
    buttons = [(f"k:{pid}", "🔑 Вставить ключ"), (f"use:{pid}", "✅ Использовать")]
    if own:
        buttons.append((f"kd:{pid}", "🗑 Удалить ключ"))
    elif pid.startswith("c:"):
        buttons.append((f"pdel:{pid[2:]}", "❌ Убрать"))
    await send_buttons(to, f"{name}\n{base}\nКлюч: {ks}\nАктивный: {'да' if u['provider'] == pid else 'нет'}", buttons)


async def use_provider(to, uid, pid):
    try:
        prov = await resolve(uid, pid)
    except ValueError as e:
        return await send_text(to, f"⚠️ {e}")
    if not prov:
        return await send_text(to, "Провайдер не найден.")
    u = await get_user(uid)
    if u["provider"] != pid:
        model = PROVIDER_DEFAULT_MODEL.get(pid, "")
        if pid.startswith("c:"):
            rows = await fetch("SELECT model FROM custom_providers WHERE user_id=? AND name=?", (uid, pid[2:]))
            model = (rows[0]["model"] or "") if rows else ""
        await upd(uid, provider=pid, model=model)
    if not prov["key"] and not prov["custom"]:
        return await send_buttons(to, f"Провайдер {prov['name']} выбран, но нужен ключ.", [(f"k:{pid}", "🔑 Вставить ключ")])
    await models_cmd(to, uid, "")


async def send_modes(to, uid):
    u = await get_user(uid)
    await send_list(to, "🎭 Режим ассистента", "Режимы",
                    [(f"mode:{k}", ("✅ " if k == u["mode"] else "") + v[0]) for k, v in MODES.items()])


async def send_settings(to, uid):
    u = await get_user(uid)
    await send_buttons(to, f"⚙️ Настройки\nTemperature: {u['temperature']}\n"
                           f"Свой системный промпт: {'задан' if u['sys'] else 'нет'}\n\n"
                           "/temp 0.5 · /system <текст> · /vision",
                       [("t:0.3", "Точнее 0.3"), ("t:0.7", "Обычно 0.7"), ("t:1.1", "Творчески 1.1")])


async def send_sessions(to, uid):
    cur = await active_session(uid)
    rows = []
    for r in await fetch("SELECT id, title FROM sessions WHERE user_id=? ORDER BY updated DESC LIMIT 9", (uid,)):
        n = await msg_count(r["id"])
        rows.append((f"ss:{r['id']}", ("▶️ " if r["id"] == cur else "") + r["title"], f"{n // 2} обменов"))
    rows.append(("ssnew", "➕ Новая сессия"))
    await send_list(to, "🗂 Сессии. У каждой своя история: бот помнит всё, что вы писали в ней.", "Сессии", rows)


async def session_card(to, uid, sid):
    r = await own_session(uid, sid)
    if not r:
        return await send_sessions(to, uid)
    cur = await active_session(uid)
    created = time.strftime("%d.%m.%Y %H:%M", time.localtime(r["created"]))
    buttons = [(f"ssuse:{sid}", "▶️ Открыть")] if sid != cur else []
    buttons += [(f"ssexp:{sid}", "📤 Экспорт"), (f"ssdel:{sid}", "🗑 Удалить")]
    await send_buttons(to, f"🗂 {r['title']}\nСообщений: {await msg_count(sid)}\nСоздана: {created}\n"
                           + ("▶️ активная" if sid == cur else "не активна") + "\nПереименовать: /rename <название>", buttons)


async def export_session(to, uid, sid):
    if not await own_session(uid, sid):
        return
    rows = await fetch("SELECT role, content FROM history WHERE session_id=? ORDER BY id", (sid,))
    text = "\n\n".join(("Вы: " if r["role"] == "user" else "NOVA: ") + r["content"] for r in rows) or "(пусто)"
    await send_document(to, text.encode("utf-8"), f"session_{sid}.txt")


async def send_memory(to, uid):
    rows = await fetch("SELECT text FROM memory WHERE user_id=? ORDER BY id", (uid,))
    if not rows:
        return await send_text(to, "🧠 Память пуста. Добавить: /remember Я живу в Алматы")
    await send_text(to, "🧠 Память (передаётся модели в каждом чате):\n" +
                    "\n".join(f"{i}. {r['text']}" for i, r in enumerate(rows, 1)) + "\n\nУдалить: /forget <№>")


async def send_skills(to, uid):
    rows = await fetch("SELECT name, text FROM skills WHERE user_id=? ORDER BY id", (uid,))
    if not rows:
        return await send_text(to, "🧩 Навыков нет. Добавить: /addskill Проверка эссе | Проверяй текст по критериям...")
    await send_text(to, "🧩 Навыки (ИИ применяет, когда запрос подходит):\n" +
                    "\n".join(f"{i}. {r['name']} ({len(r['text'])} симв.)" for i, r in enumerate(rows, 1)) +
                    "\n\nУдалить: /delskill <№>")


async def send_status(to, uid):
    u = await get_user(uid)
    sid = await active_session(uid)
    title = (await own_session(uid, sid))["title"]
    try:
        prov = await resolve(uid, u["provider"])
    except ValueError:
        prov = None
    v = await vision_status(prov, u["model"]) if prov and u["model"] else None
    mem = (await fetch("SELECT COUNT(*) c FROM memory WHERE user_id=?", (uid,)))[0]["c"]
    sk = (await fetch("SELECT COUNT(*) c FROM skills WHERE user_id=?", (uid,)))[0]["c"]
    await send_text(to, f"📊 Состояние\n\nПровайдер: {await resolve_name(uid, u['provider'])} "
                        f"(ключ: {'есть' if prov and prov['key'] else 'нет'})\nМодель: {u['model'] or '—'}\n"
                        f"Vision: {VISION_TXT[v]}\nРежим: {MODES[u['mode']][0]}\nTemperature: {u['temperature']}\n"
                        f"Сессия: {title} ({await msg_count(sid)} сообщ.)\nФактов в памяти: {mem}, навыков: {sk}")


async def send_history(to, uid):
    sid = await active_session(uid)
    rows = await fetch("SELECT role, content FROM history WHERE session_id=? ORDER BY id DESC LIMIT 6", (sid,))
    if not rows:
        return await send_text(to, "В этой сессии пока пусто.")
    await send_text(to, "Последние сообщения сессии:\n\n" + "\n\n".join(
        ("👤 " if r["role"] == "user" else "🤖 ") + r["content"][:200].replace("\n", " ") for r in reversed(rows)))


# ───────────────────────── Команды и кнопки ─────────────────────────
async def command(to, uid, text):
    parts = text.split(None, 1)
    cmd = parts[0][1:].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    if cmd in ("start", "menu"):
        return await send_main(to, uid)
    if cmd == "help":
        return await send_text(to, HELP)
    if cmd == "models":
        return await models_cmd(to, uid, arg)
    if cmd == "model":
        return await set_model(to, uid, arg) if arg else await models_cmd(to, uid, "")
    if cmd in ("providers", "provider"):
        if arg:
            pid = arg if (arg in BUILTIN or arg.startswith("c:")) else f"c:{arg}"
            return await use_provider(to, uid, pid)
        return await show_providers(to, uid)
    if cmd == "key":
        u = await get_user(uid)
        bits = arg.split(None, 1)
        if len(bits) == 2:
            pid = bits[0] if (bits[0] in BUILTIN or bits[0].startswith("c:")) else f"c:{bits[0]}"
            if bits[1] == "-":
                await run("DELETE FROM keys WHERE user_id=? AND provider=?", (uid, pid))
                return await send_text(to, "🗑 Ключ удалён.")
            await set_key(uid, pid, bits[1].strip())
            return await send_text(to, "✅ Ключ сохранён. ⚠️ Удалите своё сообщение с ключом из чата "
                                       "(в WhatsApp бот не может сделать это за вас). В следующий раз используйте /key без параметров.")
        return await send_key_link(to, uid, u["provider"])
    if cmd == "addprovider":
        return await send_add_link(to, uid)
    if cmd == "delprovider":
        await run("DELETE FROM custom_providers WHERE user_id=? AND name=?", (uid, arg))
        await run("DELETE FROM keys WHERE user_id=? AND provider=?", (uid, f"c:{arg}"))
        u = await get_user(uid)
        if u["provider"] == f"c:{arg}":
            await upd(uid, provider="openrouter", model=DEFAULT_MODEL)
        return await send_text(to, "Провайдер удалён.")
    if cmd == "mode":
        if arg in MODES:
            await upd(uid, mode=arg)
            return await send_text(to, f"✅ Режим: {MODES[arg][0]}")
        return await send_modes(to, uid)
    if cmd == "sessions":
        return await send_sessions(to, uid)
    if cmd == "session":
        return await session_card(to, uid, await active_session(uid))
    if cmd in ("new", "newsession"):
        await new_session(uid, arg or "Новый чат")
        return await send_text(to, "🆕 Новая сессия создана. Прошлые сохранены: /sessions")
    if cmd == "rename":
        if not arg:
            return await send_text(to, "Использование: /rename Новое название")
        await run("UPDATE sessions SET title=? WHERE id=?", (arg[:60], await active_session(uid)))
        return await send_text(to, "✏️ Сессия переименована.")
    if cmd == "clear":
        await run("DELETE FROM history WHERE session_id=?", (await active_session(uid),))
        return await send_text(to, "🧹 История текущей сессии очищена.")
    if cmd == "export":
        return await export_session(to, uid, await active_session(uid))
    if cmd == "history":
        return await send_history(to, uid)
    if cmd == "memory":
        return await send_memory(to, uid)
    if cmd == "remember":
        if not arg:
            return await send_text(to, "Использование: /remember Я живу в Алматы")
        await run("INSERT INTO memory(user_id,text) VALUES(?,?)", (uid, arg[:500]))
        return await send_text(to, "🧠 Запомнил.")
    if cmd == "forget":
        rows = await fetch("SELECT id FROM memory WHERE user_id=? ORDER BY id", (uid,))
        if arg.isdigit() and 1 <= int(arg) <= len(rows):
            await run("DELETE FROM memory WHERE id=?", (rows[int(arg) - 1]["id"],))
            return await send_text(to, "🗑 Удалено.")
        return await send_text(to, "Использование: /forget <номер из /memory>")
    if cmd == "skills":
        return await send_skills(to, uid)
    if cmd == "addskill":
        name, sep, body = arg.partition("|")
        if not sep or not name.strip() or not body.strip():
            return await send_text(to, "Использование: /addskill Название | Инструкция")
        await run("INSERT INTO skills(user_id,name,text) VALUES(?,?,?)", (uid, name.strip()[:60], body.strip()[:8000]))
        return await send_text(to, "🧩 Навык добавлен.")
    if cmd == "delskill":
        rows = await fetch("SELECT id FROM skills WHERE user_id=? ORDER BY id", (uid,))
        if arg.isdigit() and 1 <= int(arg) <= len(rows):
            await run("DELETE FROM skills WHERE id=?", (rows[int(arg) - 1]["id"],))
            return await send_text(to, "🗑 Навык удалён.")
        return await send_text(to, "Использование: /delskill <номер из /skills>")
    if cmd == "settings":
        return await send_settings(to, uid)
    if cmd == "temp":
        try:
            t = float(arg.replace(",", "."))
            assert 0 <= t <= 2
        except Exception:
            return await send_text(to, "Использование: /temp 0.7 (число от 0 до 2)")
        await upd(uid, temperature=t)
        return await send_text(to, f"Temperature: {t}")
    if cmd == "system":
        if not arg:
            cur = (await get_user(uid))["sys"]
            return await send_text(to, f"Текущий промпт: {cur or 'не задан'}\n\n/system <текст> — задать, /system - — сбросить")
        await upd(uid, sys="" if arg == "-" else arg[:4000])
        return await send_text(to, "✅ Системный промпт обновлён.")
    if cmd == "vision":
        u = await get_user(uid)
        try:
            prov = await resolve(uid, u["provider"])
        except ValueError:
            prov = None
        v = await vision_status(prov, u["model"]) if prov and u["model"] else None
        return await send_text(to, f"Модель: {u['model'] or '—'}\nПонимает изображения: {VISION_TXT[v]}\n\n"
                                   "Vision-модели помечены 👁 в /models (или /models vision).")
    if cmd == "status":
        return await send_status(to, uid)
    if cmd == "cancel":
        return await send_text(to, "Ок.")
    await send_text(to, "Неизвестная команда. Список: /help")


async def action(to, uid, aid):
    k, _, v = aid.partition(":")
    if k == "m":
        if v == "main":
            return await send_main(to, uid)
        if v == "model":
            return await models_cmd(to, uid, "")
        if v == "prov":
            return await show_providers(to, uid)
        if v == "sess":
            return await send_sessions(to, uid)
        if v == "mode":
            return await send_modes(to, uid)
        if v == "mem":
            return await send_memory(to, uid)
        if v == "skills":
            return await send_skills(to, uid)
        if v == "set":
            return await send_settings(to, uid)
        if v == "status":
            return await send_status(to, uid)
        if v == "new":
            await new_session(uid)
            return await send_text(to, "🆕 Новая сессия создана. Прошлые сохранены: /sessions")
    elif k == "mm":
        c = MC.get(uid)
        try:
            return await set_model(to, uid, c["flt"][int(v)])
        except (TypeError, KeyError, IndexError, ValueError):
            return await send_text(to, "Список устарел, откройте /models заново.")
    elif k == "mp":
        return await show_models(to, uid, int(v))
    elif k == "pp":
        return await show_providers(to, uid, int(v))
    elif k == "p":
        return await provider_card(to, uid, v)
    elif k == "k":
        return await send_key_link(to, uid, v)
    elif k == "kd":
        await run("DELETE FROM keys WHERE user_id=? AND provider=?", (uid, v))
        return await send_text(to, "🗑 Ключ удалён.")
    elif k == "use":
        return await use_provider(to, uid, v)
    elif k == "pdel":
        return await command(to, uid, f"/delprovider {v}")
    elif k == "mode" and v in MODES:
        await upd(uid, mode=v)
        return await send_text(to, f"✅ Режим: {MODES[v][0]}")
    elif k == "t":
        await upd(uid, temperature=float(v))
        return await send_text(to, f"Temperature: {v}")
    elif k == "ss":
        return await session_card(to, uid, int(v))
    elif k == "ssnew":
        await new_session(uid)
        return await send_text(to, "🆕 Новая сессия создана.")
    elif k == "ssuse":
        if await own_session(uid, int(v)):
            await upd(uid, session_id=int(v))
            return await send_text(to, "▶️ Сессия переключена.")
    elif k == "ssexp":
        return await export_session(to, uid, int(v))
    elif k == "ssdel":
        if await own_session(uid, int(v)):
            await delete_session(uid, int(v))
            return await send_text(to, "🗑 Сессия удалена.")


# ───────────────────────── Чат ─────────────────────────
async def build_system(uid, name, u, sid, vision):
    title = (await own_session(uid, sid))["title"]
    s = (PERSONA + "\n\n" if PERSONA else "") + MODE_NOTES.get(u["mode"], "") + WA_STYLE
    if name:
        s += f"\nПользователя зовут {name}."
    s += (f"\n\nТы ведёшь непрерывный диалог в сессии «{title}». Предыдущие сообщения этой сессии переданы тебе выше: "
          "опирайся на них и не говори, что не помнишь прошлые сообщения.")
    if vision is not False:
        s += " Если к сообщению приложено изображение или стикер, ты можешь его видеть и описывать."
    if u["sys"]:
        s += "\n\nДополнительные инструкции пользователя:\n" + u["sys"]
    mem = await fetch("SELECT text FROM memory WHERE user_id=? ORDER BY id", (uid,))
    if mem:
        s += "\n\nФакты о пользователе:\n" + "\n".join(f"- {r['text']}" for r in mem)
    sk = await fetch("SELECT name, text FROM skills WHERE user_id=? ORDER BY id", (uid,))
    if sk:
        s += "\n\nНавыки пользователя (применяй, когда запрос подходит):\n" + \
             "\n\n".join(f"### {r['name']}\n{r['text']}" for r in sk)[:8000]
    return s


async def media_image(b64):
    try:
        return await asyncio.to_thread(lambda: prep_image(base64.b64decode(b64)))
    except Exception:
        raise ValueError("Не удалось обработать изображение.")


async def build_user(msg, text, vision):
    """→ (content для модели, текст для истории, base64-картинка или None)"""
    t, cap, b64 = msg.get("type"), (msg.get("caption") or "").strip(), msg.get("media_b64")
    no_vision = ("Выбранная модель не умеет смотреть изображения. Выберите модель с пометкой 👁 "
                 "в /models (проверить: /vision).")
    if t == "toolarge":
        raise ValueError("Файл слишком большой (максимум 15 МБ).")
    if t == "sticker":
        label, img = "[Стикер]", None
        if vision is not False and b64:
            try:
                img = await media_image(b64)
            except ValueError:
                img = None
        prompt = (f"{label}\nПользователь прислал стикер (картинка приложена). Посмотри на него и отреагируй по смыслу."
                  if img else f"{label}\nПользователь прислал стикер, но картинка недоступна.")
        return image_content(prompt, img), label, img
    if t == "image" or (t == "document" and (msg.get("mime") or "").startswith("image/")):
        if vision is False:
            raise ValueError(no_vision)
        if not b64:
            raise ValueError("Не удалось получить изображение.")
        img = await media_image(b64)
        return image_content(cap or "Опиши изображение.", img), f"[изображение] {cap}".strip(), img
    if t == "document":
        if not b64:
            raise ValueError("Поддерживаются текстовые файлы в UTF-8 (txt, md, py, json, csv…) и изображения.")
        data = base64.b64decode(b64)
        if len(data) > 1_000_000:
            raise ValueError("Файл больше 1 МБ.")
        try:
            body = data.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("Нужен текстовый файл в UTF-8.")
        full = f"Файл «{msg.get('filename') or 'file'}»:\n```\n{body[:30000]}\n```\n\n{cap}"
        return full, full[:12000], None
    return text, text, None


async def complete(base, key, model, messages, temperature):
    payload = {"model": model, "messages": messages, "temperature": temperature}
    async with session.post(base + "/chat/completions", json=payload, headers=auth_headers(base, key),
                            allow_redirects=False) as r:
        raw = await r.text()
        if r.status != 200:
            if key:
                raw = raw.replace(key, "***")
            raise ProviderError(f"{r.status}: {raw[:500]}", r.status)
    try:
        d = json.loads(raw)
    except ValueError:
        raise ProviderError("провайдер вернул не JSON")
    if d.get("error"):
        raise ProviderError(str(d["error"])[:500])
    c = ((d.get("choices") or [{}])[0].get("message") or {}).get("content")
    if isinstance(c, list):
        c = "".join(p.get("text", "") for p in c if isinstance(p, dict))
    return c or ""


async def chat(to, uid, msg, name, text):
    u = await get_user(uid)
    spark = u["model"] == SPARK_NAME      # для NOVA-Spark-5 любые сбои скрыты за сообщением о лимите
    if not msg.get("self") and not rate_ok(uid):
        return await send_text(to, LIMIT_MSG if spark else "⚠️ Слишком много сообщений, подождите немного.")
    try:
        prov = await resolve(uid, u["provider"])
    except ValueError as e:
        return await send_text(to, f"⚠️ {e}")
    if not prov:
        return await send_text(to, "Провайдер не найден. Откройте /providers")
    if spark and prov["id"] != "openrouter":
        return await send_text(to, "NOVA-Spark-5 работает только через OpenRouter: /providers")
    if not prov["key"] and not prov["custom"]:
        if spark:
            log.warning("нет ключа OpenRouter для NOVA-Spark-5 (задайте DEFAULT_OPENROUTER_KEY)")
            return await send_text(to, LIMIT_MSG)
        return await send_buttons(to, f"Для {prov['name']} нужен API-ключ.", [(f"k:{prov['id']}", "🔑 Вставить ключ")])
    if not u["model"]:
        return await send_buttons(to, "Сначала выберите модель.", [("m:model", "🤖 Выбрать модель")])
    model = SPARK_ROUTER if spark else u["model"]
    vision = await vision_status(prov, u["model"])
    try:
        user_content, user_text, img = await build_user(msg, text, vision)
    except ValueError as e:
        return await send_text(to, f"⚠️ {e}")

    sid = await active_session(uid)
    messages = ([{"role": "system", "content": await build_system(uid, name, u, sid, vision)}]
                + await load_history(sid, vision) + [{"role": "user", "content": user_content}])
    try:
        answer = await complete(prov["base"], prov["key"], model, messages, u["temperature"])
    except ProviderError as e:
        log.warning("ошибка провайдера (%s): %s", u["model"], e)
        if spark:
            return await send_text(to, LIMIT_MSG)
        hint = ("\n\nВозможно, модель не принимает изображения. Выберите модель с пометкой 👁 в /models."
                if img and e.status in (400, 404, 413, 415, 422) else "")
        return await send_text(to, f"⚠️ Ошибка провайдера {prov['name']}:\n{e}{hint}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("сетевая ошибка (%s): %s", u["model"], type(e).__name__)
        return await send_text(to, LIMIT_MSG if spark else f"⚠️ Сетевая ошибка: {type(e).__name__}")
    if not answer.strip():
        return await send_text(to, LIMIT_MSG if spark else "⚠️ Пустой ответ от модели. Попробуйте другую модель: /models")
    await send_text(to, to_wa(answer))

    if img:
        await run("UPDATE history SET img=NULL WHERE session_id=?", (sid,))
    await run("INSERT INTO history(user_id,session_id,role,content,img) VALUES(?,?,?,?,?)",
              (uid, sid, "user", user_text[:12000], img))
    await run("INSERT INTO history(user_id,session_id,role,content) VALUES(?,?,?,?)",
              (uid, sid, "assistant", answer[:12000]))
    title = (await own_session(uid, sid))["title"]
    if title == "Новый чат" and user_text.strip():
        title = re.sub(r"\s+", " ", user_text).strip()[:40]
    await run("UPDATE sessions SET title=?, updated=? WHERE id=?", (title, time.time(), sid))
    await run("DELETE FROM history WHERE session_id=? AND id NOT IN "
              "(SELECT id FROM history WHERE session_id=? ORDER BY id DESC LIMIT ?)", (sid, sid, SESSION_CAP))


# ───────────────────────── Входящие от шлюза ─────────────────────────
SEEN: "OrderedDict[str, int]" = OrderedDict()
LOCKS: dict[int, asyncio.Lock] = {}
TASKS: set = set()


async def handle(to, uid, msg, name):
    await get_user(uid)
    t = msg.get("type")
    if t == "text":
        text = (msg.get("text") or "").strip()
        if text.startswith("/"):
            PENDING.pop(to, None)
            return await command(to, uid, text)
        p = PENDING.get(to)
        if text.isdigit() and len(text) <= 2 and p and p["exp"] > time.time() and text in p["opts"]:
            return await action(to, uid, p["opts"][text])
        return await chat(to, uid, msg, name, text)
    if t in ("image", "sticker", "document", "toolarge"):
        return await chat(to, uid, msg, name, "")
    if t in ("audio", "video"):
        return await send_text(to, "Голосовые и видео пока не поддерживаются. Напишите текстом или пришлите картинку.")


async def process(msg):
    digits = re.sub(r"\D", "", str(msg.get("from", "")))
    to = msg.get("chat", "")
    if not digits or not to or not msg.get("allowed"):
        return
    mid = msg.get("id", "")
    if mid in SEEN:
        return
    SEEN[mid] = 1
    while len(SEEN) > 2000:
        SEEN.popitem(last=False)
    uid = int(digits)
    CHATS[uid] = to
    async with LOCKS.setdefault(uid, asyncio.Lock()):
        try:
            await handle(to, uid, msg, msg.get("name", ""))
        except Exception:
            log.exception("handler error")
            await send_text(to, "⚠️ Внутренняя ошибка. Попробуйте ещё раз.")


async def incoming(req):
    if not INTERNAL_KEY or not hmac.compare_digest(req.headers.get("X-Internal", ""), INTERNAL_KEY):
        return web.Response(status=403, text="forbidden")
    try:
        msg = await req.json()
    except ValueError:
        return web.Response(status=400)
    task = asyncio.create_task(process(msg))
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)
    return web.Response(text="ok")  # быстрый ответ, обработка идёт в фоне


async def pair_proxy(req):
    """Код сопряжения и статус подключения (для ноутбука/браузера), защищено PAIR_KEY."""
    if not PAIR_KEY or not hmac.compare_digest(req.query.get("key", ""), PAIR_KEY):
        return web.Response(status=403, text="forbidden")
    path = "/status" if req.path == "/status" else "/pair"
    try:
        async with session.get(f"{GATEWAY_URL}{path}", headers={"X-Internal": INTERNAL_KEY},
                               timeout=aiohttp.ClientTimeout(total=40)) as r:
            return web.Response(text=await r.text(), status=r.status, content_type="application/json")
    except Exception as e:
        return web.Response(text=json.dumps({"status": "gateway_down", "error": type(e).__name__}),
                            status=502, content_type="application/json")


async def health(_):
    return web.Response(text="NOVA UNIVERSE WhatsApp bot: ok")


async def start_web():
    app = web.Application(client_max_size=5 * 1024 * 1024)
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post("/incoming", incoming)
    app.router.add_get("/pair", pair_proxy)
    app.router.add_get("/status", pair_proxy)
    app.router.add_get("/setup", setup_get)
    app.router.add_post("/setup", setup_post)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("сервер на порту %s", port)
    return runner


async def self_ping():
    """Будильник №1: бот сам стучится на свой публичный адрес, пока не спит."""
    url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        return
    while True:
        await asyncio.sleep(240)
        try:
            async with session.get(url + "/health", timeout=aiohttp.ClientTimeout(total=30)) as r:
                await r.read()
        except Exception as e:
            log.warning("self-ping: %s", type(e).__name__)


async def main():
    global session
    if not INTERNAL_KEY:
        raise SystemExit("INTERNAL_KEY не задан (его выдаёт start.sh)")
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600, sock_read=180))
    backup = Backup(DB_PATH, fernet, session)
    if backup.enabled and await backup.restore():
        log.info("БД восстановлена из резервной копии")
    await init_db()
    runner = await start_web()
    tasks = [asyncio.create_task(self_ping())]
    if backup.enabled:
        tasks.append(asyncio.create_task(backup.loop()))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sg in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sg, stop.set)
        except NotImplementedError:
            pass
    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        if backup.enabled:  # финальный снимок при остановке (Render шлёт SIGTERM)
            try:
                await asyncio.wait_for(backup.backup(force=True), 25)
            except Exception:
                log.exception("final backup failed")
        await runner.cleanup()
        await session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
