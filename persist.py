"""Резервная копия SQLite для бесплатного Render (там диск стирается при каждом рестарте).

Раз в BACKUP_INTERVAL секунд (если база изменилась) бот кладёт зашифрованный снимок БД
в релиз приватного GitHub-репозитория BACKUP_REPO, а при старте восстанавливает его.
Снимок шифруется ENCRYPTION_KEY, поэтому в GitHub лежит только шифртекст.
"""
import asyncio
import gzip
import hashlib
import logging
import os
import sqlite3
import tempfile
import time

import aiohttp

log = logging.getLogger("persist")
API = "https://api.github.com"
PREFIX = "nova-"  # Python-бэкап БД; Node-шлюз хранит свою сессию под префиксом auth-


class Backup:
    def __init__(self, db_path, fernet, session):
        self.db_path, self.f, self.session = db_path, fernet, session
        self.token = os.getenv("BACKUP_GITHUB_TOKEN", "").strip()
        self.repo = os.getenv("BACKUP_REPO", "").strip()
        self.interval = max(60, int(os.getenv("BACKUP_INTERVAL", "300")))
        self.enabled = bool(self.token and self.repo)
        self.last_hash = None

    def _h(self, accept="application/vnd.github+json"):
        return {"Authorization": f"Bearer {self.token}", "Accept": accept,
                "User-Agent": "nova-bot", "X-GitHub-Api-Version": "2022-11-28"}

    async def _release_id(self, create):
        url = f"{API}/repos/{self.repo}/releases/tags/backup"
        async with self.session.get(url, headers=self._h()) as r:
            if r.status == 200:
                return (await r.json())["id"]
        if not create:
            return None
        body = {"tag_name": "backup", "name": "NOVA backup", "prerelease": True,
                "body": "Зашифрованные снимки базы бота. Не удаляйте."}
        async with self.session.post(f"{API}/repos/{self.repo}/releases", json=body, headers=self._h()) as r:
            if r.status == 201:
                return (await r.json())["id"]
            log.warning("не удалось создать релиз бэкапа: HTTP %s", r.status)
            return None

    async def _assets(self, rid):
        async with self.session.get(f"{API}/repos/{self.repo}/releases/{rid}/assets?per_page=100",
                                    headers=self._h()) as r:
            return await r.json() if r.status == 200 else []

    async def restore(self):
        """Восстановить БД, если локального файла нет. True — восстановлено."""
        if not self.enabled or os.path.exists(self.db_path):
            return False
        try:
            rid = await self._release_id(create=False)
            assets = sorted([a for a in await self._assets(rid) if a["name"].startswith(PREFIX)],
                            key=lambda a: a["name"], reverse=True) if rid else []
            if not assets:
                return False
            url = f"{API}/repos/{self.repo}/releases/assets/{assets[0]['id']}"
            async with self.session.get(url, headers=self._h("application/octet-stream"),
                                        allow_redirects=False) as r:
                if r.status in (301, 302, 303, 307):  # на хранилище токен не передаём
                    async with self.session.get(r.headers["Location"]) as r2:
                        blob = await r2.read()
                else:
                    blob = await r.read()
            raw = gzip.decompress(self.f.decrypt(blob))
            tmp = self.db_path + ".restore"
            with open(tmp, "wb") as fh:
                fh.write(raw)
            os.replace(tmp, self.db_path)
            self.last_hash = hashlib.sha256(raw).hexdigest()
            return True
        except Exception:
            log.exception("restore failed (неверный ENCRYPTION_KEY или повреждённый файл?)")
            return False

    def _snapshot(self) -> bytes:
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            src, dst = sqlite3.connect(self.db_path), sqlite3.connect(tmp)
            src.backup(dst)
            dst.close()
            src.close()
            with open(tmp, "rb") as fh:
                return fh.read()
        finally:
            os.remove(tmp)

    async def backup(self, force=False):
        if not self.enabled or not os.path.exists(self.db_path):
            return
        raw = await asyncio.to_thread(self._snapshot)
        h = hashlib.sha256(raw).hexdigest()
        if h == self.last_hash and not force:
            return
        blob = self.f.encrypt(gzip.compress(raw, 6))
        rid = await self._release_id(create=True)
        if not rid:
            return
        name = f"{PREFIX}{int(time.time())}.db.enc"
        up = f"https://uploads.github.com/repos/{self.repo}/releases/{rid}/assets?name={name}"
        async with self.session.post(up, data=blob, headers={**self._h(), "Content-Type": "application/octet-stream"}) as r:
            if r.status not in (200, 201):
                log.warning("бэкап не загружен: HTTP %s %s", r.status, (await r.text())[:200])
                return
        self.last_hash = h
        for a in await self._assets(rid):  # старые снимки удаляем только после успешной загрузки
            if a["name"].startswith(PREFIX) and a["name"] != name:
                async with self.session.delete(f"{API}/repos/{self.repo}/releases/assets/{a['id']}",
                                               headers=self._h()):
                    pass
        log.info("бэкап БД загружен (%d КБ)", len(blob) // 1024)

    async def loop(self):
        await asyncio.sleep(60)
        while True:
            try:
                await self.backup()
            except Exception:
                log.exception("backup failed")
            await asyncio.sleep(self.interval)
