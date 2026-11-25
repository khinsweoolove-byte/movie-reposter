"""Repost movies from the source channel through @UwinzawnyuntBot's own flow.

The bot publishes to its channels itself.  This script only drives the bot DM:
  /cancel -> /newpost -> poster photo -> synopsis -> a -> video file -> a
and waits for the bot's "Post ဖန်တီးပြီးပါပြီ" DM before marking the movie done.
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.sessions import StringSession

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = r"C:\Users\Bless Computer\AppData\Local\Temp\opencode"
SESSION = os.path.join(ROOT, "session_local.txt")
STATE = os.path.join(ROOT, "movie_repost_state.json")
API_ID = 31778137
API_HASH = "357247ed45b28459c6600e31cce5b773"
SOURCE = -1001705158369
BOT = "@UwinzawnyuntBot"
LEO = "@Leo_Movie_Review_Channel"
MMT = timezone(timedelta(hours=6, minutes=30))


def log(*parts):
    print(datetime.now(MMT).strftime("%H:%M:%S"), *parts, flush=True)


async def retry(coro_factory, times=5):
    for n in range(times):
        try:
            return await coro_factory()
        except FloodWaitError as exc:
            wait = int(exc.seconds) + 3
            log("FLOOD_WAIT", wait, "s")
            await asyncio.sleep(wait)
        except RPCError as exc:
            if n == times - 1:
                raise
            await asyncio.sleep(3 + n * 3)
    raise RuntimeError("retry exhausted")


STATE_ID = "movie_reposter_v1"
_MONGO_COL = None


def mongo_col():
    global _MONGO_COL
    if _MONGO_COL is not None:
        return _MONGO_COL
    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        _MONGO_COL = client.get_database()["movie_repost_state"]
    except Exception as exc:
        log("MONGO_STATE_ERR", type(exc).__name__)
        _MONGO_COL = None
    return _MONGO_COL


def session_string():
    env_value = os.getenv("USER_SESSION", "").strip()
    if env_value:
        return env_value
    path = (os.path.join(ROOT, "session_render_native.txt")
            if os.getenv("RENDER") else SESSION)
    return open(path, encoding="utf-8").read().strip()


def load_state():
    col = mongo_col()
    if col is not None:
        try:
            doc = col.find_one({"_id": STATE_ID}) or {}
            doc.pop("_id", None)
            return {"done": {}, "failed": {}, "cursor": 4, **doc}
        except Exception as exc:
            log("MONGO_LOAD_ERR", type(exc).__name__)
    if os.path.exists(STATE):
        try:
            with open(STATE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": {}, "failed": {}, "cursor": 4}


def save_state(state):
    col = mongo_col()
    if col is not None:
        try:
            doc = dict(state)
            doc["_id"] = STATE_ID
            col.replace_one({"_id": STATE_ID}, doc, upsert=True)
            return
        except Exception as exc:
            log("MONGO_SAVE_ERR", type(exc).__name__)
    tmp = STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE)


async def notify_completion(client, state, newest):
    """One-off Telegram notice when the whole source channel is done."""
    chat = os.getenv("NOTIFY_CHAT", "me").strip() or "me"
    stamp = datetime.now(MMT).strftime("%Y-%m-%d %H:%M")
    text = (
        "✅ အခုချန်နယ်ထဲက ဇာတ်ကား/စီးရီး အားလုံး တင်ပြီးပါပြီ။\n"
        f"Source last post: {newest}\n"
        f"Movies finished: {len(state['done'])}\n"
        f"Time: {stamp}"
    )
    try:
        await retry(lambda: client.send_message(chat, text))
        log("COMPLETION_DM_SENT", chat)
    except Exception as exc:
        log("COMPLETION_DM_FAIL", type(exc).__name__)


def norm(value):
    return " ".join(re.findall(r"[a-z0-9]+", (value or "").lower()))


def title_key(title):
    words = [w for w in norm(title).split()
             if w not in {"the", "a", "an", "of", "and", "part"}]
    return " ".join(words[:4])


EPISODE_RE = re.compile(
    r"^\s*(?:ep(?:isode)?|part|ပိုင်း)\s*[\.\-_: ]?\s*\d+", re.I)


def base_key(title):
    """Series key without an episode/part suffix."""
    cleaned = re.sub(
        r"\b(?:ep(?:isode)?|part)\s*[\.\-_]?\s*\d+\b",
        "", title or "", flags=re.I)
    return title_key(cleaned)


def clean_syn(text, title=""):
    lines = []
    tnorm = norm(title)
    for raw in (text or "").splitlines():
        line = raw.strip()
        if len(line) < 25:
            continue
        low = line.lower()
        if any(x in low for x in ("http", "t.me/", "@", "imdb", "rotten tomatoes",
                                   "rating", "subscribe", "join channel")):
            continue
        if norm(line) == tnorm:
            continue
        if set(line) <= set("=-_~*#• 0123456789"):
            continue
        lines.append(line)
    return "\n".join(lines[:6])[:1000].strip()


def parse_title(caption):
    for line in (caption or "").splitlines():
        line = line.strip()
        if not line or set(line) <= set("=-_~*#• 0123456789"):
            continue
        if re.search(r"imdb|rating|ရုပ်ရှင်|ဇာတ်ကား", line, re.I):
            continue
        return line[:160]
    return ""


def is_video(msg):
    if not msg or not msg.media:
        return False
    if msg.video:
        return True
    mime = str(getattr(getattr(msg, "document", None), "mime_type", "") or "")
    return mime.startswith("video/")


def scan_units(msgs):
    """One unit per movie file; photos/text just before it are its poster/synopsis."""
    units = []
    pending = []
    for m in sorted(msgs, key=lambda x: x.id):
        if is_video(m):
            title = parse_title(m.message) or parse_title(
                next((p.message for p in pending if p.message), ""))
            key = title_key(title)
            photos = [p for p in pending if p.photo]
            texts = [p for p in pending
                     if (p.message or "") and (p.photo or not p.media)]
            syn_parts = [(p.message or "") for p in texts]
            syn_parts.append(m.message or "")
            synopsis = clean_syn("\n".join(syn_parts), title)
            poster = photos[-1].id if photos else None
            prev = units[-1] if units else None
            episode = bool(EPISODE_RE.match((m.message or "").strip()))
            same_series = bool(
                prev and base_key(title) and base_key(title) == base_key(
                    prev["title"]))
            if prev and (same_series or (episode and not photos)):
                # Every episode/file of a series goes into the same bot flow.
                # A new series with its own poster starts a fresh unit.
                prev["videos"].append(m.id)
            else:
                units.append({
                    "video": m.id,
                    "videos": [m.id],
                    "poster": poster,
                    "title": title,
                    "key": key,
                    "synopsis": synopsis,
                })
            pending = []
        elif m.photo or m.message:
            pending.append(m)
            if len(pending) > 20:
                pending = pending[-20:]
    return units


async def homietv_lookup(title):
    if not title:
        return ""
    urls = [
        "https://www.homietv.com/?s=" + requests.utils.quote(title),
        "https://homietv.com/?s=" + requests.utils.quote(title),
    ]
    for url in urls:
        try:
            r = await asyncio.to_thread(
                requests.get, url, timeout=10,
                headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200 or len(r.text) < 500:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            node = (soup.select_one("div.entry-content")
                    or soup.select_one("article")
                    or soup.select_one("div.post-content"))
            chunks = []
            if node:
                chunks = [p.get_text(" ", strip=True)
                          for p in node.find_all("p")]
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                chunks.append(meta["content"])
            good = [c for c in chunks if len(c) >= 60]
            if good:
                return clean_syn(max(good, key=len), title)
        except Exception as exc:
            log("HOMIE_SKIP", type(exc).__name__)
    return ""


async def leo_lookup(client, title):
    words = [w for w in norm(title).split() if len(w) >= 4]
    if not words:
        return ""
    try:
        hits = await retry(lambda: client.get_messages(
            LEO, search=words[0], limit=30), times=3)
    except Exception as exc:
        log("LEO_SKIP", type(exc).__name__)
        return ""
    key = norm(title)
    scored = []
    for m in hits:
        text = m.message or ""
        n = norm(text)
        if len(text) < 80:
            continue
        score = sum(1 for w in key.split() if w in n)
        need = 2 if len(key.split()) >= 2 else 1
        if key in n or score >= need:
            scored.append((score, len(text), text))
    if not scored:
        return ""
    scored.sort(reverse=True)
    return clean_syn(scored[0][2], title)


async def tmdb_lookup(title):
    key = os.getenv("TMDB_API_KEY", "").strip()
    key_file = os.path.join(ROOT, "tmdb_key.txt")
    if not key and os.path.exists(key_file):
        try:
            key = open(key_file, encoding="utf-8").read().strip()
        except Exception:
            key = ""
    if not key or not title:
        return ""
    bearer = key.startswith("eyJ")

    variants = []

    def add_variant(value):
        v = " ".join((value or "").split())
        if v and v not in variants:
            variants.append(v)

    add_variant(title)
    no_year = re.sub(
        r"\s*\(\s*(?:19|20)\d{2}\s*\)\s*$|\s+(?:19|20)\d{2}\s*$",
        "", title).strip()
    add_variant(no_year)
    ascii_only = re.sub(r"[^\x00-\x7F]+", " ", no_year)
    add_variant(ascii_only)

    url = "https://api.themoviedb.org/3/search/movie"
    try:
        for query in variants:
            params = {"query": query}
            hdr = {}
            if bearer:
                hdr = {"Authorization": "Bearer " + key}
            else:
                params["api_key"] = key
            r = await asyncio.to_thread(
                requests.get, url, params=params, timeout=25, headers=hdr)
            if r.status_code != 200:
                continue
            results = (r.json() or {}).get("results") or []
            if not results:
                continue
            overview = (results[0].get("overview") or "").strip()
            mid = results[0].get("id")
            if len(overview) < 40 and mid:
                if bearer:
                    durl = "https://api.themoviedb.org/3/movie/%s" % mid
                    dh = hdr
                    dp = {}
                else:
                    durl = "https://api.themoviedb.org/3/movie/%s" % mid
                    dh = {}
                    dp = {"api_key": key}
                dr = await asyncio.to_thread(
                    requests.get, durl, params=dp, timeout=25, headers=dh)
                if dr.status_code == 200:
                    overview = (dr.json() or {}).get("overview") or ""
            if len(overview) >= 40:
                return clean_syn(overview, title)
    except Exception as exc:
        log("TMDB_SKIP", type(exc).__name__)
    return ""


async def resolve_synopsis(client, unit):
    syn = clean_syn(unit["synopsis"], unit["title"])
    if len(syn) >= 40:
        return syn, "source"
    log("FALLBACK", unit["title"][:60])
    for label, fn in (("homietv", lambda: homietv_lookup(unit["title"])),
                      ("leo", lambda: leo_lookup(client, unit["title"])),
                      ("tmdb", lambda: tmdb_lookup(unit["title"]))):
        try:
            found = await fn()
        except Exception as exc:
            log(label, "ERR", type(exc).__name__)
            found = ""
        if len(found) >= 40:
            return found, label
    return "", "none"


async def wait_confirm(client, bot, before, timeout=420):
    start = time.time()
    while time.time() - start < timeout:
        got = await retry(lambda: client.get_messages(
            bot, min_id=before, limit=40), times=3)
        fresh = [m for m in got if m and not m.out and m.id > before]
        for m in fresh:
            text = m.message or ""
            if "Post ဖန်တီးပြီးပါပြီ" in text or "ဖန်တီးပြီးပါပြီ" in text:
                return True, text
            if "❌" in text or "မအောင်မြင်" in text:
                return False, text
        await asyncio.sleep(6)
    return False, "confirm timeout"


async def publish(client, bot, unit, synopsis):
    before = max([m.id for m in await client.get_messages(bot, limit=20)]
                 + [0])
    caption = unit["title"] + ("\n" + synopsis if synopsis else "")
    await retry(lambda: client.send_message(bot, "/cancel"))
    await asyncio.sleep(1.5)
    await retry(lambda: client.send_message(bot, "/newpost"))
    await asyncio.sleep(2)
    if not unit["poster"]:
        return False, "no poster photo"
    await retry(lambda: client.forward_messages(
        bot, [unit["poster"]], from_peer=SOURCE))
    await asyncio.sleep(1.3)
    await retry(lambda: client.send_message(bot, caption[:4000]))
    await asyncio.sleep(1)
    await retry(lambda: client.send_message(bot, "a"))
    await asyncio.sleep(1.8)
    for vid in unit["videos"]:
        await retry(lambda v=vid: client.forward_messages(
            bot, [v], from_peer=SOURCE))
        await asyncio.sleep(1.3)
    await retry(lambda: client.send_message(bot, "a"))
    ok, reply = await wait_confirm(client, bot, before)
    return ok, reply


def done_today(state):
    today = datetime.now(MMT).strftime("%Y-%m-%d")
    return sum(1 for r in state["done"].values()
               if str(r.get("date", "")).startswith(today))


async def run_batch(client, bot, src, state, args):
    newest = (await retry(lambda: client.get_messages(src, limit=1)))[0].id
    cursor = int(state.get("cursor", 4))
    start_id = args.start if args.start is not None else cursor
    if args.start is None and state["failed"]:
        # Retry failed movies, with a little lookback for their poster/text,
        # but never let an old failure shrink the fresh window at the cursor.
        retry_lo = min([start_id] + [int(k) for k in state["failed"]]) - 20
        start_id = min(start_id, retry_lo)
    lo = max(1, start_id)
    hi = min(newest, max(lo + args.window - 1, cursor + args.window - 1))
    log("SCAN", lo, hi, "NEWEST", newest, "CURSOR", cursor,
        "FAILED", len(state["failed"]))
    ids = list(range(lo, hi + 1))
    msgs = await retry(lambda: client.get_messages(src, ids=ids))
    units = scan_units([m for m in msgs if m])
    log("UNITS", len(units), "DONE_TODAY", done_today(state))
    done_keys = {str(v.get("key", "")) for v in state["done"].values()}
    made = 0
    limit = args.pilot or args.hourly_limit or 0

    for unit in units:
        if limit and made >= limit:
            break
        uid = str(unit["video"])
        if uid in state["done"] or unit["key"] in done_keys:
            continue
        if args.daily > 0 and done_today(state) >= args.daily:
            log("DAILY_LIMIT_REACHED", args.daily)
            break
        syn, source = await resolve_synopsis(client, unit)
        log("MOVIE", uid, unit["title"][:70], "SYN", source, len(syn),
            "FILES", len(unit["videos"]))
        if args.dry_run:
            log("SYN_PREVIEW", uid, syn[:160].replace("\n", " "))
            made += 1
            continue
        ok, reply = await publish(client, bot, unit, syn)
        stamp = datetime.now(MMT).isoformat(timespec="seconds")
        if ok:
            state["done"][uid] = {
                "title": unit["title"], "key": unit["key"],
                "date": stamp, "syn": source, "reply": reply[:160],
                "files": len(unit["videos"])}
            state["failed"].pop(uid, None)
            done_keys.add(unit["key"])
            log("OK", uid, unit["title"][:60], "FILES", len(unit["videos"]))
            made += 1
        else:
            state["failed"][uid] = {
                "title": unit["title"], "date": stamp,
                "why": str(reply)[:200]}
            log("FAIL", uid, str(reply)[:160])
        state["cursor"] = max(int(state.get("cursor", 4)),
                              unit["video"] + 1)
        save_state(state)
        if limit and made >= limit:
            break
        await asyncio.sleep(args.interval)

    if hi >= newest and not state["failed"]:
        state["cursor"] = newest + 1
        save_state(state)
    if int(state.get("cursor", 4)) > newest and not state.get(
            "completed_notified"):
        await notify_completion(client, state, newest)
        state["completed_notified"] = True
        save_state(state)
    log("FINISHED", "NEW", made, "DONE_TOTAL", len(state["done"]))
    return made, newest


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=None,
                    help="source message id; defaults to saved cursor")
    ap.add_argument("--window", type=int,
                    default=int(os.getenv("WINDOW", "120")))
    ap.add_argument("--pilot", type=int, default=0, help="max new movies, 0=loop")
    ap.add_argument("--daily", type=int,
                    default=int(os.getenv("DAILY_LIMIT", "50")))
    ap.add_argument("--interval", type=int,
                    default=int(os.getenv("INTERVAL_SECONDS", "1200")),
                    help="seconds/movie")
    ap.add_argument("--hourly", action="store_true")
    ap.add_argument("--posts-per-hour", type=int,
                    default=int(os.getenv("POSTS_PER_HOUR", "0")))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    hourly = args.hourly or os.getenv("HOURLY", "0") == "1"
    if hourly and args.posts_per_hour <= 0:
        args.posts_per_hour = 5
    args.hourly_limit = args.posts_per_hour if hourly else 0

    state = load_state()
    client = TelegramClient(StringSession(session_string()), API_ID, API_HASH)
    await client.connect()
    try:
        me = await client.get_me()
        log("USER", me.id, "MODE", "hourly" if hourly else "single")
        src = await retry(lambda: client.get_entity(SOURCE))
        bot = await retry(lambda: client.get_entity(BOT))
        while True:
            made, newest = await run_batch(client, bot, src, state, args)
            if state.get("completed_notified") or not hourly:
                break
            now = datetime.now(MMT)
            nxt = (now + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0)
            wait = max(60, int((nxt - now).total_seconds()) + 5)
            log("NEXT_BATCH_IN", wait, "s", "MADE", made)
            await asyncio.sleep(wait)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
