"""Post queue: decides when each finished clip goes to which account, posts it, and tracks views."""
import secrets
import threading
import time
from pathlib import Path

import json

from . import accounts, alerts, config, creators, ctx, jobs, live

_lock = threading.RLock()
_FILE = config.DATA_DIR / "posts.json"
try:
    _posts: list = json.loads(_FILE.read_text(encoding="utf-8")) if _FILE.exists() else []
except ValueError:
    _posts = []
_mfile_mtime = _FILE.stat().st_mtime if _FILE.exists() else 0.0


def _sync_from_disk():
    """Pick up queue changes written while the server was running (e.g. fast reschedule)."""
    global _posts, _mfile_mtime
    try:
        mt = _FILE.stat().st_mtime
    except OSError:
        return
    if mt <= _mfile_mtime + 0.001:
        return
    with _lock:
        try:
            _posts = json.loads(_FILE.read_text(encoding="utf-8"))
            _mfile_mtime = mt
        except (OSError, ValueError):
            pass


def _mine() -> list:
    u = ctx.uid()
    return [p for p in _posts if p.get("user") == u]
OPEN = ("scheduled", "posting")
_upload_lock = threading.Lock()
FAST_GAP = {"youtube": 3, "instagram": 15, "tiktok": 3}


def _fast_on() -> bool:
    try:
        return bool(accounts.settings().get("post_fast"))
    except Exception:  # noqa: BLE001
        return False


def _gap_minutes(platform: str) -> int:
    if _fast_on():
        return FAST_GAP.get(platform, 3)
    return 45


def _save():
    with _lock:
        tmp = _FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_posts, indent=1), encoding="utf-8")
        tmp.replace(_FILE)
        try:
            _mfile_mtime = _FILE.stat().st_mtime
        except OSError:
            pass


def forget(uid: str):
    """Drop everything queued or recorded for a deleted account, so nothing is posted or retried for it any more."""
    with _lock:
        for p in _posts:
            if p.get("user") == uid:
                p["status"] = "deleted"          # the posting loop may be holding this one already: it then skips it
        _posts[:] = [p for p in _posts if p.get("user") != uid]
        _save()


def _day(t):
    return time.strftime("%Y-%m-%d", time.localtime(t))


def _mirror(p):
    """Copy the post's state onto its clip so the clip card can show it."""
    uid = p.get("user")
    if not uid:
        return
    with ctx.as_user(uid):
        job = jobs.get(p["job"])
        clip = next((c for c in job["clips"] if c["id"] == p["clip"]), None) if job else None
        if clip is not None:
            clip.setdefault("posts", {})[p["id"]] = {k: p.get(k) for k in (
                "account_name", "platform", "status", "url", "due_at", "error", "post_progress", "post_started_at")}
            jobs.save(job)


def _next_slot(account_id, gap_min, cap):
    """Earliest time that respects the gap between posts and the daily cap for this account."""
    mine = [p for p in _mine() if p["account"] == account_id and p["status"] in OPEN + ("posted",)]
    t = max(time.time(), max((p.get("posted_at") or p["due_at"] for p in mine), default=0) + gap_min * 60)
    for _ in range(30):
        if sum(1 for p in mine if _day(p.get("posted_at") or p["due_at"]) == _day(t)) < cap:
            break
        lt = time.localtime(t + 86400)
        t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 9, 0, 0, 0, 0, -1))  # 9:00 next day
    return t


def enqueue(job, clip, account_ids, now=False) -> list:
    c = creators.get(job["creator"]) or creators.DEFAULTS
    made = []
    with _lock:
        for aid in account_ids:
            acc = accounts.get(aid)
            if not acc:
                continue
            if any(p["job"] == job["id"] and p["clip"] == clip["id"] and p["account"] == aid
                   and p["status"] in OPEN + ("posted",) for p in _mine()):
                continue  # never post the same clip to the same account twice
            if now and acc["platform"] != "instagram":
                due = time.time()
            else:
                gap = c["post_gap_min"]
                if acc["platform"] == "instagram":
                    gap = max(15, gap)  # Meta rejects back-to-back Reels even when you press Post now
                due = _next_slot(aid, gap, c["daily_cap"])
            p = {"id": secrets.token_hex(8), "user": ctx.uid(), "job": job["id"], "clip": clip["id"], "creator": job["creator"],
                 "account": aid, "account_name": acc["name"], "platform": acc["platform"], "status": "scheduled",
                 "due_at": due, "created": time.time(), "title": clip.get("title") or clip.get("hook") or "Clip",
                 "score": clip.get("score", 0), "tries": 0, "url": "", "remote_id": "", "error": None, "views": None}
            _posts.append(p)
            made.append(p)
        _save()
    for p in made:
        _mirror(p)
    return made


def bulk_enqueue(pairs: list, account_ids: list, now: bool = False) -> dict:
    """Queue many clips to the same accounts. Default spacing keeps Instagram under rate limits."""
    if accounts.settings()["paused"]:
        raise ValueError("Posting is paused. Resume on Home, then try again.")
    account_ids = [str(a) for a in account_ids if a][:20]
    if not account_ids:
        raise ValueError("Pick at least one account.")
    posts_made, clips_ok, skipped = 0, 0, 0
    first_immediate = not now  # when now=True, only the first clip that actually queues goes out ASAP
    for jid, cid in pairs:
        job = jobs.get(jid)
        if not job:
            skipped += 1
            continue
        clip = next((c for c in job["clips"] if c["id"] == cid), None)
        if not clip or clip.get("status") != "done":
            skipped += 1
            continue
        use_now = now and not first_immediate
        made = enqueue(job, clip, account_ids, now=use_now)
        if use_now and made:
            first_immediate = True
        if made:
            posts_made += len(made)
            clips_ok += 1
        else:
            skipped += 1
    if not posts_made:
        raise ValueError("Nothing was queued. Each clip may already be on those accounts, or none are ready.")
    return {"queued": posts_made, "clips": clips_ok, "skipped": skipped}


def enqueue_ready_clips(job):
    """After shared framing is set, queue auto-post for clips that were waiting."""
    c = creators.get(job.get("creator"))
    if not c or not c["autopost"] or not c["accounts"]:
        return 0
    n = 0
    for clip in job.get("clips", []):
        if clip.get("status") != "done" or clip.get("score", 0) < c["post_min_score"]:
            continue
        made = enqueue(job, clip, c["accounts"])
        n += len(made)
    return n


def on_clip_ready(job, clip):
    """Called once when a clip first finishes rendering."""
    c = creators.get(job["creator"])
    if not c or not c["autopost"] or not c["accounts"]:
        return
    lay = c.get("layout") or clip.get("layout") or "blur"
    if c.get("post_after_framing", True) and lay in ("stack", "gameplay"):
        if not (job.get("facecam") and job.get("gameplay")):
            creators.log(c["id"], "Auto-post waiting: set face & gameplay once for this video (all clips share it).")
            return
    if clip.get("score", 0) < c["post_min_score"]:
        return  # includes hand-made clips (score 0): those wait for you to press Post
    made = enqueue(job, clip, c["accounts"])
    if made:
        creators.log(c["id"], f"Queued for {len(made)} account(s): {clip.get('title') or clip.get('hook')}")


def _parse_tag_list(raw) -> list:
    import re
    if isinstance(raw, list):
        parts = raw
    else:
        parts = re.split(r"[,;#\n]+", str(raw or ""))
    out, seen = [], set()
    for p in parts:
        t = re.sub(r"\W", "", str(p).strip())[:30]
        if t and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)
    return out[:15]


def _all_tags(clip, creator) -> list:
    """Clip tags plus Settings → Default tags. post_no_tags skips AI clip tags only."""
    s = accounts.settings()
    custom = _parse_tag_list(s.get("post_tags") or "")
    if s.get("post_no_tags"):
        return custom[:15]
    out, seen = [], set()
    for t in [h for h in clip.get("hashtags", []) if h] + custom:
        k = str(t).lower()
        if k not in seen:
            seen.add(k)
            out.append(t)
    return out[:15]


def _caption(clip, creator):
    s = accounts.settings()
    parts = [clip.get("description", "")]
    tags = _all_tags(clip, creator)
    if tags:
        parts.append(" ".join("#" + h for h in tags))
    footer = (s.get("post_footer") or creator.get("post_footer") or "").strip()
    if footer:
        parts.append(footer)
    return "\n\n".join(x for x in parts if x)


def _yt_tags(clip, creator) -> list:
    return _all_tags(clip, creator)


def _yt_upload_cap_day() -> float:
    """~9:00 next local day after YouTube's daily upload cap."""
    lt = time.localtime(time.time() + 86400)
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 9, 0, 0, 0, 0, -1))


def _human_error(error: str, platform: str) -> str:
    err = (error or "").lower()
    if platform == "youtube" and ("uploadlimitexceeded" in err.replace(" ", "") or "exceeded the number of videos" in err):
        return "YouTube daily upload limit on this channel. Will retry tomorrow morning."
    if platform == "instagram" and "request limit" in err:
        return "Instagram rate limit — waiting before retry."
    return (error or "Upload failed.")[-300:]


def _retry_at(error: str, tries: int, platform: str) -> float:
    """When Meta or TikTok rate-limits, wait longer than a normal failure."""
    err = (error or "").lower()
    if platform == "youtube" and ("uploadlimitexceeded" in err.replace(" ", "") or "exceeded the number of videos" in err):
        return _yt_upload_cap_day()
    if platform == "instagram" and ("request limit" in err or '"code":4' in err or "rate" in err and "limit" in err):
        return time.time() + (2700 if tries < 2 else 5400)  # 45 min, then 90 min
    if "exceeded the number" in err or "quota" in err:
        return _yt_upload_cap_day() if platform == "youtube" else time.time() + 3600
    if _fast_on():
        return time.time() + min(300, 60 * tries)  # 1 min, 2 min, … up to 5 min
    return time.time() + 600


def _defer_account_quota(account_id: str, due_at: float):
    """When one channel hits YouTube's daily cap, push its other queued posts to the same time."""
    with _lock:
        n = 0
        for p in _posts:
            if p.get("account") != account_id or p["status"] != "scheduled":
                continue
            if p["due_at"] < due_at:
                p["due_at"] = due_at
                n += 1
        if n:
            _save()


def _max_tries() -> int:
    return 8 if _fast_on() else 3


def accelerate_queue() -> dict:
    """Pull the whole queue forward: one clip after another per account with safe minimum gaps."""
    if accounts.settings()["paused"]:
        raise ValueError("Posting is paused. Resume on Home first.")
    moved, revived = 0, 0
    with _lock:
        sched = sorted([p for p in _mine() if p["status"] == "scheduled"], key=lambda p: (p["due_at"], p.get("created", 0)))
        last_end = {}
        t0 = time.time()
        for p in sched:
            aid = p["account"]
            gap = _gap_minutes(p.get("platform", "youtube")) * 60
            start = max(t0, last_end.get(aid, t0))
            if abs(p["due_at"] - start) > 5:
                moved += 1
            p["due_at"] = start
            last_end[aid] = start + gap
        for p in _mine():
            if p["status"] != "failed" or (p.get("tries") or 0) >= _max_tries():
                continue
            err = (p.get("error") or "").lower()
            if "already posted" in err or "disconnected" in err:
                continue
            aid = p["account"]
            gap = _gap_minutes(p.get("platform", "youtube")) * 60
            start = max(t0, last_end.get(aid, t0))
            p.update(status="scheduled", due_at=start, tries=min(p.get("tries") or 0, _max_tries() - 1))
            last_end[aid] = start + gap
            revived += 1
        _save()
    return {"rescheduled": moved, "retries_queued": revived, "fast": _fast_on()}


def _unstick_posting():
    """A post stuck in 'posting' (crash mid-upload) goes back in the queue."""
    now = time.time()
    n = 0
    with _lock:
        for p in _posts:
            if p.get("status") != "posting":
                continue
            started = p.get("post_started_at") or p.get("due_at") or now
            if now - started < 480:  # 8 min on one upload → retry (stops 96% freeze)
                continue
            p.update(status="scheduled", due_at=now, error=(p.get("error") or "")[:80] + " (retry after stall)")
            n += 1
        if n:
            _save()
    return n


def _progress_loop(p, path: Path):
    est = max(40.0, path.stat().st_size / (1.1 * 1024 * 1024))
    while True:
        time.sleep(1.2)
        with _lock:
            if p.get("status") != "posting":
                return
            el = time.time() - (p.get("post_started_at") or time.time())
            base = int(6 + el / est * 90)
            if el > est * 1.1:
                base = max(base, min(99, 96 + int((el - est) / max(est, 1) * 8)))
            p["post_progress"] = min(99, base)
        _save()
        _mirror(p)


def _post(p):
    job = jobs.get(p["job"])
    clip = next((c for c in job["clips"] if c["id"] == p["clip"]), None) if job else None
    acc = accounts.get(p["account"])
    p["post_started_at"] = time.time()
    p["post_progress"] = 3
    _save()
    _mirror(p)
    progress = None
    try:
        if not job:
            raise RuntimeError("That video was removed. This post was cancelled.")
        if not clip or clip.get("status") != "done":
            raise RuntimeError("The clip no longer exists or is not finished.")
        if not acc:
            raise RuntimeError("That account was disconnected.")
        path = Path(job["dir"]) / "clips" / clip["file"]
        progress = threading.Thread(target=_progress_loop, args=(p, path), daemon=True)
        progress.start()
        creator = creators.get(job["creator"]) or dict(creators.DEFAULTS, name="clips")
        title, cap = clip.get("title") or clip.get("hook") or "Clip", _caption(clip, creator)
        if acc["platform"] == "youtube":
            rid, url = accounts.youtube_upload(acc, path, title, cap + "\n\n#Shorts", _yt_tags(clip, creator), creator["privacy"])
        elif acc["platform"] == "instagram":
            rid, url = accounts.instagram_upload(acc, path, f"{title}\n\n{cap}", job["id"])
        elif acc["platform"] == "tiktok":
            rid, url = accounts.tiktok_upload(acc, path, f"{title}\n\n{cap}", creator["privacy"])
        else:
            raise RuntimeError("That kind of account is no longer supported.")
        p.update(status="posted", posted_at=time.time(), remote_id=rid, url=url, error=None, post_progress=100)
        creators.log(p["creator"], f"Posted to {acc['name']}: {title}")
        _drop_stale_queue_dupes()
        dedupe_posted()
    except Exception as e:  # noqa: BLE001
        p["tries"] += 1
        p["error"] = _human_error(str(e), p.get("platform", ""))
        err_l = str(e).lower()
        if clip and acc and p["tries"] < _max_tries():
            due = _retry_at(str(e), p["tries"], p.get("platform", ""))
            p.update(status="scheduled", due_at=due)
            if acc["platform"] == "youtube" and "exceeded the number of videos" in err_l:
                _defer_account_quota(p["account"], due)
        else:
            p["status"] = "failed"
            creators.log(p["creator"], f"Post failed on {p['account_name']}: {p['error']}")
            alerts.notify(f"post failed on {p['account_name']} ({p['title']}): {p['error'][:200]}", kind="post")
    _save()
    _mirror(p)


def _next_due():
    """One scheduled post whose time has come (soonest first)."""
    now = time.time()
    due = [p for p in _posts if p["status"] == "scheduled" and p["due_at"] <= now and p.get("user")]
    if not due:
        return None
    return min(due, key=lambda p: (p["due_at"], p.get("created", 0)))


def _queue_progress() -> dict:
    mine = _mine()
    open_ps = [p for p in mine if p["status"] in OPEN]
    posted_n = sum(1 for p in mine if p["status"] == "posted")
    failed_n = sum(1 for p in mine if p["status"] == "failed")
    total = posted_n + len(open_ps) + failed_n
    active = next((p for p in _posts if p.get("status") == "posting"), None)
    sched = sorted((p for p in mine if p["status"] == "scheduled"), key=lambda p: p["due_at"])
    next_id = sched[0]["id"] if sched else ""
    pct = int(100 * posted_n / total) if total else 0
    if active:
        pct = min(99, max(pct, int(active.get("post_progress") or 0)))
    def slim(p):
        if not p:
            return None
        return {"id": p["id"], "title": p.get("title"), "account_name": p.get("account_name"),
                "platform": p.get("platform"), "job": p.get("job"), "clip": p.get("clip"),
                "post_progress": p.get("post_progress"), "status": p.get("status"), "due_at": p.get("due_at")}
    return {"percent": pct, "posted": posted_n, "waiting": len(open_ps), "failed": failed_n, "total": total,
            "active": slim(active), "next": slim(sched[0] if sched else None), "next_id": next_id}


def _run():
    while True:
        _sync_from_disk()
        _unstick_posting()
        time.sleep(5 if any(p.get("user") and p["status"] in OPEN for p in _posts) else 12)
        p = _next_due()
        if not p:
            continue
        try:
            with ctx.as_user(p["user"]):
                if accounts.settings()["paused"]:
                    continue
                with _lock:
                    if p["status"] != "scheduled":
                        continue
                    p["status"] = "posting"
                with _upload_lock:  # one upload at a time across all accounts
                    _post(p)
                if _fast_on():
                    gap = _gap_minutes(p.get("platform", "youtube"))
                    time.sleep(min(gap * 60, 30) if gap <= 3 else 0)  # brief pause after YouTube; IG spacing is in due_at
        except Exception:  # noqa: BLE001
            pass


def _stats_for(recent):
    for aid in {p["account"] for p in recent if p["platform"] == "youtube"}:
        acc = accounts.get(aid)
        if acc:
            views = accounts.youtube_views(acc, [p["remote_id"] for p in recent if p["account"] == aid])
            for p in recent:
                if p["account"] == aid and p["remote_id"] in views:
                    p["views"] = views[p["remote_id"]]
    for p in recent:
        acc = accounts.get(p["account"]) if p["platform"] == "instagram" else None
        if acc:
            v = accounts.instagram_views(acc, p["remote_id"])
            if v is not None:
                p["views"] = v


def _stats():
    while True:
        for uid in {p.get("user") for p in _posts if p.get("user")}:
            try:
                with ctx.as_user(uid):
                    _stats_for([p for p in _mine() if p["status"] == "posted" and time.time() - p["posted_at"] < 14 * 86400])
            except Exception:  # noqa: BLE001
                pass
        _save()
        time.sleep(1800)


def cancel(pid):
    with _lock:
        p = next((p for p in _mine() if p["id"] == pid), None)
        if not p or p["status"] != "scheduled":
            raise ValueError("Only posts that are still waiting can be cancelled.")
        p["status"] = "cancelled"
        _save()
    _mirror(p)


def _drop_stale_queue_dupes():
    """If a clip already posted to an account, cancel any failed/scheduled rows for the same triple."""
    by = {}
    for p in _mine():
        k = (p["job"], p["clip"], p["account"])
        by.setdefault(k, []).append(p)
    n = 0
    with _lock:
        for ps in by.values():
            if not any(x["status"] == "posted" for x in ps):
                continue
            for p in ps:
                if p["status"] in OPEN + ("failed",):
                    p["status"] = "cancelled"
                    p["error"] = (p.get("error") or "")[:120] + " (skipped: already posted)"
                    n += 1
        if n:
            _save()
    return n


def dedupe_posted() -> dict:
    """Same clip on the same account twice: keep the first upload, delete the extra on YouTube/Instagram."""
    removed, cancelled, errors = 0, _drop_stale_queue_dupes(), []
    by = {}
    for p in _mine():
        if p["status"] == "posted" and p.get("remote_id"):
            k = (p["job"], p["clip"], p["account"])
            by.setdefault(k, []).append(p)
    with _lock:
        for ps in by.values():
            if len(ps) < 2:
                continue
            ps.sort(key=lambda x: x.get("posted_at") or x.get("created", 0))
            keep, extras = ps[0], ps[1:]
            for p in extras:
                acc = accounts.get(p["account"])
                try:
                    if acc and p["platform"] == "youtube":
                        accounts.youtube_delete(acc, p["remote_id"])
                    elif acc and p["platform"] == "instagram":
                        accounts.instagram_delete(acc, p["remote_id"])
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{p['title'][:40]}: {e}")
                    continue
                p.update(status="duplicate_removed", error="Removed: duplicate of an earlier post to this account.")
                removed += 1
                creators.log(p["creator"], f"Removed duplicate on {p['account_name']}: {p['title']}")
        if removed:
            _save()
    return {"removed": removed, "cancelled_stale": cancelled, "errors": errors}


def retry(pid):
    with _lock:
        p = next((p for p in _mine() if p["id"] == pid), None)
        if not p or p["status"] not in ("failed", "cancelled"):
            raise ValueError("Only failed or cancelled posts can be retried.")
        p.update(status="scheduled", due_at=time.time(), tries=0, error=None)
        _save()
    _mirror(p)


def overview() -> dict:
    today = _day(time.time())
    _posts = _mine()  # everything below is about the signed-in user only
    rows = []
    all_cs, all_js = creators.all_creators(), jobs.list_jobs()
    for c in all_cs:
        js = [j for j in all_js if j["creator"] == c["id"]]
        clips = [k for j in js for k in j["clips"] if k.get("status") == "done"]
        ps = [p for p in _posts if p["creator"] == c["id"]]
        views = sum(p["views"] or 0 for p in ps)
        rows.append({
            "id": c["id"], "name": c["name"],
            "state": "recording" if live.active_for(c["id"]) else "waiting" if c["auto_watch"] and c["channel"] else "idle",
            "autopost": c["autopost"] and bool(c["accounts"]),
            "clips_today": sum(1 for k in clips if _day(k.get("made_at", 0)) == today), "clips_total": len(clips),
            "posted_today": sum(1 for p in ps if p["status"] == "posted" and _day(p["posted_at"]) == today),
            "posted_total": sum(1 for p in ps if p["status"] == "posted"),
            "queued": sum(1 for p in ps if p["status"] in OPEN), "failed": sum(1 for p in ps if p["status"] == "failed"),
            "views": views, "earnings": round(views / 1000 * c["rate_per_1k"], 2)})
    names = {c["id"]: c["name"] for c in all_cs}
    qprog = _queue_progress()
    next_id = qprog.get("next_id")
    queue = sorted((p for p in _posts if p["status"] in OPEN), key=lambda p: p["due_at"])[:40]
    done = sorted((p for p in _posts if p["status"] == "posted"), key=lambda p: -(p.get("posted_at") or 0))[:8]
    failed = [p for p in _posts if p["status"] == "failed"][:8]
    show = []
    for p in queue + done + failed:
        row = {**{k: v for k, v in p.items() if k != "user"}, "creator_name": names.get(p["creator"], "")}
        row["is_next"] = p.get("id") == next_id and p.get("status") == "scheduled"
        show.append(row)
    # last 14 days, oldest first, for the home graph
    days = [_day(time.time() - 86400 * i) for i in range(13, -1, -1)]
    made, posted, views = {d: 0 for d in days}, {d: 0 for d in days}, {d: 0 for d in days}
    for j in all_js:
        for k in j["clips"]:
            d = _day(k.get("made_at") or j["created"])
            if k.get("status") == "done" and d in made:
                made[d] += 1
    for p in _posts:
        if p["status"] == "posted" and _day(p["posted_at"]) in posted:
            posted[_day(p["posted_at"])] += 1
            views[_day(p["posted_at"])] += p["views"] or 0
    daily = [{"day": d, "clips": made[d], "posted": posted[d], "views": views[d]} for d in days]
    s = accounts.settings()
    return {"paused": s["paused"], "post_fast": bool(s.get("post_fast")), "queue_progress": qprog,
            "creators": rows, "posts": show, "daily": daily,
            "alerts": alerts.configured(), "recording": len(live.active_mine())}


def digest_text() -> str:
    o = overview()
    lines = ["PeakCut daily summary"]
    for r in o["creators"]:
        lines.append(f"- {r['name']}: {r['state']}, {r['clips_today']} clips today, {r['posted_today']} posted today, "
                     f"{r['queued']} waiting, {r['failed']} failed, {r['views']} views")
    if not o["creators"]:
        lines.append("No creators set up yet.")
    if o["paused"]:
        lines.append("Posting is paused.")
    return "\n".join(lines)


def start():
    with _lock:
        for p in _posts:
            if p["status"] == "posting":  # interrupted mid-upload last time
                p.update(status="failed", error="The studio closed during this upload. Check the account, then retry.")
        _save()
    jobs.on_clip_ready.append(on_clip_ready)
    threading.Thread(target=_run, daemon=True).start()
    threading.Thread(target=_stats, daemon=True).start()
    threading.Thread(target=alerts.digest_loop, args=(digest_text,), daemon=True).start()
    threading.Thread(target=alerts.keep_awake_loop,
                     args=(lambda: bool(live.active()) or any(p["status"] in OPEN for p in _posts),), daemon=True).start()
