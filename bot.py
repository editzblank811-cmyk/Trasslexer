#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hindi Translate Bot for Telegram  (EPUB + TXT, Chinese or English -> Hindi)

* Pure Python standard library. Nothing to pip install.
* Only needs one setting: BOT_TOKEN (from @BotFather).
* Files up to 15 MB. Send a .epub or .txt, get the Hindi file back.
* Uses many free translate engines at the same time (work-stealing queue),
  falls back automatically when one is slow, blocked or wrong.
* Live progress bar with time used and estimated time left.
* Every translated line is checked; lines that still look untranslated are
  retried on other engines, then split into smaller pieces and retried again,
  and the finished file is scanned one last time.

PATCHES (v1.0.1):
* Run.salvage(): when the run stalls but >= 90% is translated, the unfinished
  lines keep their best/original text and the file is still built and sent.
* leftover_score(): short names / words that an engine returns unchanged are
  no longer treated as "untranslated" (stops endless retries on names).
"""
from __future__ import annotations

import collections
import gzip
import html
import http.client
import http.server
import json
import logging
import os
import posixpath
import re
import shutil
import signal
import socket
import ssl
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import uuid
import zipfile
import zlib
from concurrent.futures import ThreadPoolExecutor

VERSION = "1.0.1"

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
MAX_FILE_BYTES = 15 * 1024 * 1024          # 15 MB input limit
TG_UPLOAD_LIMIT = 49 * 1000 * 1000         # Telegram bots can upload 50 MB
PIECE_MAX = 1000                           # longest text sent to an engine
MAX_TRIES = 3                              # different engines tried per suspicious line
STALL_SECONDS = float(os.environ.get("STALL_SECONDS", "240"))
MAX_JOBS = max(1, int(os.environ.get("MAX_JOBS", "2")))
ZIP_MAX_ENTRIES = 30000
ZIP_MAX_UNPACKED = 600 * 1024 * 1024
SCAN_LIMIT = 400
SALVAGE_MIN_FRACTION = 0.90                # on stall, send the file if at least this much is done
TARGET = "hi"
UA = ("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36")

log = logging.getLogger("hindibot")
STOP = threading.Event()
FATAL = threading.Event()          # set when the token is rejected: exit code 1, no auto-restart
MAX_RUNTIME_MIN = float(os.environ.get("MAX_RUNTIME_MIN", "0") or 0)   # 0 = run forever
DRAIN_MIN = float(os.environ.get("DRAIN_MIN", "12") or 12)

# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class UserError(Exception):
    """Problem with the user's file. Message is shown to the user."""


class Cancelled(Exception):
    pass


class Stalled(Exception):
    pass


class AlignError(Exception):
    """Engine returned a different number of lines than we sent."""


class EngineError(Exception):
    def __init__(self, msg, rate=False, soft=False, item_issue=False, cooldown=None):
        super().__init__(msg)
        self.rate = rate
        self.soft = soft
        self.item_issue = item_issue
        self.cooldown = cooldown


class HttpError(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


class TelegramError(Exception):
    def __init__(self, code, description, retry_after=None):
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description
        self.retry_after = retry_after


# --------------------------------------------------------------------------
# Small HTTP layer (keep-alive, one connection per thread and host)
# --------------------------------------------------------------------------
_SSL_CTX = ssl.create_default_context()
_tls = threading.local()


def _conn_for(scheme, host, port, timeout):
    cache = getattr(_tls, "conns", None)
    if cache is None:
        cache = _tls.conns = {}
    key = (scheme, host, port, timeout)
    conn = cache.get(key)
    if conn is None:
        if scheme == "https":
            conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=_SSL_CTX)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=timeout)
        cache[key] = conn
    return conn


def _drop_conn(scheme, host, port, timeout):
    cache = getattr(_tls, "conns", None)
    if not cache:
        return
    conn = cache.pop((scheme, host, port, timeout), None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def close_thread_connections():
    cache = getattr(_tls, "conns", None)
    if cache:
        for conn in list(cache.values()):
            try:
                conn.close()
            except Exception:
                pass
        cache.clear()


def http_request(method, url, headers=None, body=None, timeout=25.0, redirects=3):
    """Returns (status, headers_lowercase, body_bytes). Raises HttpError."""
    for _ in range(redirects + 1):
        sp = urllib.parse.urlsplit(url)
        scheme = (sp.scheme or "").lower()
        if scheme not in ("http", "https") or not sp.hostname:
            raise HttpError("bad url")
        port = sp.port or (443 if scheme == "https" else 80)
        path = sp.path or "/"
        if sp.query:
            path += "?" + sp.query
        hdrs = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip"}
        if headers:
            hdrs.update(headers)
        if isinstance(body, str):
            body = body.encode("utf-8")
        status = None
        rh = {}
        data = b""
        for attempt in range(2):
            conn = _conn_for(scheme, sp.hostname, port, timeout)
            try:
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                rh = {k.lower(): v for k, v in resp.getheaders()}
                if resp.will_close:
                    _drop_conn(scheme, sp.hostname, port, timeout)
                break
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError,
                    http.client.CannotSendRequest, http.client.ResponseNotReady,
                    http.client.CannotSendHeader) as exc:
                _drop_conn(scheme, sp.hostname, port, timeout)
                if attempt == 0:
                    continue
                raise HttpError(f"connection lost ({type(exc).__name__})")
            except (socket.timeout, TimeoutError):
                _drop_conn(scheme, sp.hostname, port, timeout)
                raise HttpError("timeout")
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                _drop_conn(scheme, sp.hostname, port, timeout)
                raise HttpError(f"{type(exc).__name__}: {exc}")
        if status is None:
            raise HttpError("no response")
        enc = rh.get("content-encoding", "").lower()
        try:
            if enc == "gzip":
                data = gzip.decompress(data)
            elif enc == "deflate":
                data = zlib.decompress(data)
        except (OSError, zlib.error, EOFError):
            raise HttpError("bad compressed body")
        if status in (301, 302, 303, 307, 308) and rh.get("location"):
            url = urllib.parse.urljoin(url, rh["location"])
            if status == 303:
                method, body = "GET", None
            continue
        return status, rh, data
    raise HttpError("too many redirects")


def http_download(url, dest, timeout=120.0, max_bytes=None):
    sp = urllib.parse.urlsplit(url)
    scheme = sp.scheme.lower()
    port = sp.port or (443 if scheme == "https" else 80)
    path = sp.path or "/"
    if sp.query:
        path += "?" + sp.query
    if scheme == "https":
        conn = http.client.HTTPSConnection(sp.hostname, port, timeout=timeout, context=_SSL_CTX)
    else:
        conn = http.client.HTTPConnection(sp.hostname, port, timeout=timeout)
    try:
        conn.request("GET", path, headers={"User-Agent": UA})
        resp = conn.getresponse()
        if resp.status != 200:
            resp.read()
            raise HttpError(f"HTTP {resp.status}", resp.status)
        total = 0
        with open(dest, "wb") as fh:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise HttpError("file too big")
                fh.write(chunk)
        return total
    except HttpError:
        raise
    except (socket.timeout, TimeoutError):
        raise HttpError("timeout")
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        raise HttpError(f"{type(exc).__name__}: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Text helpers
# --------------------------------------------------------------------------
WS_RE = re.compile(r"\s+")
NL_RE = re.compile(r"\r\n|\r|\n")
CJK_RE = re.compile("[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002fa1f]")
DEV_RE = re.compile("[\u0900-\u097f]")
DEV_WORD_RE = re.compile("[\u0900-\u097f]+")
LATIN_WORD_RE = re.compile(r"[A-Za-z]{2,}")
LATIN_LETTER_RE = re.compile(r"[A-Za-z]")
BAD_XML_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]")
SENT_SPLIT_RE = re.compile(r"(?<=[。！？!?；;…])\s*|(?<=[.])\s+")


def split_core(s):
    core = s.strip()
    if not core:
        return s, "", ""
    lead = s[: len(s) - len(s.lstrip())]
    trail = s[len(s.rstrip()):]
    return lead, core, trail


def needs_translation(core):
    letters = 0
    for ch in core:
        if ch.isalpha():
            letters += 1
    if letters == 0:
        return False
    dev = len(DEV_RE.findall(core))
    return dev < 0.5 * letters


def split_pieces(text, maxlen=PIECE_MAX):
    """Cut long text into pieces <= maxlen at sentence ends (or commas / spaces)."""
    if len(text) <= maxlen:
        return [text]
    sents = [x for x in SENT_SPLIT_RE.split(text) if x]
    pieces = []
    cur = ""
    for s in sents:
        while len(s) > maxlen:
            window = s[:maxlen]
            cut = max(window.rfind(" "), window.rfind("，"), window.rfind("、"),
                      window.rfind(","), window.rfind("："), window.rfind(":"))
            cut = cut + 1 if cut >= maxlen // 2 else maxlen
            head, s = s[:cut], s[cut:]
            if cur:
                pieces.append(cur)
                cur = ""
            pieces.append(head.strip() or head)
        if not s:
            continue
        if not cur:
            cur = s
        elif len(cur) + 1 + len(s) <= maxlen:
            cur = cur + " " + s
        else:
            pieces.append(cur)
            cur = s
    if cur:
        pieces.append(cur)
    return [p for p in pieces if p.strip()] or [text]


def clean_result(s):
    s = BAD_XML_RE.sub("", s)
    return WS_RE.sub(" ", s).strip()


def split_aligned(out, n):
    out = out.replace("\r\n", "\n").replace("\r", "\n")
    if n == 1:
        return [WS_RE.sub(" ", out).strip()]
    lines = out.split("\n")
    while len(lines) > n and not lines[-1].strip():
        lines.pop()
    if len(lines) != n:
        raise AlignError()
    return [ln.strip() for ln in lines]


def leftover_score(src, out):
    """0 = looks translated. Bigger = worse. Used to decide retries."""
    out = out.strip()
    if not out:
        return 1.0
    # PATCH: a short Latin name / word that comes back unchanged is fine
    # (names like "Ryan", "Chapter 1"). Do not retry these forever.
    s = src.strip()
    if (len(s) <= 20 and not CJK_RE.search(s)
            and len(LATIN_WORD_RE.findall(s)) <= 2
            and out.lower() == s.lower()):
        return 0.0
    score = 0.0
    s_cjk = len(CJK_RE.findall(src))
    o_cjk = len(CJK_RE.findall(out))
    if o_cjk and s_cjk:
        score = max(score, 0.5 + 0.5 * min(1.0, o_cjk / s_cjk))
    o_dev = len(DEV_RE.findall(out))
    if o_dev == 0:
        score = max(score, 0.45)
    else:
        lw = len(LATIN_WORD_RE.findall(out))
        if lw >= 4:
            dw = len(DEV_WORD_RE.findall(out))
            if lw > dw:
                score = max(score, 0.3 + 0.2 * lw / (lw + dw))
    return score


def final_flag(core):
    """Check on finished output. None = fine, 'kept' = short name/word kept,
    'problem' = looks untranslated."""
    if not any(ch.isalpha() for ch in core):
        return None
    if CJK_RE.search(core):
        return "problem"
    if not DEV_RE.search(core):
        words = len(LATIN_WORD_RE.findall(core))
        return "kept" if (len(core) <= 30 and words <= 4) else "problem"
    lw = len(LATIN_WORD_RE.findall(core))
    if lw >= 4 and lw > len(DEV_WORD_RE.findall(core)):
        return "problem"
    return None


def detect_source(texts, limit=400000):
    cjk = lat = seen = 0
    for t in texts:
        cjk += len(CJK_RE.findall(t))
        lat += len(LATIN_LETTER_RE.findall(t))
        seen += len(t)
        if seen > limit:
            break
    if cjk == 0 and lat == 0:
        return "auto"
    return "zh" if cjk * 4 >= lat else "en"


def decode_text(raw):
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", "replace")
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", "replace")
    head = raw[:2048].decode("ascii", "ignore")
    cands = []
    m = (re.search(r"encoding\s*=\s*[\"']([A-Za-z0-9_\-]+)", head)
         or re.search(r"charset\s*=\s*[\"']?([A-Za-z0-9_\-]+)", head, re.I))
    if m:
        cands.append(m.group(1))
    cands += ["utf-8", "gb18030", "big5", "cp1252"]
    fallback = None
    for enc in cands:
        try:
            text = raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
        if enc.lower() in ("gb18030", "gbk", "gb2312") and text:
            pua = sum(1 for ch in text if "\ue000" <= ch <= "\uf8ff")
            if pua / len(text) > 0.002:      # looks like Big5 read as GB: try the next one
                if fallback is None:
                    fallback = text
                continue
        return text
    return fallback if fallback is not None else raw.decode("utf-8", "replace")


def fix_charset(text):
    text = re.sub(r"(<\?xml[^>]*?encoding\s*=\s*[\"'])[^\"']+([\"'])", r"\1utf-8\2",
                  text, count=1, flags=re.I)
    text = re.sub(r"(<meta[^>]*?charset\s*=\s*[\"']?)[A-Za-z0-9_\-]+", r"\1utf-8",
                  text, flags=re.I)
    return text


def fmt_t(sec):
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02}:{s:02}" if h else f"{m:02}:{s:02}"


def fmt_n(n):
    return f"{int(n):,}"


def progress_bar(p, n=20):
    p = min(1.0, max(0.0, p))
    filled = int(p * n + 1e-9)
    return "█" * filled + "░" * (n - filled)


# --------------------------------------------------------------------------
# Segment table and fragments (shared by EPUB and TXT)
# --------------------------------------------------------------------------
class SegTable:
    def __init__(self):
        self.index = {}
        self.texts = []

    def add(self, text):
        i = self.index.get(text)
        if i is None:
            i = len(self.texts)
            self.index[text] = i
            self.texts.append(text)
        return i


class Frag:
    __slots__ = ("lead", "trail", "idxs", "esc")

    def __init__(self, lead, trail, idxs, esc):
        self.lead = lead
        self.trail = trail
        self.idxs = idxs
        self.esc = esc

    def render(self, results):
        s = self.lead + " ".join(results[i] for i in self.idxs) + self.trail
        return html.escape(s, quote=False) if self.esc else s


def render_parts(parts, results):
    return "".join(p if isinstance(p, str) else p.render(results) for p in parts)


def make_translate_handler(table, esc=True):
    def handler(raw):
        plain = html.unescape(raw) if (esc and "&" in raw) else raw
        lead, core, trail = split_core(plain)
        if not core or not needs_translation(core):
            return raw
        core = WS_RE.sub(" ", core)
        idxs = [table.add(p) for p in split_pieces(core)]
        return Frag(lead, trail, idxs, esc)
    return handler


def make_check_handler(acc, esc=True):
    def handler(raw):
        plain = html.unescape(raw) if (esc and "&" in raw) else raw
        core = plain.strip()
        if core:
            acc.append(core)
        return raw
    return handler


# --------------------------------------------------------------------------
# HTML / XHTML tokenizer that keeps every byte of markup untouched
# --------------------------------------------------------------------------
TAG_RE = re.compile(
    r"<!--.*?-->|<!\[CDATA\[.*?\]\]>|<\?.*?\?>|<![^>]*>"
    r"|</?[A-Za-z][^\s/>]*(?:[^>\"']|\"[^\"]*\"|'[^']*')*>", re.S)
TAG_NAME_RE = re.compile(r"<(/?)([A-Za-z][^\s/>]*)")
LANG_ATTR_RE = re.compile(r"(\blang\s*=\s*)([\"'])[^\"']*\2", re.I)


def parse_markup(s, handler):
    parts = []
    pos = 0
    pre_depth = 0

    def text_part(text):
        if not text:
            return
        if pre_depth:
            lines = text.split("\n")
            for i, line in enumerate(lines):
                parts.append(handler(line) if line else "")
                if i < len(lines) - 1:
                    parts.append("\n")
        else:
            parts.append(handler(text))

    while True:
        m = TAG_RE.search(s, pos)
        if m is None:
            break
        text_part(s[pos:m.start()])
        tag = m.group()
        pos = m.end()
        nm = TAG_NAME_RE.match(tag)
        closing = False
        local = ""
        if nm:
            closing = nm.group(1) == "/"
            local = nm.group(2).lower().split(":")[-1]
        if local == "html" and not closing:
            tag = LANG_ATTR_RE.sub(r'\1"hi"', tag)
        parts.append(tag)
        if not nm:
            continue
        selfclose = tag.endswith("/>")
        if local in ("script", "style") and not closing and not selfclose:
            em = re.compile(r"</\s*" + local + r"\s*>", re.I).search(s, pos)
            end = em.start() if em else len(s)
            if end > pos:
                parts.append(s[pos:end])
            pos = end
        elif local == "pre" and not selfclose:
            pre_depth = max(0, pre_depth + (-1 if closing else 1))
    text_part(s[pos:])
    return parts


NCX_RE = re.compile(r"(<(?:\w+:)?text\b[^>]*>)([^<]*)(</(?:\w+:)?text>)", re.I)
OPF_RE = re.compile(
    r"(<(?:\w+:)?(?:title|description)\b[^>]*>)([^<]*)(</(?:\w+:)?(?:title|description)>)", re.I)


def parse_regex_doc(text, pattern, handler):
    parts = []
    pos = 0
    for m in pattern.finditer(text):
        parts.append(text[pos:m.start(2)])
        parts.append(handler(m.group(2)))
        pos = m.end(2)
    parts.append(text[pos:])
    return parts


# --------------------------------------------------------------------------
# EPUB and TXT processors
# --------------------------------------------------------------------------
ITEM_RE = re.compile(r"<(?:\w+:)?item\b([^>]*)>", re.I)
ITEMREF_RE = re.compile(r"<(?:\w+:)?itemref\b([^>]*)>", re.I)
ATTR_RE = re.compile(r"([\w:.-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)')")
HTML_MEDIA = ("application/xhtml+xml", "text/html")
NCX_MEDIA = ("application/x-dtbncx+xml",)


def _attrs(blob):
    return {k.lower(): (a if a is not None and a != "" else b or "")
            for k, a, b in ATTR_RE.findall(blob)}


class Doc:
    __slots__ = ("name", "kind", "parts")

    def __init__(self, name, kind, parts):
        self.name = name
        self.kind = kind
        self.parts = parts


class EpubProcessor:
    kind = "epub"

    def __init__(self, path):
        self.path = path
        self.table = SegTable()
        self.docs = []
        self.units = 0

    # ---- reading ----
    def load(self):
        try:
            zf = zipfile.ZipFile(self.path)
        except (zipfile.BadZipFile, OSError):
            raise UserError("This file is not a valid EPUB (the ZIP inside is broken).")
        with zf:
            infos = zf.infolist()
            if len(infos) > ZIP_MAX_ENTRIES:
                raise UserError("This EPUB has too many files inside.")
            if sum(i.file_size for i in infos) > ZIP_MAX_UNPACKED:
                raise UserError("This EPUB is too big once unpacked.")
            names = [i.filename for i in infos]
            self._check_drm(zf, names)
            html_names, ncx_names, opf_name = self._content_files(zf, names)
            handler = make_translate_handler(self.table, True)
            for name in html_names:
                text = fix_charset(decode_text(self._read(zf, name)))
                self.docs.append(Doc(name, "html", parse_markup(text, handler)))
            for name in ncx_names:
                text = fix_charset(decode_text(self._read(zf, name)))
                self.docs.append(Doc(name, "ncx", parse_regex_doc(text, NCX_RE, handler)))
            if opf_name:
                text = fix_charset(decode_text(self._read(zf, opf_name)))
                self.docs.append(Doc(opf_name, "opf", parse_regex_doc(text, OPF_RE, handler)))
        if not self.table.texts:
            raise UserError("I could not find any text to translate in this EPUB "
                            "(it may already be Hindi, or it only has images).")

    @staticmethod
    def _read(zf, name):
        try:
            return zf.read(name)
        except (zipfile.BadZipFile, KeyError, RuntimeError, OSError, zlib.error, EOFError):
            raise UserError(f"This EPUB is damaged (cannot read {name}).")

    @staticmethod
    def _check_drm(zf, names):
        if "META-INF/encryption.xml" not in names:
            return
        try:
            txt = decode_text(zf.read("META-INF/encryption.xml"))
        except Exception:
            return
        ok = ("http://www.idpf.org/2008/embedding", "http://ns.adobe.com/pdf/enc#RC")
        algos = re.findall(r"<(?:\w+:)?EncryptionMethod\b[^>]*Algorithm\s*=\s*[\"']([^\"']+)", txt)
        if any(a not in ok for a in algos):
            raise UserError("This EPUB is DRM-protected, so I cannot translate it.")

    def _content_files(self, zf, names):
        lookup = {n: n for n in names}
        folded = {n.casefold(): n for n in names}

        def resolve(base_dir, href):
            href = urllib.parse.unquote(html.unescape(href.split("#")[0]))
            if not href:
                return None
            full = posixpath.normpath(posixpath.join(base_dir, href))
            return lookup.get(full) or folded.get(full.casefold())

        opf_name = None
        if "META-INF/container.xml" in lookup:
            try:
                cont = decode_text(zf.read("META-INF/container.xml"))
                m = re.search(r"full-path\s*=\s*[\"']([^\"']+)", cont)
                if m:
                    opf_name = resolve("", m.group(1))
            except Exception:
                opf_name = None
        if not opf_name:
            opf_name = next((n for n in names if n.lower().endswith(".opf")), None)

        html_names, ncx_names = [], []
        if opf_name:
            try:
                opf = decode_text(zf.read(opf_name))
            except Exception:
                opf = ""
            base = posixpath.dirname(opf_name)
            manifest = {}
            order = []
            for blob in ITEM_RE.findall(opf):
                a = _attrs(blob)
                if a.get("id") and a.get("href"):
                    manifest[a["id"]] = (a["href"], a.get("media-type", "").lower())
                    order.append(a["id"])
            spine = [_attrs(b).get("idref") for b in ITEMREF_RE.findall(opf)]
            seen = set()
            for iid in [i for i in spine if i in manifest] + order:
                if iid in seen:
                    continue
                seen.add(iid)
                href, media = manifest[iid]
                target = resolve(base, href)
                if not target:
                    continue
                if media in HTML_MEDIA and target not in html_names:
                    html_names.append(target)
                elif media in NCX_MEDIA and target not in ncx_names:
                    ncx_names.append(target)
        if not html_names:
            html_names = sorted(n for n in names
                                if n.lower().endswith((".xhtml", ".html", ".htm"))
                                and not n.startswith("META-INF/"))
        if not ncx_names:
            ncx_names = [n for n in names if n.lower().endswith(".ncx")]
        return html_names, ncx_names, opf_name

    # ---- writing ----
    def build(self, results, out_path):
        replaced = {}
        for doc in self.docs:
            text = render_parts(doc.parts, results)
            if doc.kind == "opf":
                text = re.sub(r"(<(?:\w+:)?language\b[^>]*>)[^<]*(</(?:\w+:)?language>)",
                              r"\1hi\2", text, flags=re.I)
            replaced[doc.name] = text.encode("utf-8")
        try:
            with zipfile.ZipFile(self.path) as zin, \
                    zipfile.ZipFile(out_path, "w", allowZip64=True) as zout:
                infos = sorted(zin.infolist(), key=lambda i: 0 if i.filename == "mimetype" else 1)
                for info in infos:
                    data = replaced[info.filename] if info.filename in replaced else zin.read(info)
                    zi = zipfile.ZipInfo(info.filename, info.date_time)
                    zi.external_attr = info.external_attr
                    zi.compress_type = (zipfile.ZIP_STORED if info.filename == "mimetype"
                                        else zipfile.ZIP_DEFLATED)
                    zout.writestr(zi, data)
        except (zipfile.BadZipFile, OSError, zlib.error) as exc:
            raise UserError(f"Could not build the translated EPUB ({type(exc).__name__}).")
        if os.path.getsize(out_path) > TG_UPLOAD_LIMIT:
            raise UserError("The translated EPUB is bigger than Telegram's 50 MB bot upload limit.")

    def outputs(self, out_path, base_name):
        return [(out_path, f"{base_name}_hindi.epub")]

    # ---- final scan of the finished file ----
    def scan_output(self, out_path):
        acc = []
        try:
            with zipfile.ZipFile(out_path) as zf:
                if zf.testzip() is not None:
                    raise UserError("The translated EPUB failed its own integrity test.")
                handler = make_check_handler(acc, True)
                for doc in self.docs:
                    if doc.kind == "opf":
                        continue
                    text = zf.read(doc.name).decode("utf-8")
                    if doc.kind == "html":
                        parse_markup(text, handler)
                    else:
                        parse_regex_doc(text, NCX_RE, handler)
        except UserError:
            raise
        except (zipfile.BadZipFile, KeyError, UnicodeDecodeError, OSError, zlib.error) as exc:
            raise UserError(f"The translated EPUB did not pass the final check ({type(exc).__name__}).")
        return acc


class TxtProcessor:
    kind = "txt"

    def __init__(self, path):
        self.path = path
        self.table = SegTable()
        self.lines = []
        self.eol = "\n"

    def load(self):
        try:
            with open(self.path, "rb") as fh:
                raw = fh.read()
        except OSError:
            raise UserError("I could not read that text file.")
        text = decode_text(raw).lstrip("\ufeff")
        self.eol = "\r\n" if "\r\n" in text else "\n"
        handler = make_translate_handler(self.table, False)
        for line in NL_RE.split(text):
            self.lines.append(handler(line) if line.strip() else line)
        if not self.table.texts:
            raise UserError("I could not find any text to translate in this file "
                            "(it may already be Hindi).")

    def build(self, results, out_path):
        try:
            with open(out_path, "w", encoding="utf-8", newline="") as fh:
                fh.write(self.eol.join(render_parts([ln], results) for ln in self.lines))
        except OSError:
            raise UserError("Could not write the translated text file.")

    def outputs(self, out_path, base_name):
        size = os.path.getsize(out_path)
        if size <= TG_UPLOAD_LIMIT:
            return [(out_path, f"{base_name}_hindi.txt")]
        res = []
        part, written = 1, 0
        limit = 45 * 1000 * 1000
        cur = None
        folder = os.path.dirname(out_path)
        try:
            with open(out_path, "r", encoding="utf-8", newline="") as src:
                for line in src:
                    b = len(line.encode("utf-8"))
                    if cur is None or written + b > limit:
                        if cur:
                            cur.close()
                        p = os.path.join(folder, f"part{part}.txt")
                        cur = open(p, "w", encoding="utf-8", newline="")
                        res.append((p, f"{base_name}_hindi_part{part}.txt"))
                        part += 1
                        written = 0
                    cur.write(line)
                    written += b
        finally:
            if cur:
                cur.close()
        return res

    def scan_output(self, out_path):
        acc = []
        try:
            with open(out_path, "r", encoding="utf-8", newline="") as fh:
                for line in NL_RE.split(fh.read()):
                    core = line.strip()
                    if core:
                        acc.append(core)
        except (OSError, UnicodeDecodeError):
            raise UserError("The translated text file did not pass the final check.")
        return acc


# --------------------------------------------------------------------------
# Translate engines (all free, no key)
# --------------------------------------------------------------------------
class Engine:
    name = "engine"
    tier = 0
    rescue_only = False
    threads = 4
    max_items = 1
    base_cost = 4000
    timeout = 25.0

    def __init__(self):
        self._lock = threading.Lock()
        self.streak = 0
        self.disabled_until = 0.0
        self.item_limit = self.max_items
        self.misaligns = 0
        self.ok_batches = 0
        self.failures = 0
        self.last_ok = 0.0
        self.issue_streak = 0

    @property
    def max_cost(self):
        return self.base_cost

    def cost(self, text):
        return len(text)

    def available(self):
        return time.time() >= self.disabled_until

    def on_ok(self):
        with self._lock:
            self.streak = 0
            self.issue_streak = 0
            self.last_ok = time.time()
            self.ok_batches += 1
            if (self.misaligns < 3 and self.item_limit < self.max_items
                    and self.ok_batches % 20 == 0):
                self.item_limit += 1

    def on_fail(self, err):
        with self._lock:
            self.failures += 1
            if err.soft:
                return
            if err.item_issue:
                # a bad line, not a bad engine - unless it keeps happening
                self.issue_streak += 1
                if self.issue_streak < 5:
                    return
            self.streak += 1
            base = 30.0 if err.rate else 3.0
            cd = err.cooldown or min(900.0, base * (2 ** min(self.streak - 1, 6)))
            self.disabled_until = time.time() + cd

    def on_misalign(self):
        with self._lock:
            self.misaligns += 1
            self.item_limit = max(1, self.item_limit // 2)

    def shrink(self):
        with self._lock:
            self.item_limit = max(1, self.item_limit // 2)

    def translate(self, texts, sl):
        raise NotImplementedError

    # helpers
    @staticmethod
    def check_status(status, data, name=""):
        if status == 200:
            return
        low = data[:400].decode("utf-8", "ignore").lower()
        if status in (429, 403, 503) or "captcha" in low or "unusual traffic" in low:
            raise EngineError(f"{name} HTTP {status} (rate limited)", rate=True)
        if status in (400, 413, 414, 422):
            raise EngineError(f"{name} HTTP {status}", item_issue=True)
        raise EngineError(f"{name} HTTP {status}")

    @staticmethod
    def http(method, url, headers=None, body=None, timeout=25.0):
        try:
            return http_request(method, url, headers, body, timeout)
        except HttpError as exc:
            raise EngineError(f"network: {exc}")


class GoogleEngine(Engine):
    tier = 0
    threads = 10
    max_items = 12

    def __init__(self, name="google", client="gtx", base="https://translate.googleapis.com",
                 threads=None):
        super().__init__()
        self.name = name
        self.client = client
        self.base = base.rstrip("/")
        if threads:
            self.threads = threads
        self.use_post = True

    @property
    def max_cost(self):
        return 4500 if self.use_post else 1800

    def cost(self, text):
        return len(text) if self.use_post else len(urllib.parse.quote(text, safe=""))

    def translate(self, texts, sl):
        code = {"zh": "zh-CN", "en": "en"}.get(sl, "auto")
        q = "\n".join(texts)
        url = (f"{self.base}/translate_a/single?client={self.client}&sl={code}"
               f"&tl={TARGET}&dt=t&ie=UTF-8&oe=UTF-8")
        if self.use_post:
            status, _, data = self.http(
                "POST", url, {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
                "q=" + urllib.parse.quote(q, safe=""), self.timeout)
            if status in (400, 404, 405, 411, 413, 501):
                self.use_post = False
                raise EngineError(f"{self.name}: POST not accepted, using GET", soft=True)
        else:
            status, _, data = self.http("GET", url + "&q=" + urllib.parse.quote(q, safe=""),
                                        None, None, self.timeout)
        self.check_status(status, data, self.name)
        try:
            j = json.loads(data.decode("utf-8", "replace"))
            out = "".join(seg[0] for seg in j[0] if seg and isinstance(seg[0], str))
        except (ValueError, IndexError, TypeError, KeyError):
            raise EngineError(f"{self.name}: bad response")
        return split_aligned(out, len(texts))


class GoogleMobileEngine(Engine):
    name = "google-mobile"
    tier = 0
    threads = 4
    max_items = 6
    base_cost = 2400
    RES1 = re.compile(r"<div[^>]*class=[\"']result-container[\"'][^>]*>(.*?)</div>", re.S)
    RES2 = re.compile(r"<div[^>]*class=[\"']t0[\"'][^>]*>(.*?)</div>", re.S)

    def __init__(self, base="https://translate.google.com"):
        super().__init__()
        self.base = base.rstrip("/")

    def cost(self, text):
        return len(urllib.parse.quote(text, safe="")) + 3

    def translate(self, texts, sl):
        code = {"zh": "zh-CN", "en": "en"}.get(sl, "auto")
        q = "\n".join(texts)
        url = (f"{self.base}/m?sl={code}&tl={TARGET}&hl=en&q=" + urllib.parse.quote(q, safe=""))
        status, _, data = self.http("GET", url, {"Accept-Language": "en"}, None, self.timeout)
        self.check_status(status, data, self.name)
        page = data.decode("utf-8", "replace")
        m = self.RES1.search(page) or self.RES2.search(page)
        if not m:
            raise EngineError(f"{self.name}: result not found")
        out = re.sub(r"<br\s*/?>", "\n", m.group(1), flags=re.I)
        out = html.unescape(re.sub(r"<[^>]+>", "", out))
        return split_aligned(out, len(texts))


class BingEngine(Engine):
    name = "bing"
    tier = 0
    threads = 8
    max_items = 40
    base_cost = 4800

    def __init__(self, auth_url="https://edge.microsoft.com/translate/auth",
                 api_url="https://api.cognitive.microsofttranslator.com/translate"):
        super().__init__()
        self.auth_url = auth_url
        self.api_url = api_url
        self._tok = None
        self._tok_exp = 0.0
        self._tlock = threading.Lock()

    def _token(self, force=False):
        with self._tlock:
            if force or not self._tok or time.time() > self._tok_exp:
                status, _, data = self.http("GET", self.auth_url, None, None, 15.0)
                tok = data.decode("utf-8", "ignore").strip()
                if status != 200 or len(tok) < 20 or " " in tok or "<" in tok:
                    raise EngineError("bing: no token", rate=(status in (429, 403)))
                self._tok = tok
                self._tok_exp = time.time() + 480
            return self._tok

    def translate(self, texts, sl):
        code = {"zh": "zh-Hans", "en": "en"}.get(sl)
        qs = f"api-version=3.0&to={TARGET}&textType=plain" + (f"&from={code}" if code else "")
        body = json.dumps([{"Text": t} for t in texts], ensure_ascii=False)
        status, data = 0, b""
        for attempt in range(2):
            tok = self._token(force=(attempt == 1))
            status, _, data = self.http(
                "POST", f"{self.api_url}?{qs}",
                {"Authorization": "Bearer " + tok, "Content-Type": "application/json; charset=UTF-8"},
                body, self.timeout)
            if status == 401 and attempt == 0:
                continue
            break
        self.check_status(status, data, self.name)
        try:
            j = json.loads(data.decode("utf-8", "replace"))
            outs = [it["translations"][0]["text"] for it in j]
        except (ValueError, IndexError, TypeError, KeyError):
            raise EngineError("bing: bad response")
        if len(outs) != len(texts):
            raise AlignError()
        return [WS_RE.sub(" ", o).strip() for o in outs]


class LingvaEngine(Engine):
    tier = 1
    threads = 2
    max_items = 4
    base_cost = 2000
    timeout = 15.0

    def __init__(self, base):
        super().__init__()
        self.base = base.rstrip("/")
        host = urllib.parse.urlsplit(self.base).hostname or self.base
        self.name = f"lingva:{host}"

    def cost(self, text):
        return len(urllib.parse.quote(text, safe="")) + 3

    def translate(self, texts, sl):
        code = {"zh": "zh", "en": "en"}.get(sl, "auto")
        q = "\n".join(texts)
        url = f"{self.base}/api/v1/{code}/{TARGET}/" + urllib.parse.quote(q, safe="")
        status, _, data = self.http("GET", url, None, None, self.timeout)
        self.check_status(status, data, self.name)
        try:
            out = json.loads(data.decode("utf-8", "replace"))["translation"]
            if not isinstance(out, str):
                raise TypeError
        except (ValueError, KeyError, TypeError):
            raise EngineError(f"{self.name}: bad response")
        return split_aligned(out, len(texts))


class YandexEngine(Engine):
    name = "yandex"
    tier = 1
    threads = 3
    max_items = 10
    base_cost = 4000

    def __init__(self, base="https://translate.yandex.net"):
        super().__init__()
        self.base = base.rstrip("/")

    def translate(self, texts, sl):
        lang = {"zh": "zh-hi", "en": "en-hi"}.get(sl, "hi")
        url = f"{self.base}/api/v1/tr.json/translate?id={uuid.uuid4().hex}-0-0&srv=android"
        body = "&".join(["lang=" + lang, "format=plain"] +
                        ["text=" + urllib.parse.quote(t, safe="") for t in texts])
        status, _, data = self.http("POST", url,
                                    {"Content-Type": "application/x-www-form-urlencoded"},
                                    body, self.timeout)
        self.check_status(status, data, self.name)
        try:
            j = json.loads(data.decode("utf-8", "replace"))
            outs = j["text"]
            if not isinstance(outs, list) or not all(isinstance(o, str) for o in outs):
                raise TypeError
        except (ValueError, KeyError, TypeError):
            raise EngineError("yandex: bad response")
        if len(outs) != len(texts):
            raise AlignError()
        return [WS_RE.sub(" ", o).strip() for o in outs]


class MyMemoryEngine(Engine):
    name = "mymemory"
    tier = 2
    rescue_only = True
    threads = 1
    max_items = 1
    base_cost = 450
    timeout = 15.0

    def __init__(self, email="", base="https://api.mymemory.translated.net"):
        super().__init__()
        self.email = email
        self.base = base.rstrip("/")

    def cost(self, text):
        return len(text.encode("utf-8"))

    def translate(self, texts, sl):
        code = {"zh": "zh-CN", "en": "en-GB"}.get(sl, "Autodetect")
        url = (f"{self.base}/get?q=" + urllib.parse.quote(texts[0], safe="")
               + f"&langpair={urllib.parse.quote(code + '|hi-IN', safe='')}")
        if self.email:
            url += "&de=" + urllib.parse.quote(self.email, safe="")
        status, _, data = self.http("GET", url, None, None, self.timeout)
        self.check_status(status, data, self.name)
        try:
            j = json.loads(data.decode("utf-8", "replace"))
            text = j["responseData"]["translatedText"]
            code_ = int(j.get("responseStatus", 200))
        except (ValueError, KeyError, TypeError):
            raise EngineError("mymemory: bad response")
        if code_ == 429 or "MYMEMORY WARNING" in str(text).upper():
            raise EngineError("mymemory: daily limit reached", rate=True, cooldown=3600)
        if code_ != 200 or not isinstance(text, str):
            raise EngineError(f"mymemory: status {code_}")
        return [WS_RE.sub(" ", html.unescape(text)).strip()]


class LibreEngine(Engine):
    tier = 2
    rescue_only = True
    threads = 2
    max_items = 5
    base_cost = 1500
    timeout = 20.0

    def __init__(self, base, api_key=""):
        super().__init__()
        self.base = base.rstrip("/")
        self.api_key = api_key
        host = urllib.parse.urlsplit(self.base).hostname or self.base
        self.name = f"libre:{host}"

    def translate(self, texts, sl):
        payload = {"q": "\n".join(texts), "source": {"zh": "zh", "en": "en"}.get(sl, "auto"),
                   "target": TARGET, "format": "text"}
        if self.api_key:
            payload["api_key"] = self.api_key
        status, _, data = self.http("POST", f"{self.base}/translate",
                                    {"Content-Type": "application/json"},
                                    json.dumps(payload), self.timeout)
        if status in (400, 401, 403):
            low = data[:300].decode("utf-8", "ignore").lower()
            if "key" in low:
                raise EngineError(f"{self.name}: needs an API key", cooldown=3600)
        self.check_status(status, data, self.name)
        try:
            out = json.loads(data.decode("utf-8", "replace"))["translatedText"]
            if not isinstance(out, str):
                raise TypeError
        except (ValueError, KeyError, TypeError):
            raise EngineError(f"{self.name}: bad response")
        return split_aligned(out, len(texts))


def build_engines(env=None):
    env = os.environ if env is None else env
    engines = [
        GoogleEngine("google", "gtx"),
        GoogleEngine("google-chrome", "dict-chrome-ex", threads=6),
        BingEngine(),
        GoogleMobileEngine(),
        YandexEngine(),
    ]
    lingva = env.get("LINGVA_URLS") or ("https://lingva.ml,https://lingva.lunar.icu,"
                                        "https://translate.plausibility.cloud,"
                                        "https://lingva.garudalinux.org")
    for base in [u.strip() for u in lingva.split(",") if u.strip()]:
        engines.append(LingvaEngine(base))
    engines.append(MyMemoryEngine(env.get("MYMEMORY_EMAIL", "").strip()))
    libre = env.get("LIBRETRANSLATE_URLS") or ("https://translate.argosopentech.com,"
                                               "https://lt.vern.cc")
    key = env.get("LIBRETRANSLATE_API_KEY", "").strip()
    for base in [u.strip() for u in libre.split(",") if u.strip()]:
        engines.append(LibreEngine(base, key))
    off = {x.strip().lower() for x in env.get("DISABLE_ENGINES", "").split(",") if x.strip()}
    return [e for e in engines if e.name.lower() not in off
            and e.name.split(":")[0].lower() not in off]


# --------------------------------------------------------------------------
# The translation run: shared queue, one pool of threads per engine
# --------------------------------------------------------------------------
class Item:
    __slots__ = ("idx", "text", "tried", "attempts", "hard_fails", "fails_by",
                 "best", "best_score", "result", "done", "solo", "bad", "batch_fails")

    def __init__(self, idx, text):
        self.idx = idx
        self.text = text
        self.tried = None
        self.attempts = 0
        self.hard_fails = 0
        self.fails_by = None
        self.best = None
        self.best_score = 9.0
        self.result = None
        self.done = False
        self.solo = False
        self.bad = False
        self.batch_fails = 0


class Run:
    def __init__(self, texts, engines, sl, cancel_event, max_tries=MAX_TRIES, stall=None):
        self.items = [Item(i, t) for i, t in enumerate(texts)]
        self.engines = engines
        self.sl = sl
        self.cancel = cancel_event
        self.max_tries = max(1, min(max_tries, len(engines)))
        self.stall = STALL_SECONDS if stall is None else stall
        self.all_names = {e.name for e in engines}
        self.fresh = collections.deque(range(len(self.items)))
        self.retry = collections.deque()
        self.lock = threading.Lock()
        self.finished = threading.Event()
        self.stop = threading.Event()
        self.total_chars = sum(len(t) for t in texts) or 1
        self.done_chars = 0
        self.done_count = 0
        self.retried = 0
        self.engine_chars = collections.Counter()
        self.last_progress = time.time()
        if not self.items:
            self.finished.set()

    # ----- queue handling -----
    def _better_available(self):
        return any(e.available() and not e.rescue_only for e in self.engines)

    def _eligible(self, it, engine):
        if it.tried and engine.name in it.tried:
            return False
        return True

    def _fill(self, dq, engine, batch, state):
        scanned = 0
        skipped = []
        while dq and scanned < SCAN_LIMIT and len(batch) < engine.item_limit:
            idx = dq.popleft()
            scanned += 1
            it = self.items[idx]
            if it.done:
                continue
            c = engine.cost(it.text)
            if (not self._eligible(it, engine) or c > engine.max_cost
                    or (it.solo and batch)
                    or (batch and state[0] + c + 1 > engine.max_cost)):
                skipped.append(idx)
                continue
            batch.append(idx)
            state[0] += c + 1
            if it.solo:
                break
        for idx in reversed(skipped):
            dq.appendleft(idx)

    def take_batch(self, engine):
        with self.lock:
            batch = []
            state = [0]
            self._fill(self.retry, engine, batch, state)
            if not batch or not self.items[batch[0]].solo:
                if (not engine.rescue_only) or not self._better_available():
                    self._fill(self.fresh, engine, batch, state)
            return batch

    def release(self, idxs, solo=False):
        with self.lock:
            for i in idxs:
                if solo:
                    self.items[i].solo = True
            self.retry.extendleft(reversed(idxs))

    def _finalize(self, it, result, engine_name=None):
        # lock held
        if it.done:
            return
        it.done = True
        it.result = result
        self.done_chars += len(it.text)
        self.done_count += 1
        self.last_progress = time.time()
        if engine_name:
            self.engine_chars[engine_name] += len(it.text)
        if self.done_count >= len(self.items):
            self.finished.set()

    def finish_batch(self, engine, idxs, outs):
        with self.lock:
            for idx, out in zip(idxs, outs):
                it = self.items[idx]
                if it.done:
                    continue
                out = clean_result(out)
                score = leftover_score(it.text, out)
                if score == 0.0:
                    self._finalize(it, out, engine.name)
                    continue
                if it.attempts == 0:
                    self.retried += 1
                it.attempts += 1
                if it.tried is None:
                    it.tried = set()
                it.tried.add(engine.name)
                if out and (it.best is None or score < it.best_score):
                    it.best, it.best_score = out, score
                if len(it.tried) >= self.max_tries or self.all_names <= it.tried:
                    it.bad = True
                    self._finalize(it, it.best if it.best is not None else it.text, engine.name)
                else:
                    self.retry.append(idx)
            self.last_progress = time.time()

    def batch_failed(self, engine, idxs, err):
        """A request failed. Lines go back in the queue. A line is only given up on
        when it keeps failing on engines that are otherwise working (a 'poison' line);
        an outage just waits (and ends in Stalled with a clean message)."""
        healthy = (time.time() - engine.last_ok) < 60
        if len(idxs) > 1:
            if err.item_issue:
                engine.shrink()
            if err.item_issue or healthy:
                with self.lock:
                    for i in idxs:
                        it = self.items[i]
                        it.batch_fails += 1
                        if it.batch_fails >= 2:
                            it.solo = True       # test it alone so one bad line cannot block others
            self.release(idxs)
            return
        with self.lock:
            it = self.items[idxs[0]]
            if it.done:
                return
            if err.item_issue or healthy:
                it.hard_fails += 1
                if it.fails_by is None:
                    it.fails_by = collections.Counter()
                it.fails_by[engine.name] += 1
                if it.fails_by[engine.name] >= 2 or err.item_issue:
                    if it.tried is None:
                        it.tried = set()
                    it.tried.add(engine.name)
            it.solo = True
            if it.hard_fails >= 4 or self.all_names <= (it.tried or set()):
                it.bad = True
                self._finalize(it, it.best if it.best is not None else it.text)
            else:
                self.retry.appendleft(idxs[0])

    # ----- worker -----
    def _worker(self, engine):
        try:
            while not self.stop.is_set():
                if not engine.available():
                    time.sleep(0.25)
                    continue
                idxs = self.take_batch(engine)
                if not idxs:
                    time.sleep(0.15)
                    continue
                texts = [self.items[i].text for i in idxs]
                try:
                    outs = engine.translate(texts, self.sl)
                    if len(outs) != len(texts):
                        raise AlignError()
                except AlignError:
                    if len(idxs) > 1:
                        engine.on_misalign()
                        self.release(idxs)
                    else:
                        err = EngineError(f"{engine.name}: count mismatch")
                        engine.on_fail(err)
                        self.batch_failed(engine, idxs, err)
                    continue
                except EngineError as err:
                    engine.on_fail(err)
                    self.batch_failed(engine, idxs, err)
                    continue
                except Exception as exc:  # a bug must never kill a worker
                    log.debug("engine %s crashed: %s", engine.name, traceback.format_exc())
                    err = EngineError(f"{engine.name}: unexpected {type(exc).__name__}")
                    engine.on_fail(err)
                    self.batch_failed(engine, idxs, err)
                    continue
                engine.on_ok()
                self.finish_batch(engine, idxs, outs)
        finally:
            close_thread_connections()

    def execute(self):
        if not self.items:
            return
        self.last_progress = time.time()
        for engine in self.engines:
            for k in range(engine.threads):
                threading.Thread(target=self._worker, args=(engine,), daemon=True,
                                 name=f"w-{engine.name}-{k}").start()
        try:
            while not self.finished.wait(0.4):
                if self.cancel.is_set() or STOP.is_set():
                    raise Cancelled()
                if time.time() - self.last_progress > self.stall:
                    raise Stalled()
        finally:
            self.stop.set()

    # PATCH: give up on unfinished lines instead of throwing the whole job away.
    def salvage(self):
        """Mark every unfinished line as 'bad' and keep its best (or original) text.
        Returns how many lines were given up on."""
        n = 0
        with self.lock:
            for it in self.items:
                if not it.done:
                    it.bad = True
                    self._finalize(it, it.best if it.best is not None else it.text)
                    n += 1
        return n

    def results(self):
        return [(it.result if it.result is not None else it.text) for it in self.items]

    def bad_indexes(self):
        return [it.idx for it in self.items if it.bad]


def rescue_pass(run, engines, sl, cancel_event, stall=None):
    """Retry still-bad lines by cutting them into short pieces."""
    bad = run.bad_indexes()
    plan = {}
    table = SegTable()
    for idx in bad:
        text = run.items[idx].text
        pieces = split_pieces(text, 60)
        if len(pieces) < 2:
            continue
        plan[idx] = [table.add(p) for p in pieces]
    if not plan:
        return 0
    sub = Run(table.texts, engines, sl, cancel_event, max_tries=MAX_TRIES,
              stall=60.0 if stall is None else stall)
    try:
        sub.execute()
    except Stalled:
        pass                      # keep whatever pieces did get translated
    sub_res = sub.results()
    fixed = 0
    for idx, ids in plan.items():
        it = run.items[idx]
        combined = clean_result(" ".join(sub_res[i] for i in ids))
        score = leftover_score(it.text, combined)
        if combined and score < it.best_score:
            it.result = combined
            it.best_score = score
            if score == 0.0:
                it.bad = False
                fixed += 1
    return fixed


# --------------------------------------------------------------------------
# Telegram client (Bot API over plain HTTPS)
# --------------------------------------------------------------------------
class TgClient:
    def __init__(self, token, base="https://api.telegram.org"):
        self.token = token
        self.base = base.rstrip("/")

    def url(self, method):
        return f"{self.base}/bot{self.token}/{method}"

    def call(self, method, params=None, timeout=30.0, retries=4):
        body = json.dumps(params or {}).encode("utf-8")
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                status, _, data = http_request("POST", self.url(method),
                                               {"Content-Type": "application/json"}, body, timeout)
            except HttpError as exc:
                if attempt >= retries:
                    raise TelegramError(0, f"network problem: {exc}")
                time.sleep(delay)
                delay = min(delay * 2, 15.0)
                continue
            try:
                j = json.loads(data.decode("utf-8", "replace"))
            except ValueError:
                j = {}
            if j.get("ok"):
                return j.get("result")
            code = j.get("error_code") or status
            desc = j.get("description") or f"HTTP {status}"
            ra = (j.get("parameters") or {}).get("retry_after")
            if code == 429 and attempt < retries:
                time.sleep(min((ra or 1) + 1, 60))
                continue
            if code >= 500 and attempt < retries:
                time.sleep(delay)
                delay = min(delay * 2, 15.0)
                continue
            raise TelegramError(code, desc, ra)
        raise TelegramError(0, "unreachable")

    def download_file(self, file_id, dest, max_bytes):
        info = self.call("getFile", {"file_id": file_id})
        fp = (info or {}).get("file_path")
        if not fp:
            raise TelegramError(400, "no file path")
        url = f"{self.base}/file/bot{self.token}/{fp.lstrip('/')}"
        last = None
        for attempt in range(3):
            try:
                return http_download(url, dest, timeout=120.0, max_bytes=max_bytes)
            except HttpError as exc:
                last = exc
                if "too big" in str(exc):
                    raise
                time.sleep(1 + attempt * 2)
        raise TelegramError(0, f"download failed: {last}")

    def send_document(self, chat_id, path, filename, caption="", retries=3):
        last = None
        for attempt in range(retries + 1):
            try:
                return self._send_document_once(chat_id, path, filename, caption)
            except TelegramError as exc:
                last = exc
                if exc.code == 429:
                    time.sleep(min((exc.retry_after or 2) + 1, 60))
                elif exc.code in (400, 401, 403, 404):
                    raise
                else:
                    time.sleep(2 + attempt * 3)
        raise last

    def _send_document_once(self, chat_id, path, filename, caption):
        boundary = "----hb" + uuid.uuid4().hex
        pre = b""
        for k, v in (("chat_id", str(chat_id)), ("caption", caption[:1000])):
            pre += (f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'
                    ).encode("utf-8")
        safe = filename.replace('"', "_").replace("\r", " ").replace("\n", " ")
        pre += (f'--{boundary}\r\nContent-Disposition: form-data; name="document"; '
                f'filename="{safe}"\r\nContent-Type: application/octet-stream\r\n\r\n'
                ).encode("utf-8")
        post = f"\r\n--{boundary}--\r\n".encode()
        size = os.path.getsize(path)
        total = len(pre) + size + len(post)

        def gen():
            yield pre
            with open(path, "rb") as fh:
                while True:
                    chunk = fh.read(262144)
                    if not chunk:
                        break
                    yield chunk
            yield post

        sp = urllib.parse.urlsplit(self.url("sendDocument"))
        port = sp.port or (443 if sp.scheme == "https" else 80)
        if sp.scheme == "https":
            conn = http.client.HTTPSConnection(sp.hostname, port, timeout=600, context=_SSL_CTX)
        else:
            conn = http.client.HTTPConnection(sp.hostname, port, timeout=600)
        try:
            conn.request("POST", sp.path, body=gen(), headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Content-Length": str(total), "User-Agent": UA})
            resp = conn.getresponse()
            data = resp.read()
        except (socket.timeout, TimeoutError):
            raise TelegramError(0, "upload timed out")
        except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
            raise TelegramError(0, f"upload failed: {type(exc).__name__}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        try:
            j = json.loads(data.decode("utf-8", "replace"))
        except ValueError:
            j = {}
        if j.get("ok"):
            return j.get("result")
        raise TelegramError(j.get("error_code") or resp.status, j.get("description") or "upload failed",
                            (j.get("parameters") or {}).get("retry_after"))


# --------------------------------------------------------------------------
# Jobs
# --------------------------------------------------------------------------
HELP_TEXT = (
    "📚 Hindi Translate Bot\n\n"
    "Send me a .epub or .txt file (up to 15 MB).\n"
    "I translate Chinese or English into Hindi and send the file back.\n\n"
    "• Live progress bar, time used and time left\n"
    "• Many free translate engines work together for speed\n"
    "• Every line is checked, leftovers are retried\n\n"
    "Just send the file. No settings needed."
)


class Job:
    def __init__(self, bot, chat_id, file_id, file_name, ext):
        self.bot = bot
        self.tg = bot.tg
        self.chat_id = chat_id
        self.file_id = file_id
        self.file_name = file_name
        self.ext = ext
        self.id = uuid.uuid4().hex[:10]
        self.cancel_ev = threading.Event()
        self.msg_id = None
        self.t0 = time.time()
        self.phase = "queued"
        self.run = None
        self.sl_label = ""
        self.samples = collections.deque()
        self.tr_start = None
        self.last_text = None
        self.next_edit = 0.0
        self.workdir = None
        self.done_flag = threading.Event()

    # ----- progress text -----
    def eta_rate(self):
        run = self.run
        if run is None:
            return None, 0.0
        now = time.time()
        done, total = run.done_chars, run.total_chars
        self.samples.append((now, done))
        while len(self.samples) > 2 and now - self.samples[0][0] > 40:
            self.samples.popleft()
        t0, d0 = self.samples[0]
        rate = None
        if now - t0 >= 4 and done > d0:
            rate = (done - d0) / (now - t0)
        if rate is None and done > 0 and self.tr_start:
            rate = done / max(0.001, now - self.tr_start)
        if not rate:
            return None, 0.0
        return (total - done) / rate, rate

    def render(self):
        el = time.time() - self.t0
        name = self.file_name if len(self.file_name) <= 40 else self.file_name[:37] + "..."
        lines = [f"📄 {name} → Hindi"]
        if self.sl_label:
            lines.append(self.sl_label)
        run = self.run
        if self.phase == "translate" and run is not None:
            p = run.done_chars / run.total_chars
            eta, rate = self.eta_rate()
            left = f"~{fmt_t(eta)}" if eta is not None else "calculating..."
            lines += ["", "🌐 Translating",
                      f"{progress_bar(p)} {p * 100:.1f}%",
                      f"⏱ Used {fmt_t(el)}  •  Left {left}"]
            if rate:
                lines.append(f"⚡ {fmt_n(rate)} chars/s")
            if run.retried:
                lines.append(f"🔁 Re-checked lines: {fmt_n(run.retried)}")
            top = run.engine_chars.most_common(3)
            tot = sum(run.engine_chars.values())
            if tot:
                lines.append("🔧 " + "  ".join(f"{n} {c * 100 // tot}%" for n, c in top))
        else:
            labels = {"queued": "⏳ Waiting in queue...", "download": "📥 Downloading...",
                      "read": "🔍 Reading the file...", "rescue": "🔎 Fixing leftover lines...",
                      "build": "📦 Building the Hindi file...", "check": "🧪 Final check...",
                      "send": "📤 Sending the file..."}
            lines += ["", labels.get(self.phase, "Working..."), f"⏱ Used {fmt_t(el)}"]
        return "\n".join(lines)

    def push(self, force=False, final=False):
        if self.msg_id is None:
            return
        now = time.time()
        if not force and now < self.next_edit:
            return
        text = self.render() if not final else self.last_text
        if text == self.last_text and not final:
            return
        params = {"chat_id": self.chat_id, "message_id": self.msg_id, "text": text}
        if not final:
            params["reply_markup"] = {"inline_keyboard": [[
                {"text": "❌ Cancel", "callback_data": f"c:{self.id}"}]]}
        try:
            self.tg.call("editMessageText", params, timeout=20, retries=0)
            self.last_text = text
            self.next_edit = now + 3.0
        except TelegramError as exc:
            if "not modified" in exc.description.lower():
                self.last_text = text
            elif exc.code == 429:
                self.next_edit = now + (exc.retry_after or 5) + 1
            else:
                self.next_edit = now + 5.0

    def finish_text(self, text):
        self.last_text = text
        params = {"chat_id": self.chat_id, "message_id": self.msg_id, "text": text}
        try:
            self.tg.call("editMessageText", params, timeout=20, retries=2)
        except TelegramError:
            try:
                self.tg.call("sendMessage", {"chat_id": self.chat_id, "text": text}, timeout=20, retries=2)
            except TelegramError:
                pass

    def ticker(self):
        try:
            while not self.done_flag.wait(1.0):
                try:
                    self.push()
                except Exception:
                    log.debug("ticker: %s", traceback.format_exc())
        finally:
            close_thread_connections()

    def check_cancel(self):
        if self.cancel_ev.is_set() or STOP.is_set():
            raise Cancelled()

    # ----- the whole job -----
    def start(self):
        try:
            m = self.tg.call("sendMessage", {
                "chat_id": self.chat_id, "text": f"⏳ Got it: {self.file_name}\nWaiting to start...",
                "reply_markup": {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": f"c:{self.id}"}]]}},
                timeout=20)
            self.msg_id = m["message_id"]
        except (TelegramError, KeyError, TypeError):
            self.bot.finish_job(self)
            return False
        return True

    def execute(self):
        threading.Thread(target=self.ticker, daemon=True).start()
        try:
            self.t0 = time.time()
            self._pipeline()
        except Cancelled:
            self.done_flag.set()
            self.finish_text("❌ Cancelled." if not STOP.is_set()
                             else "⚠️ The bot is restarting. Please send the file again in a minute.")
        except UserError as exc:
            self.done_flag.set()
            self.finish_text(f"⚠️ {exc}")
        except Stalled:
            self.done_flag.set()
            self.finish_text("⚠️ The translate services are not answering right now.\n"
                             "Nothing was lost, please send the file again in a few minutes.")
        except TelegramError as exc:
            log.warning("telegram problem in job: %s", exc)
            self.done_flag.set()
            self.finish_text("⚠️ Telegram had a problem while I worked on your file. Please send it again.")
        except MemoryError:
            self.done_flag.set()
            self.finish_text("⚠️ This file needs more memory than this server has.")
        except Exception:
            log.error("job crashed:\n%s", traceback.format_exc())
            self.done_flag.set()
            self.finish_text("⚠️ Something went wrong while translating this file. "
                             "Please send it again.")
        finally:
            self.done_flag.set()
            if self.workdir:
                shutil.rmtree(self.workdir, ignore_errors=True)
            self.bot.finish_job(self)

    def _pipeline(self):
        bot = self.bot
        self.check_cancel()
        self.workdir = tempfile.mkdtemp(prefix="hindibot_")
        src = os.path.join(self.workdir, "input" + self.ext)
        self.phase = "download"
        self.push(force=True)
        try:
            self.tg.download_file(self.file_id, src, MAX_FILE_BYTES + 1)
        except HttpError:
            raise UserError("That file is bigger than 15 MB.")
        except TelegramError as exc:
            if "too big" in exc.description.lower():
                raise UserError("That file is bigger than 15 MB.")
            raise UserError("I could not download that file from Telegram. Please send it again.")
        if os.path.getsize(src) > MAX_FILE_BYTES:
            raise UserError("That file is bigger than 15 MB.")
        self.check_cancel()

        self.phase = "read"
        self.push(force=True)
        proc = EpubProcessor(src) if self.ext == ".epub" else TxtProcessor(src)
        proc.load()
        texts = proc.table.texts
        sl = detect_source(texts)
        total_chars = sum(len(t) for t in texts)
        self.sl_label = (f"🈶 {({'zh': 'Chinese', 'en': 'English'}).get(sl, 'Auto-detected')}"
                         f"  •  {fmt_n(total_chars)} chars  •  {fmt_n(len(texts))} lines")
        self.check_cancel()

        run = Run(texts, bot.engines, sl, self.cancel_ev)
        self.run = run
        self.tr_start = time.time()
        self.phase = "translate"
        self.push(force=True)

        # PATCH: if the services stall near the end, do not throw everything away.
        salvaged = 0
        try:
            run.execute()
        except Stalled:
            if run.done_chars < SALVAGE_MIN_FRACTION * run.total_chars:
                raise
            salvaged = run.salvage()
            log.warning("stalled at %.1f%%, salvaged %d unfinished lines",
                        100.0 * run.done_chars / run.total_chars, salvaged)
        self.check_cancel()

        self.phase = "rescue"
        self.push(force=True)
        fixed = rescue_pass(run, bot.engines, sl, self.cancel_ev)
        self.check_cancel()
        results = run.results()

        self.phase = "build"
        self.push(force=True)
        out_path = os.path.join(self.workdir, "output" + self.ext)
        proc.build(results, out_path)

        self.phase = "check"
        self.push(force=True)
        units = proc.scan_output(out_path)
        problems, kept = [], []
        for u in units:
            flag = final_flag(u)
            if flag == "problem":
                problems.append(u)
            elif flag == "kept":
                kept.append(u)
        total_units = sum(1 for u in units if any(ch.isalpha() for ch in u))
        self.check_cancel()

        base_name = re.sub(r"[^\w\-. ()\[\]]+", "_", os.path.splitext(self.file_name)[0]).strip(" ._") or "book"
        base_name = base_name[:80]
        files = proc.outputs(out_path, base_name)
        if problems:
            lp = os.path.join(self.workdir, "leftover_lines.txt")
            with open(lp, "w", encoding="utf-8") as fh:
                fh.write("Lines that still look untranslated (first 300):\n\n")
                fh.write("\n".join(problems[:300]))
            extra = (lp, f"{base_name}_leftover_lines.txt")
        else:
            extra = None

        elapsed = time.time() - self.t0
        tot = sum(run.engine_chars.values())
        engines_txt = "  ".join(f"{n} {c * 100 // tot}%" for n, c in run.engine_chars.most_common(4)) if tot else ""
        many = total_units > 0 and len(problems) > 0.2 * total_units
        head = (f"⚠️ Finished in {fmt_t(elapsed)}, but many lines are still untranslated. "
                f"The translate services were unreliable, please try again later."
                if many else f"✅ Done in {fmt_t(elapsed)}")
        caption = [head, f"🈶 {({'zh': 'Chinese', 'en': 'English'}).get(sl, 'Auto')} → Hindi",
                   f"🧪 Check: {fmt_n(total_units - len(problems) - len(kept))} of {fmt_n(total_units)} lines fully translated"]
        if kept:
            caption.append(f"ℹ️ {fmt_n(len(kept))} short names/words kept as they are")
        if problems:
            caption.append(f"⚠️ {fmt_n(len(problems))} lines may still have untranslated words (list attached)")
        else:
            caption.append("✔️ No untranslated lines found")
        if salvaged:
            caption.append(f"⚠️ The translate services stopped answering near the end, "
                           f"{fmt_n(salvaged)} lines were left as they were")
        if fixed:
            caption.append(f"🔎 {fmt_n(fixed)} stubborn lines fixed in a second pass")
        if engines_txt:
            caption.append(f"🔧 {engines_txt}")
        cap = "\n".join(caption)

        self.phase = "send"
        self.push(force=True)
        for i, (path, fname) in enumerate(files):
            self.check_cancel()
            self.tg.send_document(self.chat_id, path, fname, cap if i == len(files) - 1 else "")
        if extra:
            self.tg.send_document(self.chat_id, extra[0], extra[1], "Lines to check by hand.")
        self.done_flag.set()
        self.finish_text(cap)


# --------------------------------------------------------------------------
# Bot (update handling)
# --------------------------------------------------------------------------
class Bot:
    def __init__(self, tg, engines):
        self.tg = tg
        self.engines = engines
        self.pool = ThreadPoolExecutor(max_workers=MAX_JOBS, thread_name_prefix="job")
        self.jobs = {}
        self.chat_jobs = {}
        self.lock = threading.Lock()

    def say(self, chat_id, text):
        try:
            self.tg.call("sendMessage", {"chat_id": chat_id, "text": text}, timeout=20, retries=2)
        except TelegramError as exc:
            log.debug("say failed: %s", exc)

    def finish_job(self, job):
        with self.lock:
            self.jobs.pop(job.id, None)
            if self.chat_jobs.get(job.chat_id) == job.id:
                self.chat_jobs.pop(job.chat_id, None)

    def handle_update(self, upd):
        if "callback_query" in upd:
            self.on_callback(upd["callback_query"])
        elif "message" in upd:
            self.on_message(upd["message"])

    def on_callback(self, cq):
        data = cq.get("data") or ""
        text = ""
        if data.startswith("c:"):
            with self.lock:
                job = self.jobs.get(data[2:])
            chat = ((cq.get("message") or {}).get("chat") or {}).get("id")
            if job and chat == job.chat_id:
                job.cancel_ev.set()
                text = "Cancelling..."
            else:
                text = "This job is already finished."
        try:
            self.tg.call("answerCallbackQuery", {"callback_query_id": cq.get("id"), "text": text},
                         timeout=15, retries=1)
        except TelegramError:
            pass

    def on_message(self, msg):
        chat_id = (msg.get("chat") or {}).get("id")
        if chat_id is None:
            return
        doc = msg.get("document")
        if not doc:
            txt = (msg.get("text") or "").strip()
            if txt.startswith("/start") or txt.startswith("/help") or not txt.startswith("/"):
                self.say(chat_id, HELP_TEXT if txt.startswith("/") else
                         "Please send a .epub or .txt file (up to 15 MB). I will translate it to Hindi.")
            return
        name = doc.get("file_name") or ""
        mime = (doc.get("mime_type") or "").lower()
        ext = os.path.splitext(name)[1].lower()
        if ext not in (".epub", ".txt"):
            if mime == "application/epub+zip":
                ext = ".epub"
            elif mime == "text/plain":
                ext = ".txt"
            else:
                self.say(chat_id, "I can only translate .epub and .txt files.")
                return
        if not name:
            name = "file" + ext
        size = doc.get("file_size")
        if isinstance(size, int) and size > MAX_FILE_BYTES:
            self.say(chat_id, f"That file is {size / 1048576:.1f} MB. The limit is 15 MB.")
            return
        with self.lock:
            if chat_id in self.chat_jobs:
                busy = True
            else:
                busy = False
                job = Job(self, chat_id, doc.get("file_id"), name, ext)
                self.jobs[job.id] = job
                self.chat_jobs[chat_id] = job.id
        if busy:
            self.say(chat_id, "⏳ I am still working on your previous file. "
                              "Wait for it, or press ❌ Cancel on its message.")
            return
        if not job.start():
            return
        try:
            self.pool.submit(job.execute)
        except RuntimeError:
            job.finish_text("⚠️ The bot is shutting down. Please send the file again in a minute.")
            self.finish_job(job)

    def wait_idle(self, seconds):
        """Wait until every running job is finished (or the time is up)."""
        end = time.time() + seconds
        while time.time() < end and not STOP.is_set():
            with self.lock:
                if not self.jobs:
                    return True
            time.sleep(1.0)
        with self.lock:
            return not self.jobs

    def shutdown(self):
        with self.lock:
            jobs = list(self.jobs.values())
        for j in jobs:
            j.cancel_ev.set()
        self.pool.shutdown(wait=False)


# --------------------------------------------------------------------------
# Optional tiny web server (some free hosts want an open port)
# --------------------------------------------------------------------------
class _Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"Hindi translate bot is running"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_HEAD = do_GET

    def log_message(self, *args):
        pass


def start_health_server():
    port = os.environ.get("PORT", "").strip()
    if not port.isdigit():
        return None
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", int(port)), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("health server on port %s", port)
    return srv


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------
class _Redact(logging.Filter):
    def __init__(self, token):
        super().__init__()
        self.token = token

    def filter(self, record):
        if self.token:
            msg = record.getMessage()
            if self.token in msg:
                record.msg = msg.replace(self.token, "***")
                record.args = ()
        return True


def poll_loop(bot, tg, deadline=None):
    """Returns (next_offset, timed_out)."""
    offset = None
    last_conflict = 0.0
    while not STOP.is_set():
        if deadline is not None and time.time() >= deadline:
            return offset, True
        params = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
        if offset is not None:
            params["offset"] = offset
        try:
            updates = tg.call("getUpdates", params, timeout=45, retries=0)
        except TelegramError as exc:
            if exc.code == 401:
                log.error("Telegram says the BOT_TOKEN is not valid. Stopping.")
                FATAL.set()
                STOP.set()
                return offset, False
            if exc.code == 409:
                if time.time() - last_conflict > 60:
                    log.warning("Another copy of this bot is polling with the same token. Waiting...")
                    last_conflict = time.time()
                try:
                    tg.call("deleteWebhook", {"drop_pending_updates": False}, retries=0)
                except TelegramError:
                    pass
            else:
                log.warning("getUpdates problem: %s", exc)
            STOP.wait(3.0)
            continue
        except Exception:
            log.error("poll crashed:\n%s", traceback.format_exc())
            STOP.wait(3.0)
            continue
        for upd in updates or []:
            offset = upd.get("update_id", 0) + 1
            try:
                bot.handle_update(upd)
            except Exception:
                log.error("update crashed:\n%s", traceback.format_exc())
    return offset, False


def main():
    token = os.environ.get("BOT_TOKEN", "").strip()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for handler in logging.getLogger().handlers:
        handler.addFilter(_Redact(token))
    if not token:
        print("BOT_TOKEN is missing.\n"
              "Get one from @BotFather in Telegram, then run:\n"
              "  export BOT_TOKEN='123456:ABC...'\n  python bot.py")
        return 1
    if not re.match(r"^\d+:[\w-]{20,}$", token):
        print("BOT_TOKEN does not look right. It should look like 123456789:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        return 1
    tg = TgClient(token, os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org"))
    try:
        me = tg.call("getMe", retries=3)
    except TelegramError as exc:
        print(f"Could not log in to Telegram: {exc.description}")
        return 1
    try:
        tg.call("deleteWebhook", {"drop_pending_updates": False}, retries=2)
    except TelegramError:
        pass
    engines = build_engines()
    bot = Bot(tg, engines)
    start_health_server()
    log.info("Hindi translate bot v%s running as @%s with %d engines",
             VERSION, (me or {}).get("username", "?"), len(engines))

    def _stop(*_):
        STOP.set()

    signal.signal(signal.SIGINT, _stop)
    try:
        signal.signal(signal.SIGTERM, _stop)
    except (ValueError, AttributeError, OSError):
        pass
    deadline = time.time() + MAX_RUNTIME_MIN * 60 if MAX_RUNTIME_MIN > 0 else None
    offset, timed_out = poll_loop(bot, tg, deadline)
    if timed_out:
        log.info("time limit reached: no new files, finishing running jobs (up to %d min)", DRAIN_MIN)
        try:        # confirm what we already handled so the next copy does not repeat it
            if offset is not None:
                for upd in tg.call("getUpdates", {"offset": offset, "limit": 1, "timeout": 0},
                                   timeout=20, retries=1) or []:
                    bot.handle_update(upd)
        except TelegramError as exc:
            log.warning("final confirm failed: %s", exc)
        bot.wait_idle(DRAIN_MIN * 60)
    log.info("stopping...")
    bot.shutdown()
    time.sleep(1.0)
    return 1 if FATAL.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())
