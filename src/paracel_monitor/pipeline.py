from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import requests
import feedparser
import trafilatura
from dateutil import parser as dtparser
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


class HTTPError(RuntimeError):
    pass


@retry(
    wait=wait_exponential(multiplier=0.8, min=1, max=12),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.RequestException, HTTPError)),
)
def http_get(url: str, params: Optional[dict] = None, timeout: int = 25) -> requests.Response:
    resp = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    if resp.status_code >= 400:
        raise HTTPError(f"HTTP {resp.status_code} for {url}")
    return resp


def safe_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = dtparser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def domain_from_url(url: str) -> Optional[str]:
    try:
        from urllib.parse import urlparse
        netloc = urlparse(url).netloc.lower().strip()
        return netloc if netloc else None
    except Exception:
        return None


def fingerprint(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8", errors="ignore")).hexdigest()


@dataclass(frozen=True)
class Mention:
    query: str
    source: str
    title: Optional[str]
    url: str
    published_at: Optional[datetime]
    domain: Optional[str]
    snippet: Optional[str]
    raw: Optional[dict]


def build_google_news_rss_url(query: str, hl: str = "es-419", gl: str = "PY", ceid: str = "PY:es-419") -> str:
    from urllib.parse import quote_plus
    q = quote_plus(query)
    return f"https://news.google.com/rss/search?q={q}&hl={hl}&gl={gl}&ceid={ceid}"


def fetch_google_news_rss(query: str, hl: str = "es-419", gl: str = "PY", ceid: str = "PY:es-419") -> List[Mention]:
    rss_url = build_google_news_rss_url(query=query, hl=hl, gl=gl, ceid=ceid)
    d = feedparser.parse(rss_url)

    out: List[Mention] = []
    for e in d.entries or []:
        url = getattr(e, "link", None)
        if not url:
            continue
        title = getattr(e, "title", None)
        published = safe_dt(getattr(e, "published", None)) if hasattr(e, "published") else None
        summary = getattr(e, "summary", None)

        out.append(
            Mention(
                query=query,
                source="google_news_rss",
                title=title,
                url=url,
                published_at=published,
                domain=domain_from_url(url),
                snippet=summary,
                raw={"rss": rss_url},
            )
        )
    return out


def fetch_rss_feed(feed_url: str, query_label: str) -> List[Mention]:
    d = feedparser.parse(feed_url)
    out: List[Mention] = []
    for e in d.entries or []:
        url = getattr(e, "link", None)
        if not url:
            continue
        title = getattr(e, "title", None)
        summary = getattr(e, "summary", None)

        published = None
        if hasattr(e, "published"):
            published = safe_dt(getattr(e, "published", None))
        elif hasattr(e, "updated"):
            published = safe_dt(getattr(e, "updated", None))

        out.append(
            Mention(
                query=query_label,
                source="rss",
                title=title,
                url=url,
                published_at=published,
                domain=domain_from_url(url),
                snippet=summary,
                raw={"feed": feed_url},
            )
        )
    return out


def fetch_gdelt_2_doc(
    query: str,
    max_records: int = 250,
    start_datetime_utc: Optional[datetime] = None,
    end_datetime_utc: Optional[datetime] = None,
    language: Optional[str] = None,
) -> List[Mention]:
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params: Dict[str, Any] = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": int(max_records),
        "sort": "HybridRel",
    }
    if start_datetime_utc is not None:
        params["startdatetime"] = start_datetime_utc.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")
    if end_datetime_utc is not None:
        params["enddatetime"] = end_datetime_utc.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")
    if language:
        params["language"] = language

    r = http_get(base_url, params=params)
    payload = r.json()
    articles = payload.get("articles", []) or []

    out: List[Mention] = []
    for a in articles:
        url = a.get("url")
        if not url:
            continue
        out.append(
            Mention(
                query=query,
                source="gdelt",
                title=a.get("title"),
                url=url,
                published_at=safe_dt(a.get("seendate") or a.get("published")),
                domain=domain_from_url(url),
                snippet=a.get("snippet") or a.get("summary"),
                raw=a,
            )
        )
    return out


def extract_article(url: str, max_chars: int = 120_000) -> Tuple[Optional[str], Optional[str]]:
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None, None

        result = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            output_format="json",
        )
        if not result:
            return None, None

        j = json.loads(result)
        text = j.get("text")
        title = j.get("title")

        if text:
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > max_chars:
                text = text[:max_chars]
        if title:
            title = re.sub(r"\s+", " ", title).strip()

        return text or None, title or None
    except Exception:
        return None, None


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


def sentiment_proxy(text: str, pos: List[str], neg: List[str]) -> Tuple[Optional[str], Optional[float]]:
    t = normalize_text(text)
    if not t:
        return None, None

    p = sum(t.count(w.lower()) for w in pos if w)
    n = sum(t.count(w.lower()) for w in neg if w)

    length = max(len(t), 1)
    score = (p - n) / (length ** 0.5)

    if score > 0.15:
        return "positive", float(score)
    if score < -0.15:
        return "negative", float(score)
    return "neutral", float(score)


def apply_taxonomy(text: str, taxonomy: Dict[str, List[str]]) -> List[str]:
    t = normalize_text(text)
    if not t:
        return []
    out: List[str] = []
    for topic, patterns in taxonomy.items():
        for pat in patterns or []:
            try:
                if re.search(pat, t, flags=re.IGNORECASE):
                    out.append(topic)
                    break
            except re.error:
                continue
    return out


def collect_mentions(
    queries: List[str],
    days_back: int,
    use_gdelt: bool,
    use_google_news_rss: bool,
    rss_feeds: Optional[List[str]],
    gdelt_max_records: int = 250,
    gdelt_language: Optional[str] = None,
) -> List[Mention]:
    start = datetime.now(timezone.utc) - timedelta(days=int(days_back))
    end = datetime.now(timezone.utc)

    mentions: List[Mention] = []

    for q in queries:
        if use_gdelt:
            mentions.extend(
                fetch_gdelt_2_doc(
                    query=q,
                    max_records=gdelt_max_records,
                    start_datetime_utc=start,
                    end_datetime_utc=end,
                    language=gdelt_language,
                )
            )
        if use_google_news_rss:
            mentions.extend(fetch_google_news_rss(q))

        if rss_feeds:
            for rss in rss_feeds:
                mentions.extend(fetch_rss_feed(rss, query_label=q))

    seen: set[str] = set()
    uniq: List[Mention] = []
    for m in mentions:
        fp = fingerprint(m.url)
        if fp in seen:
            continue
        seen.add(fp)
        uniq.append(m)

    return uniq


def build_dataset(
    mentions: List[Mention],
    do_extract_text: bool,
    taxonomy: Dict[str, List[str]],
    sentiment_pos: List[str],
    sentiment_neg: List[str],
) -> pl.DataFrame:
    rows: List[Dict[str, Any]] = []

    for m in mentions:
        text = None
        extracted_title = None
        if do_extract_text:
            text, extracted_title = extract_article(m.url)

        joined = " ".join([x for x in [m.title, extracted_title, m.snippet, text] if x])
        topics = apply_taxonomy(joined, taxonomy)
        sent_label, sent_score = sentiment_proxy(joined, sentiment_pos, sentiment_neg)

        rows.append(
            {
                "query": m.query,
                "source": m.source,
                "title": m.title,
                "extracted_title": extracted_title,
                "best_title": extracted_title or m.title,
                "url": m.url,
                "domain": m.domain,
                "published_at": m.published_at,
                "snippet": m.snippet,
                "text": text,
                "topics": topics,
                "sentiment_label": sent_label,
                "sentiment_score": sent_score,
                "url_sha256": fingerprint(m.url),
                "ingested_at": datetime.now(timezone.utc),
            }
        )

    df = pl.DataFrame(rows).with_columns(
        [
            pl.col("published_at").cast(pl.Datetime(time_zone="UTC")),
            pl.col("ingested_at").cast(pl.Datetime(time_zone="UTC")),
            pl.col("sentiment_score").cast(pl.Float64),
            pl.col("topics").cast(pl.List(pl.Utf8)),
            pl.col("domain").str.to_lowercase().alias("domain"),
        ]
    )

    return df


def summarize(df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    by_source = df.group_by("source").agg(pl.len().alias("n")).sort("n", descending=True)

    by_domain = (
        df.filter(pl.col("domain").is_not_null())
        .group_by("domain")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(100)
    )

    by_day = (
        df.with_columns(pl.col("published_at").dt.date().alias("day"))
        .group_by("day")
        .agg(pl.len().alias("n"))
        .sort("day")
    )

    by_sentiment = (
        df.filter(pl.col("sentiment_label").is_not_null())
        .group_by("sentiment_label")
        .agg(pl.len().alias("n"), pl.mean("sentiment_score").alias("avg_score"))
        .sort("n", descending=True)
    )

    by_topic = (
        df.explode("topics")
        .filter(pl.col("topics").is_not_null())
        .group_by("topics")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )

    return {"by_source": by_source, "by_domain": by_domain, "by_day": by_day, "by_sentiment": by_sentiment, "by_topic": by_topic}


def to_dashboard_json(df: pl.DataFrame, max_items: int) -> List[Dict[str, Any]]:
    cols = ["published_at", "source", "domain", "best_title", "url", "snippet", "sentiment_label", "sentiment_score", "topics", "query"]
    dfx = df.select([c for c in cols if c in df.columns]).sort("published_at", descending=True).head(int(max_items))

    out: List[Dict[str, Any]] = []
    for r in dfx.to_dicts():
        dt = r.get("published_at")
        if dt is not None:
            r["published_at"] = dt.isoformat()
        out.append(r)
    return out
