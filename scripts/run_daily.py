from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from paracel_monitor.pipeline import collect_mentions, build_dataset, summarize


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/config.yml")
    ap.add_argument("--days-back", type=int, default=None)
    ap.add_argument("--no-extract", action="store_true")
    ap.add_argument("--out-data-dir", default="data")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))

    days_back = int(args.days_back if args.days_back is not None else cfg["project"]["days_back_default"])

    queries = list(cfg["queries"])
    use_gdelt = bool(cfg["sources"].get("gdelt", True))
    use_google_news = bool(cfg["sources"].get("google_news_rss", True))
    rss_feeds = cfg["sources"].get("rss_feeds") or []
    gdelt_max_records = int(cfg["project"].get("max_records_gdelt", 250))
    gdelt_language = cfg.get("language", {}).get("gdelt_language", None)

    taxonomy = cfg.get("taxonomy", {}).get("topics", {}) or {}
    sentiment_pos = cfg.get("sentiment", {}).get("positive", []) or []
    sentiment_neg = cfg.get("sentiment", {}).get("negative", []) or []

    mentions = collect_mentions(
        queries=queries,
        days_back=days_back,
        use_gdelt=use_gdelt,
        use_google_news_rss=use_google_news,
        rss_feeds=rss_feeds,
        gdelt_max_records=gdelt_max_records,
        gdelt_language=gdelt_language,
    )

    df = build_dataset(
        mentions=mentions,
        do_extract_text=not args.no_extract,
        taxonomy=taxonomy,
        sentiment_pos=sentiment_pos,
        sentiment_neg=sentiment_neg,
    )

    out_dir = Path(args.out_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df.write_parquet(out_dir / "paracel_mentions.parquet")
    df.write_csv(out_dir / "paracel_mentions.csv")

    sums = summarize(df)
    for k, sdf in sums.items():
        sdf.write_csv(out_dir / f"summary__{k}.csv")


if __name__ == "__main__":
    main()
