"""
Collect e-commerce news from VNExpress RSS feeds.
Used to enrich data analysis context (sentiment, trends).
"""
import feedparser
import pandas as pd
from datetime import date, datetime
from loguru import logger


RSS_FEEDS = {
    "vnexpress_kinh_doanh": "https://vnexpress.net/rss/kinh-doanh.rss",
    "vnexpress_so_hoa": "https://vnexpress.net/rss/so-hoa.rss",
    "cafef_thi_truong": "https://cafef.vn/thi-truong-chung-khoan.rss",
}

ECOMMERCE_KEYWORDS = [
    "shopee", "tiki", "lazada", "sendo", "thương mại điện tử",
    "mua sắm online", "delivery", "logistics", "bán lẻ", "e-commerce"
]


def fetch_news(target_date: date = None) -> pd.DataFrame:
    """
    Fetch and filter e-commerce related news articles.
    """
    if target_date is None:
        target_date = date.today()

    logger.info(f"Fetching news RSS for {target_date}")

    all_articles = []

    for source_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            logger.info(f"  {source_name}: {len(feed.entries)} articles found")

            for entry in feed.entries:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                content = (title + " " + summary).lower()

                # Filter only e-commerce related articles
                is_relevant = any(kw in content for kw in ECOMMERCE_KEYWORDS)

                # Parse publish date
                published_parsed = entry.get("published_parsed")
                if published_parsed:
                    pub_date = datetime(*published_parsed[:6]).isoformat()
                else:
                    pub_date = datetime.utcnow().isoformat()

                all_articles.append({
                    "source": source_name,
                    "title": title,
                    "summary": summary[:500] if summary else "",
                    "link": entry.get("link", ""),
                    "published_at": pub_date,
                    "is_ecommerce_relevant": is_relevant,
                    "date": target_date.isoformat(),
                    "ingested_at": datetime.utcnow().isoformat(),
                })

        except Exception as e:
            logger.error(f"Failed to fetch {source_name}: {e}")
            continue

    df = pd.DataFrame(all_articles)
    relevant_count = df["is_ecommerce_relevant"].sum() if not df.empty else 0
    logger.success(f"Total: {len(df)} articles, {relevant_count} e-commerce relevant")
    return df