#!/usr/bin/env python3
"""
Dubizzle Dubai Car Scraper
Scrapes used and new car listings from dubai.dubizzle.com
"""

import json
import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

BASE_URL = "https://dubai.dubizzle.com"
CATEGORY_URLS = [
    "/motors/used-cars/",
    "/motors/new-cars/",
]
MAX_PAGES = 5
DB_FILE = "listings_db_uae_cars.json"
FEED_FILE = "drops_feed_uae_cars.json"
WAR_START = "2026-03-01"
DROP_THRESHOLD = 0.05

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
}


def fetch_page(url):
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(2)
    return None


def extract_hits(html):
    """Extract listing hits from __NEXT_DATA__ Redux SSR actions."""
    marker = '<script id="__NEXT_DATA__" type="application/json">'
    idx = html.find(marker)
    if idx != -1:
        start = idx + len(marker)
        end = html.find("</script>", start)
        if end != -1:
            try:
                next_data = json.loads(html[start:end])
                actions = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("reduxWrapperActionsGIPP", [])
                )
                for action in actions:
                    if action.get("type") == "listings/fetchListingDataForQuery/fulfilled":
                        payload = action.get("payload", {})
                        hits = payload.get("hits", [])
                        pagination = payload.get("pagination", {})
                        total_pages = pagination.get("totalPages", 0)
                        return hits, total_pages
            except (json.JSONDecodeError, ValueError) as e:
                print(f"  JSON parse error: {e}")
    # Fallback: try window.state
    pattern = r'window\.state\s*=\s*(\{.*?\});\s*</script>'
    m = re.search(pattern, html, re.DOTALL)
    if m:
        try:
            state = json.loads(m.group(1))
            results = state.get("searchResult", state.get("listings", {}))
            hits = results.get("hits", results.get("listings", []))
            total_pages = results.get("totalPages", results.get("nbPages", 0))
            return hits, total_pages
        except (json.JSONDecodeError, ValueError):
            pass
    return [], 0


def get_info_field(hit, field_id):
    """Extract a field value from car_info or motor_info lists."""
    for info_list_key in ["car_info", "motor_info", "details"]:
        info_list = hit.get(info_list_key, [])
        if isinstance(info_list, list):
            for item in info_list:
                if isinstance(item, dict) and item.get("id") == field_id:
                    val = item.get("value", {})
                    if isinstance(val, dict):
                        return val.get("en", val.get("label", str(val)))
                    return str(val)
    return ""


def parse_hit(hit):
    """Parse a single car listing hit into a standardized dict."""
    try:
        name = hit.get("name", {})
        title = name.get("en", name) if isinstance(name, dict) else str(name)
        price = hit.get("price")
        if price is None:
            return None
        try:
            price = int(float(price))
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        # Location
        city = hit.get("city", {})
        city_name = city.get("name", {}).get("en", "Dubai") if isinstance(city, dict) else "Dubai"
        neighborhoods = hit.get("neighborhoods", {})
        area_names = neighborhoods.get("name", {}).get("en", []) if isinstance(neighborhoods, dict) else []
        area = area_names[0] if isinstance(area_names, list) and area_names else ""

        # URL
        abs_url = hit.get("absolute_url", {})
        url = abs_url.get("en", "") if isinstance(abs_url, dict) else str(abs_url)
        if url and not url.startswith("http"):
            url = BASE_URL + url

        # Category
        cats = hit.get("categories", {})
        cat_names = cats.get("name", {}).get("en", []) if isinstance(cats, dict) else []
        category = cat_names[0] if isinstance(cat_names, list) and cat_names else "Car"

        # Date
        added = hit.get("added")
        if added and isinstance(added, (int, float)) and added > 1000000000:
            date_str = datetime.fromtimestamp(added, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Car-specific fields
        year = get_info_field(hit, "year") or hit.get("year", "")
        make = get_info_field(hit, "make") or hit.get("make", "")
        model = get_info_field(hit, "model") or hit.get("model", "")
        mileage = get_info_field(hit, "kilometers") or get_info_field(hit, "mileage") or hit.get("mileage", "")
        body_type = get_info_field(hit, "body_type") or ""
        transmission = get_info_field(hit, "transmission") or ""

        listing_id = str(hit.get("id", hit.get("external_id", "")))
        if not listing_id:
            listing_id = url.split("/")[-2] if url else str(hash(title))

        return {
            "id": listing_id,
            "title": title,
            "price": price,
            "year": str(year),
            "make": str(make),
            "model": str(model),
            "mileage": str(mileage),
            "body_type": str(body_type),
            "transmission": str(transmission),
            "area": area,
            "city": city_name,
            "category": category,
            "url": url,
            "date": date_str,
        }
    except Exception as e:
        print(f"  Parse error: {e}")
        return None


def scrape_all():
    """Scrape all car category pages."""
    all_listings = []
    for cat_url in CATEGORY_URLS:
        print(f"\nScraping category: {cat_url}")
        page = 0
        while page < MAX_PAGES:
            url = f"{BASE_URL}{cat_url}" if page == 0 else f"{BASE_URL}{cat_url}?page={page}"
            print(f"  Page {page}: {url}")
            html = fetch_page(url)
            if not html:
                print(f"  Failed to fetch page {page}")
                break
            hits, total_pages = extract_hits(html)
            print(f"  Found {len(hits)} hits, totalPages={total_pages}")
            if not hits:
                break
            for hit in hits:
                listing = parse_hit(hit)
                if listing:
                    all_listings.append(listing)
            page += 1
            if page >= total_pages:
                break
            time.sleep(1)
    print(f"\nTotal car listings scraped: {len(all_listings)}")
    return all_listings


def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)


def load_feed():
    if os.path.exists(FEED_FILE):
        try:
            with open(FEED_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return {"meta": {"war_start": WAR_START, "threshold": DROP_THRESHOLD}, "tracked": 0, "drops": []}


def save_feed(feed):
    with open(FEED_FILE, "w") as f:
        json.dump(feed, f, indent=2)


def main():
    print("=" * 60)
    print("Dubizzle Dubai Car Scraper")
    print("=" * 60)
    listings = scrape_all()
    if not listings:
        print("No listings found. Exiting.")
        return

    db = load_db()
    feed = load_feed()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_count = 0
    drop_count = 0

    for listing in listings:
        lid = listing["id"]
        price = listing["price"]

        if lid in db:
            prev = db[lid]
            prev_price = prev.get("price", price)
            if price < prev_price:
                drop_pct = (prev_price - price) / prev_price
                if drop_pct >= DROP_THRESHOLD:
                    drop_entry = {
                        "id": lid,
                        "title": listing["title"],
                        "old_price": prev_price,
                        "new_price": price,
                        "drop_pct": round(drop_pct * 100, 1),
                        "url": listing["url"],
                        "area": listing["area"],
                        "category": listing["category"],
                        "year": listing.get("year", ""),
                        "make": listing.get("make", ""),
                        "model": listing.get("model", ""),
                        "date": today,
                        "first_seen": prev.get("first_seen", today),
                    }
                    feed["drops"].append(drop_entry)
                    drop_count += 1
            db[lid]["price"] = price
            db[lid]["last_seen"] = today
            db[lid]["title"] = listing["title"]
        else:
            db[lid] = {
                "title": listing["title"],
                "price": price,
                "year": listing.get("year", ""),
                "make": listing.get("make", ""),
                "model": listing.get("model", ""),
                "area": listing["area"],
                "city": listing["city"],
                "category": listing["category"],
                "url": listing["url"],
                "first_seen": today,
                "last_seen": today,
            }
            new_count += 1

    feed["tracked"] = len(db)
    feed["last_updated"] = today

    # Keep only last 500 drops
    feed["drops"] = feed["drops"][-500:]

    save_db(db)
    save_feed(feed)

    print(f"\nResults:")
    print(f"  New listings: {new_count}")
    print(f"  Price drops: {drop_count}")
    print(f"  Total tracked: {len(db)}")


if __name__ == "__main__":
    main()
