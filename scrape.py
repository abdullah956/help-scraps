"""
Scrape pet hospitals, vet clinics, and pet stores in Lahore
using Apify's Google Maps Scraper. Results saved to CSV incrementally.
"""

import csv
import os
import sys
import threading
import time
from apify_client import ApifyClient

APIFY_TOKEN = ""
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pet_contacts_lahore.csv")

CSV_HEADERS = [
    "name", "category", "phone", "email", "website",
    "full_address", "city", "area", "rating", "reviews_count",
    "search_query", "google_maps_url",
]

SEARCH_QUERIES = [

]

# Lock for thread-safe CSV writes
csv_lock = threading.Lock()
seen_names = set()
seen_lock = threading.Lock()


def init_csv():
    """Create CSV with headers if it doesn't exist."""
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()


def append_row(row: dict):
    """Append a single row to the CSV file (thread-safe)."""
    with csv_lock:
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writerow(row)


def extract_fields(item: dict, query: str) -> dict:
    """Pull the fields we care about from an Apify result item."""
    return {
        "name": item.get("title", ""),
        "category": item.get("categoryName", ""),
        "phone": item.get("phone", ""),
        "email": item.get("email", "") or "",
        "website": item.get("website", "") or "",
        "full_address": item.get("address", ""),
        "city": item.get("city", ""),
        "area": item.get("neighborhood", "") or item.get("street", ""),
        "rating": item.get("totalScore", ""),
        "reviews_count": item.get("reviewsCount", ""),
        "search_query": query,
        "google_maps_url": item.get("url", ""),
    }


def run_query(client: ApifyClient, query: str):
    """Run a single search query on Apify and write results to CSV incrementally."""
    print(f"[START] {query}")

    run_input = {
        "searchStringsArray": [query],
        "maxCrawledPlacesPerSearch": 50,
        "language": "en",
        "deeperCityScrape": False,
    }

    try:
        run = client.actor("nwua9Gu5YrADL7ZDj").call(run_input=run_input)
    except Exception as e:
        print(f"[ERROR] {query}: {e}")
        return

    dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
    count = 0

    for item in dataset_items:
        name = (item.get("title") or "").strip().lower()
        if not name:
            continue

        with seen_lock:
            if name in seen_names:
                continue
            seen_names.add(name)

        row = extract_fields(item, query)
        append_row(row)
        count += 1
        print(f"  + {row['name']} | {row['phone']} | {row['full_address']}")

    print(f"[DONE] {query} — {count} new results")


def main():
    print("=" * 60)
    print("")
    print("=" * 60)

    # Remove old output so we start fresh
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    init_csv()

    client = ApifyClient(APIFY_TOKEN)

    # Run queries in parallel (4 threads to stay within rate limits)
    threads: list[threading.Thread] = []
    max_workers = 4

    for i in range(0, len(SEARCH_QUERIES), max_workers):
        batch = SEARCH_QUERIES[i : i + max_workers]
        threads_batch = []
        for q in batch:
            t = threading.Thread(target=run_query, args=(client, q))
            t.start()
            threads_batch.append(t)

        for t in threads_batch:
            t.join()

    print("\n" + "=" * 60)
    print(f"All done! Results saved to: {OUTPUT_FILE}")

    # Print summary
    try:
        import pandas as pd
        df = pd.read_csv(OUTPUT_FILE)
        print(f"Total unique contacts: {len(df)}")
        print(f"\nBreakdown by city:")
        if "city" in df.columns:
            print(df["city"].value_counts().to_string())
    except Exception:
        pass


if __name__ == "__main__":
    main()

