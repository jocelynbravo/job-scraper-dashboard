# job_scraper.py
# Web scraper for job listings (Indeed-style)
# Tools: requests, BeautifulSoup, pandas

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from datetime import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

KEYWORDS = ["data analyst", "data scientist", "BI developer", "data engineer"]
LOCATION = "United States"

def scrape_jobs(keyword, location, pages=3):
    """Scrape job listings for a given keyword and location."""
    jobs = []

    for page in range(0, pages * 10, 10):
        url = (
            f"https://www.indeed.com/jobs"
            f"?q={keyword.replace(' ', '+')}"
            f"&l={location.replace(' ', '+')}"
            f"&start={page}"
        )
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  Request failed: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        cards = soup.find_all("div", class_="job_seen_beacon")

        if not cards:
            print(f"  No cards found on page {page // 10 + 1} — site structure may have changed.")
            break

        for card in cards:
            title_tag    = card.find("h2", class_="jobTitle")
            company_tag  = card.find("span", {"data-testid": "company-name"})
            location_tag = card.find("div",  {"data-testid": "text-location"})
            salary_tag   = card.find("div",  {"data-testid": "attribute_snippet_testid"})

            jobs.append({
                "title":    title_tag.get_text(strip=True)    if title_tag    else "N/A",
                "company":  company_tag.get_text(strip=True)  if company_tag  else "N/A",
                "location": location_tag.get_text(strip=True) if location_tag else "N/A",
                "salary":   salary_tag.get_text(strip=True)   if salary_tag   else "Not listed",
                "keyword":  keyword,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

        print(f"  Page {page // 10 + 1}: {len(cards)} listings found")
        time.sleep(2)   # Be polite — avoid rate limiting

    return jobs


def clean_data(df):
    """Basic cleaning and enrichment."""
    df.drop_duplicates(subset=["title", "company", "location"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Classify work type from location text
    def work_type(loc):
        loc = loc.lower()
        if "remote" in loc:
            return "Remote"
        elif "hybrid" in loc:
            return "Hybrid"
        else:
            return "On-site"

    df["work_type"] = df["location"].apply(work_type)

    # Classify experience level from title
    def exp_level(title):
        title = title.lower()
        if any(w in title for w in ["senior", "sr.", "lead", "principal", "staff"]):
            return "Senior"
        elif any(w in title for w in ["junior", "jr.", "entry", "associate", "i "]):
            return "Entry"
        else:
            return "Mid"

    df["level"] = df["title"].apply(exp_level)
    return df


def save_outputs(df):
    """Save cleaned data as CSV and JSON."""
    df.to_csv("data/jobs_raw.csv", index=False)

    summary = {
        "total_listings": len(df),
        "by_keyword": df["keyword"].value_counts().to_dict(),
        "by_work_type": df["work_type"].value_counts().to_dict(),
        "by_level": df["level"].value_counts().to_dict(),
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    with open("data/summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nSaved {len(df)} listings to data/jobs_raw.csv")
    print(f"Summary saved to data/summary.json")


if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)

    all_jobs = []
    for keyword in KEYWORDS:
        print(f"\nScraping: {keyword}")
        jobs = scrape_jobs(keyword, LOCATION, pages=3)
        all_jobs.extend(jobs)

    df = pd.DataFrame(all_jobs)
    df = clean_data(df)
    save_outputs(df)

    print("\nDone! Open index.html to view the dashboard.")
