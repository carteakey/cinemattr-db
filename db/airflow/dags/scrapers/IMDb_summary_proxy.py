import os
import re
import warnings
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.simplefilter(action="ignore", category=FutureWarning)

currentDateAndTime = datetime.now()
timestamp = currentDateAndTime.strftime("%Y%m%d%H%M%S")

IMDB_TITLE_URL = "https://www.imdb.com/title/tt"
DEFAULT_SCRAPE = f"/opt/airflow/data/imdb_plots/{timestamp}_scrape.csv"
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SCRAPER_REQUEST_TIMEOUT_SECONDS", "30"))


def convert_df(df):
    return df.to_csv().encode("utf-8")


def delete(filename):
    if os.path.exists(filename) and os.path.isfile(filename):
        os.remove(filename)


def getPlotURL(title_id):
    return IMDB_TITLE_URL + title_id + "/plotsummary"


def fetch(url):
    url_ant = "https://api.scrapingant.com/v2/general"
    params = {"url": url, "x-api-key": os.environ["SCRAPINGANT_API_KEY"], "browser": "false"}
    query_string = urlencode(params)
    full_url = f"{url_ant}?{query_string}"
    print(full_url)
    with requests.get(full_url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        return response.text


def _clean_text(value):
    return re.sub(r"\s+", " ", value).strip()


def parse_plot_page(html):
    page_html = BeautifulSoup(html, "html.parser")
    div_summaries = page_html.find("div", attrs={"data-testid": "sub-section-summaries"})
    div_synopsis = page_html.find("div", attrs={"data-testid": "sub-section-synopsis"})

    if div_summaries is not None:
        span_summaries = div_summaries.find_all("span", attrs={"data-reactroot": ""})
        if span_summaries is not None:
            for span in span_summaries:
                span.decompose()

    text_summaries = ""
    if div_summaries is not None:
        text_summaries = _clean_text(div_summaries.get_text(" ", strip=True))

    text_synopsis = ""
    if div_synopsis is not None:
        text_synopsis = _clean_text(div_synopsis.get_text(" ", strip=True))

    return {"plot_summary": text_summaries, "plot_synopsis": text_synopsis}


def fetch_and_parse(url):
    html = fetch(url)
    return parse_plot_page(html)


def scrape(title_ids, data_file=DEFAULT_SCRAPE):
    records = []
    for title_id in title_ids:
        url = getPlotURL(title_id)
        movie_data = {"imdb_title_id": title_id}
        data = fetch_and_parse(url)
        movie_data["summary"] = data["plot_summary"]
        movie_data["plot"] = data["plot_synopsis"]
        records.append(movie_data)

    if not records:
        raise RuntimeError("No IMDb plot rows were scraped.")

    scrape_df = pd.DataFrame.from_records(records)
    os.makedirs(os.path.dirname(data_file), exist_ok=True)
    delete(data_file)
    scrape_df.to_csv(data_file, encoding="utf8", mode="a", index=False, header=True)
    return data_file


if __name__ == "__main__":
    ids = ["0110912", "0133093", "0120737", "0068646"]
    scrape(ids)
