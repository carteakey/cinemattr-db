import asyncio
import os
import time
import warnings
from datetime import datetime

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup, element

warnings.simplefilter(action="ignore", category=FutureWarning)

currentDateAndTime = datetime.now()
timestamp = currentDateAndTime.strftime("%Y%m%d%H%M%S")

IMDB_TITLE_URL = "https://www.imdb.com/title/tt"
IMDB_SRCH_URL = "https://www.imdb.com/search/title/?title_type=feature&count=250"
DEFAULT_SCRAPE = f"/opt/airflow/data/imdb/{timestamp}_scrape.csv"
DEFAULT_HEADERS = {"Accept-Language": "en-US, en;q=0.5", "User-Agent": "cinemattr-db/2.0"}
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SCRAPER_REQUEST_TIMEOUT_SECONDS", "30"))
SCRAPE_DELAY_SECONDS = float(os.getenv("SCRAPER_DELAY_SECONDS", "1"))


def convert_df(df):
    return df.to_csv().encode("utf-8")


def delete(filename):
    if os.path.exists(filename) and os.path.isfile(filename):
        os.remove(filename)


def getSearchURL(year, page, rating, genre):
    url = IMDB_SRCH_URL
    if rating is not None:
        url += "&user_rating=" + str(rating[0]) + "," + str(rating[1])
    if year is not None:
        url += "&release_date=" + str(year)
    url += "&sort=num_votes,desc"
    if page is not None:
        url += "&&start=" + str(page)
    if genre is not None:
        url += "&genres=" + ",".join(genre)
    return url


async def fetch(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


def parse_search_page(html):
    movie_list = []
    page_html = BeautifulSoup(html, "html.parser")
    mv_containers = page_html.find_all("div", class_="lister-item mode-advanced")

    for container in mv_containers:
        if container.strong is None or container.strong.text is None:
            continue

        data = {}

        title_id = container.a["href"].split("/")[2]
        data["imdb_title_id"] = title_id
        data["title"] = container.h3.a.text

        year = (
            container.h3.find("span", class_="lister-item-year")
            .text.replace("(", "")
            .replace(")", "")
        )
        if " " in year:
            year = year.split(" ")[1]
        data["year"] = year

        certificate = container.find("span", class_="certificate")
        if certificate is not None:
            data["certificate"] = certificate.text

        genre = container.find("span", class_="genre")
        if genre is not None:
            data["genre"] = genre.text.strip()

        runtime = container.find("span", class_="runtime")
        if runtime is not None:
            data["runtime"] = runtime.text

        description = container.find_all("p", class_="text-muted")
        if len(description) > 1:
            data["description"] = description[1].get_text(" ", strip=True)

        data["IMDb_rating"] = float(container.strong.text)

        m_score = container.find("span", class_="metascore")
        if m_score is not None:
            data["MetaScore"] = m_score.text

        vote = container.find("span", attrs={"name": "nv"})
        if vote is None or not vote.has_attr("data-value"):
            continue
        data["ratingCount"] = vote["data-value"]

        try:
            credit_container = container.find("p", class_="")
            a_tag = credit_container.find("a")
            text = a_tag.previousSibling
            stars = []

            if text.strip() == "Director:":
                data["directors"] = a_tag.text
                stars = [a.get_text() for a in a_tag.find_next_siblings("a")]
            elif text.strip() == "Directors:":
                directors = []
                while True:
                    if isinstance(a_tag, element.Tag):
                        if a_tag.name == "span":
                            break
                        directors.append(a_tag.text)
                        a_tag = a_tag.nextSibling
                    else:
                        a_tag = a_tag.nextSibling

                stars = [a.get_text() for a in a_tag.find_next_siblings("a")]
                data["directors"] = ",".join(directors)
            else:
                stars = [a.get_text() for a in credit_container.find_all("a")]

            data["stars"] = ",".join(stars)
        except AttributeError:
            pass

        if data["year"] == "I":
            continue

        movie_list.append(data)

    return movie_list


async def fetch_and_parse(session, url):
    html = await fetch(session, url)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, parse_search_page, html)


async def scrape_urls(urls):
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        trust_env=True,
    ) as session:
        return await asyncio.gather(*(fetch_and_parse(session, url) for url in urls))


def scrape(year, pages=10, user_rating=None, genre=None, data_file=DEFAULT_SCRAPE):
    multiplier = 250
    pages_multiple = pages * multiplier
    page_count = [i for i in range(1, pages_multiple, multiplier)]

    try:
        urls = []
        start_time = time.time()

        for page in page_count:
            urls.append(getSearchURL(year, page, user_rating, genre))

        data = asyncio.run(scrape_urls(urls))
        rows = [record for page_records in data for record in page_records]
        if not rows:
            raise RuntimeError(f"No IMDb movie rows parsed for year {year}.")

        scrape_df = pd.DataFrame.from_records(rows)

        if SCRAPE_DELAY_SECONDS > 0:
            time.sleep(SCRAPE_DELAY_SECONDS)

        runtime = round(time.time() - start_time, 2)
        print("\n Scraping Year:" + str(year))
        print(*urls, sep="\n")
        print("--- %s seconds ---" % runtime)

        os.makedirs(os.path.dirname(data_file), exist_ok=True)
        delete(data_file)
        scrape_df.to_csv(data_file, encoding="utf8", mode="a", index=False, header=True)

        print("Done....")
        return data_file
    except KeyboardInterrupt:
        print("Execution halted")
        raise
