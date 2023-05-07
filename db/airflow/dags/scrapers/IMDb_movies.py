# imports
from bs4 import BeautifulSoup, element
import random
import asyncio
import time
import pandas as pd
import warnings
import os
warnings.simplefilter(action='ignore', category=FutureWarning)

# date 
from datetime import datetime
currentDateAndTime = datetime.now()
timestamp=currentDateAndTime.strftime("%Y%m%d%H%M%S")

# globals
IMDB_TITLE_URL = "https://www.imdb.com/title/tt"
IMDB_SRCH_URL = "https://www.imdb.com/search/title/?title_type=feature&languages=en&count=250"

DEFAULT_SCRAPE = f"/opt/airflow/data/imdb/{timestamp}_scrape.csv"

def convert_df(df):
   return df.to_csv().encode('utf-8')


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
        url += '&genres='+','.join(genre)
    return url

async def fetch(session, url):
    """
    Get Response from Server
    """
    async with session.get(url) as response:
        return await response.text()

def parse_search_page(html):

    movie_list = []

    # Parse the content of the request with BeautifulSoup
    page_html = BeautifulSoup(html, "html.parser")

    # Select all the 50 movie containers from a single page
    mv_containers = page_html.find_all(
        "div", class_="lister-item mode-advanced")

    # For every movie of these 50
    for container in mv_containers:

        if container.strong is not None:
            
            # If the movie has a Rating, then:
            if container.strong.text is not None:

                data = {}

                # Scrape the name
                title_id = container.a["href"].split("/")[2]
                data["imdb_title_id"] = title_id

                # Scrape the name
                name = container.h3.a.text
                data["title"] = name

                # Scrape the year
                year = (
                    container.h3.find("span", class_="lister-item-year")
                    .text.replace("(", "")
                    .replace(")", "")
                )
                if " " in year:
                    year = year.split(" ")[1]

                data["year"] = year

                # Scrape the certificate
                certificate = container.find("span", class_="certificate")
                if certificate is not None:
                    data["certificate"] = certificate.text

                # Scrape the genre
                genre = container.find("span", class_="genre")
                if genre is not None:
                    data["genre"] = genre.text.strip()

                # Scrape the runtime
                runtime = container.find("span", class_="runtime")
                if runtime is not None:
                    data["runtime"] = runtime.text

                # Scrape the description
                description = container.findAll("p", class_="text-muted")
                if description is not None:
                    data["description"] = description[1].text.strip()

                # Scrape the IMDB rating
                imdb = float(container.strong.text)
                data["IMDb_rating"] = imdb

                # Scrape the Metascore
                m_score = container.find("span", class_="metascore")
                if m_score is not None:
                    data["MetaScore"] = m_score.text

                # Scrape the number of votes
                vote = container.find("span", attrs={"name": "nv"})["data-value"]
                data["ratingCount"] = vote

                try:
                    # Scrape the directors and actors, bit wonky
                    credit_container = container.find("p", class_="")
                    a_tag = credit_container.find("a")

                    text = a_tag.previousSibling

                    stars = []

                    if text.strip() == "Director:":
                        data["directors"] = a_tag.text
                        stars = [a.get_text()
                                for a in a_tag.find_next_siblings("a")]

                    elif text.strip() == "Directors:":
                        directors = []
                        while True:
                            if isinstance(a_tag, element.Tag):
                                if a_tag.name == "span":
                                    break
                                else:
                                    # string concatenation
                                    directors.append(a_tag.text)
                                    a_tag = a_tag.nextSibling
                            else:
                                a_tag = a_tag.nextSibling

                        stars = [a.get_text()
                                for a in a_tag.find_next_siblings("a")]

                        data["directors"] = ",".join(directors)

                    else:
                        stars = [
                            a.get_text() for a in credit_container.find_all("a")
                        ]

                    data["stars"] = ",".join(stars)

                except AttributeError:
                    pass
                # append to list
                if data["year"] == 'I':
                    continue
                else:
                    movie_list.append(data)

    return movie_list


async def fetch_and_parse(session, url):

    html = await fetch(session, url)
    loop = asyncio.get_event_loop()

    # run parse(html) in a separate thread, and
    # resume this coroutine when it completes
    paras = await loop.run_in_executor(None, parse_search_page, html)
    return paras


async def scrape_urls(urls):

    headers = {"Accept-Language": "en-US, en;q=0.5"}
    async with aiohttp.ClientSession(headers=headers,trust_env=True) as session:
        return await asyncio.gather(*(fetch_and_parse(session, url) for url in urls))


def scrape(year, pages=10, user_rating=None, genre=None,data_file=DEFAULT_SCRAPE):

    scrape = pd.DataFrame()
    multiplier=250
    pages_multiple=pages*multiplier
    page_count = [i for i in range(1, pages_multiple, multiplier)]

    try:
        # Loop through years
        # for year in tqdm(years):
        data = []
        urls = []

        # show progress
        start_time = time.time()

        for page in page_count:
            urls.append(getSearchURL(year, page, user_rating, genre))

        # Asynchronously get data from server
        data = asyncio.run(scrape_urls(urls))

        # Append response to dataframe
        for rec in data:
            scrape = scrape._append(rec, ignore_index=True)
            # print(rec)
            
        # Waiting randomly to not overload server and get banned :)
        time.sleep(random.randint(7,10))
        
        #check runtime
        runtime = round(time.time() - start_time, 2)
        print("\n Scraping Year:" + str(year))
        print(*urls, sep="\n")
        print("--- %s seconds ---" % (runtime))

        delete(data_file)

        scrape.to_csv(
            data_file, encoding="utf8", mode="a", index=False, header=True
        )

        # csv = convert_df(scrape)
        print("Done....")
        return data_file

    except KeyboardInterrupt:
        print("Execution halted")
        pass


