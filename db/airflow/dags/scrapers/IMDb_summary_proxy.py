# imports
from bs4 import BeautifulSoup, element
import random
import time
import pandas as pd
import warnings
import os
import requests
from urllib.parse import urlencode
warnings.simplefilter(action='ignore', category=FutureWarning)

# date 
from datetime import datetime
currentDateAndTime = datetime.now()
timestamp=currentDateAndTime.strftime("%Y%m%d%H%M%S")

# globals
IMDB_TITLE_URL = "https://www.imdb.com/title/tt"
DEFAULT_SCRAPE = f"/opt/airflow/data/imdb_wiki/{timestamp}_scrape.csv"

def convert_df(df):
   return df.to_csv().encode('utf-8')

def delete(filename):
    if os.path.exists(filename) and os.path.isfile(filename):
        os.remove(filename)

def getPlotURL(title_id):
    return IMDB_TITLE_URL + title_id + "/plotsummary"

def fetch(url):
    """
    Get Response from Server
    """
    url_ant = 'https://api.scrapingant.com/v2/general'
    params = {'url': url, 'x-api-key': os.environ['SCRAPINGANT_API_KEY'], 'browser': 'false'}
    query_string = urlencode(params)
    full_url = f"{url_ant}?{query_string}"
    print(full_url)
    with requests.get(full_url) as response:
        return response.text

def parse_plot_page(html):
    data = {}
    # Parse the content of the request with BeautifulSoup
    page_html = BeautifulSoup(html, "html.parser")

    #get the section with data-test-id='sub-section-summaries'
    div_summaries = page_html.find("div", attrs={"data-testid": "sub-section-summaries"})

    #get the section with data-test-id='sub-section-synopsis'
    div_synopsis = page_html.find("div", attrs={"data-testid": "sub-section-synopsis"})

    #Remove the <span data-reactroot="" element from the div_summaries
    if div_summaries is not None:
        span_summaries = div_summaries.find_all("span", attrs={"data-reactroot": ""})
        if span_summaries is not None:
            for span in span_summaries:
                span.decompose()

    #Extract the text from the div_summaries
    if div_summaries is not None:
        text_summaries = div_summaries.text
    else:
        text_summaries = ''

    #Extract the text from the div_synopsis
    if div_synopsis is not None:
        text_synopsis = div_synopsis.text
    else:
        text_synopsis = ''
    
    #Create a dictionary with the data
    data = {
        'plot_summary': text_summaries,
        'plot_synopsis': text_synopsis
    }
    return data

def fetch_and_parse(url):

    html = fetch(url)
    paras = parse_plot_page(html)
    return paras


def scrape(title_ids,data_file=DEFAULT_SCRAPE):
    scrape = pd.DataFrame()
    for title_id in title_ids:
        url = getPlotURL(title_id)
        movie_data = {}
        movie_data['imdb_title_id'] = title_id
        # Asynchronously get data from server
        data = fetch_and_parse(url)
        movie_data['summary'] = data['plot_summary']
        movie_data['plot'] = data['plot_synopsis']

        # Append response to dataframe
        scrape = scrape.append(movie_data, ignore_index=True)

    scrape.to_csv(
            data_file, encoding="utf8", mode="a", index=False, header=True
        )
    
    return data_file

if __name__=='__main__':
    ids = ['0110912', '0133093', '0120737', '0068646']
    scrape(ids)
