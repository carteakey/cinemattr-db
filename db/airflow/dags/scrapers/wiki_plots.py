import os
import re
from datetime import datetime

import mwclient
import mwparserfromhell
import pandas as pd

currentDateAndTime = datetime.now()
timestamp = currentDateAndTime.strftime("%Y%m%d%H%M%S")
DEFAULT_SCRAPE = f"/opt/airflow/data/wiki/{timestamp}_scrape.csv"

WIKI_SITE = "en.wikipedia.org"
WIKI_MAX_CATEGORY_DEPTH = int(os.getenv("WIKI_MAX_CATEGORY_DEPTH", "1"))
WIKI_USER_AGENT = os.getenv("WIKI_USER_AGENT", "cinemattr-db/2.0")
SECTIONS_TO_INCLUDE = [
    "Plot",
    "External links",
]
SECTIONS_TO_IGNORE = [
    "See also",
    "References",
    "External links",
    "Further reading",
    "Footnotes",
    "Bibliography",
    "Sources",
    "Citations",
    "Literature",
    "Footnotes",
    "Notes and references",
    "Photo gallery",
    "Works cited",
    "Photos",
    "Gallery",
    "Notes",
    "References and sources",
    "References and notes",
]


def titles_from_category(category: mwclient.listing.Category, max_depth: int):
    titles = set()
    for cm in category.members():
        if type(cm) == mwclient.page.Page:
            titles.add(cm.name)
        elif isinstance(cm, mwclient.listing.Category) and max_depth > 0:
            titles.update(titles_from_category(cm, max_depth=max_depth - 1))
    return titles


def all_subsections_from_section(
    section: mwparserfromhell.wikicode.Wikicode,
    parent_titles,
    sections_to_ignore,
    sections_to_include,
):
    headings = [str(h) for h in section.filter_headings()]
    title = headings[0]

    if title.strip("=" + " ") not in sections_to_include:
        return []

    titles = parent_titles + [title]
    full_text = str(section)
    section_text = full_text.split(title)[1]
    if len(headings) == 1:
        return [(titles, section_text)]

    first_subtitle = headings[1]
    section_text = section_text.split(first_subtitle)[0]
    results = [(titles, section_text)]
    for subsection in section.get_sections(levels=[len(titles) + 1]):
        results.extend(
            all_subsections_from_section(
                subsection, titles, sections_to_ignore, sections_to_include
            )
        )
    return results


def clean_section(section):
    titles, text = section
    text = re.sub(r"<ref.*?</ref>", "", text)
    text = text.replace("[[", "")
    text = text.replace("]]", "")
    text = re.sub(r"\s+", " ", text).strip()
    return (titles, text)


def all_subsections_from_title(
    title,
    sections_to_ignore=SECTIONS_TO_IGNORE,
    sections_to_include=SECTIONS_TO_INCLUDE,
    site_name=WIKI_SITE,
):
    site = mwclient.Site(site_name, clients_useragent=WIKI_USER_AGENT)
    page = site.pages[title]
    text = page.text()
    parsed_text = mwparserfromhell.parse(text)
    headings = [str(h) for h in parsed_text.filter_headings()]
    if headings:
        summary_text = str(parsed_text).split(headings[0])[0]
    else:
        summary_text = str(parsed_text)

    results = [clean_section(([title], summary_text))]
    for subsection in parsed_text.get_sections(levels=[2]):
        results.extend(
            [
                clean_section(section)
                for section in all_subsections_from_section(
                    subsection, [title], sections_to_ignore, sections_to_include
                )
            ]
        )
    return results


def scrape(year, data_file=DEFAULT_SCRAPE):
    category_title = f"Category:{str(year)} films"
    print(category_title)
    site = mwclient.Site(WIKI_SITE, clients_useragent=WIKI_USER_AGENT)
    category_page = site.pages[category_title]
    print(category_page)
    titles = titles_from_category(category_page, max_depth=WIKI_MAX_CATEGORY_DEPTH)
    print(f"Found {len(titles)} article titles in {category_title}.")
    if not titles:
        raise RuntimeError(f"No Wikipedia titles found for {category_title}.")

    wikipedia_sections = []
    for title in sorted(titles):
        try:
            wikipedia_sections.extend(all_subsections_from_title(title))
        except Exception as exc:
            print(f"Skipping Wikipedia title '{title}': {exc}")

    print(f"Found {len(wikipedia_sections)} sections in {len(titles)} pages.")
    if not wikipedia_sections:
        raise RuntimeError(f"No Wikipedia sections parsed for {category_title}.")

    data = []
    movie_name = None
    summary = None
    plot = None
    external_links = None
    for ws in wikipedia_sections:
        if ws[0][0] != movie_name:
            if movie_name:
                data.append(
                    [
                        movie_name,
                        summary or "No summary available",
                        plot or "No plot available",
                        external_links or "No external links available",
                    ]
                )
            movie_name = ws[0][0]
            summary = ws[1]
            plot = None
            external_links = None
        elif str(ws[0][1]).replace("=", "").strip() == "Plot":
            plot = ws[1]
        elif str(ws[0][1]).replace("=", "").strip() == "External links":
            external_links = ws[1]

    if movie_name:
        data.append(
            [
                movie_name,
                summary or "No summary available",
                plot or "No plot available",
                external_links or "No external links available",
            ]
        )

    if not data:
        raise RuntimeError(f"No Wikipedia movie rows assembled for {category_title}.")

    df = pd.DataFrame(data, columns=["title", "summary", "plot", "external_links"])
    os.makedirs(os.path.dirname(data_file), exist_ok=True)
    df.to_csv(data_file, encoding="utf8", mode="a", index=False, header=True)
    return data_file
