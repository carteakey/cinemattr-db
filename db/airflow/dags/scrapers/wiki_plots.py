import mwclient  # for downloading example Wikipedia articles
import mwparserfromhell  # for splitting Wikipedia articles into sections
import pandas as pd  # for DataFrames to store article sections and embeddings
import re  # for cutting <ref> links out of Wikipedia articles

from datetime import datetime
currentDateAndTime = datetime.now()
timestamp=currentDateAndTime.strftime("%Y%m%d%H%M%S")
DEFAULT_SCRAPE = f"/opt/airflow/data/wiki/{timestamp}_scrape.csv"

WIKI_SITE = "en.wikipedia.org"
SECTIONS_TO_INCLUDE= [ 
    "Plot",
    "External links"
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

def titles_from_category(
    category: mwclient.listing.Category, max_depth: int
) :
    """Return a set of page titles in a given Wiki category and its subcategories."""
    titles = set()
    for cm in category.members():
        if type(cm) == mwclient.page.Page:
            # ^type() used instead of isinstance() to catch match w/ no inheritance
            titles.add(cm.name)
        elif isinstance(cm, mwclient.listing.Category) and max_depth > 0:
            deeper_titles = titles_from_category(cm, max_depth=max_depth - 1)
            titles.update(deeper_titles)
    return titles

# define functions to split Wikipedia pages into sections
def all_subsections_from_section(
    section: mwparserfromhell.wikicode.Wikicode,
    parent_titles ,
    sections_to_ignore ,
    sections_to_include
):
    """
    From a Wikipedia section, return a flattened list of all nested subsections.
    Each subsection is a tuple, where:
        - the first element is a list of parent subtitles, starting with the page title
        - the second element is the text of the subsection (but not any children)
    """
    headings = [str(h) for h in section.filter_headings()]
    title = headings[0]
    
    #if title.strip("=" + " ") in sections_to_ignore:
    # ^wiki headings are wrapped like "== Heading =="
    #   return []

    if title.strip("=" + " ") not in sections_to_include:
        # ^wiki headings are wrapped like "== Heading =="
        return []
    
    titles = parent_titles + [title]
    full_text = str(section)
    section_text = full_text.split(title)[1]
    if len(headings) == 1:
        return [(titles, section_text)]
    else:
        first_subtitle = headings[1]
        section_text = section_text.split(first_subtitle)[0]
        results = [(titles, section_text)]
        for subsection in section.get_sections(levels=[len(titles) + 1]):
            results.extend(all_subsections_from_section(subsection, titles, sections_to_ignore,sections_to_include))
        return results


def all_subsections_from_title(
    title,
    sections_to_ignore = SECTIONS_TO_IGNORE,
    sections_to_include = SECTIONS_TO_INCLUDE,
    site_name = WIKI_SITE,
):
    
    """From a Wikipedia page title, return a flattened list of all nested subsections.
    Each subsection is a tuple, where:
        - the first element is a list of parent subtitles, starting with the page title
        - the second element is the text of the subsection (but not any children)
    """
    site = mwclient.Site(site_name)
    page = site.pages[title]
    text = page.text()
    parsed_text = mwparserfromhell.parse(text)
    headings = [str(h) for h in parsed_text.filter_headings()]
    if headings:
        summary_text = str(parsed_text).split(headings[0])[0]
    else:
        summary_text = str(parsed_text)
    results = [([title], summary_text)]
    for subsection in parsed_text.get_sections(levels=[2]):
        results.extend(all_subsections_from_section(subsection, [title], sections_to_ignore,sections_to_include))
    return results


# clean text
def clean_section(section):
    """
    Return a cleaned up section with:
        - <ref>xyz</ref> patterns removed
        - leading/trailing whitespace removed
    """
    titles, text = section
    text = re.sub(r"<ref.*?</ref>", "", text)
    text = text.replace("[[","")
    text = text.replace("]]","")
    text = text.strip()
    return (titles, text)


def scrape(year,data_file=DEFAULT_SCRAPE):
    CATEGORY_TITLE = f"Category:{str(year)} films"
    print(CATEGORY_TITLE)
    site = mwclient.Site(WIKI_SITE)
    category_page = site.pages[CATEGORY_TITLE]
    print(category_page)
    titles = titles_from_category(category_page, max_depth=1)
    # ^note: max_depth=1 means we go one level deep in the category tree
    print(f"Found {len(titles)} article titles in {CATEGORY_TITLE}.")
    
    # split pages into sections
    # may take ~1 minute per 100 articles
    wikipedia_sections = []
    titles_list = list(titles)
    for title in titles_list:
        wikipedia_sections.extend(all_subsections_from_title(title))
    print(f"Found {len(wikipedia_sections)} sections in {len(titles)} pages.")

    data = []
    movie_name = None
    summary = None
    plot = None
    external_links = None
    for ws in wikipedia_sections:
        if ws[0][0] != movie_name:  # new movie
            if not summary:
                summary = "No summary available"
            if not plot:
                plot = "No plot available"
            if not external_links:
                external_links = "No external links available"
            if movie_name:  # append previous movie data
                data.append([movie_name, summary, plot, external_links])
            movie_name = ws[0][0]
            summary = ws[1]
            plot = None
            external_links = None

        else:
            if str(ws[0][1]).replace("=","").strip() == "Plot":
                plot = ws[1]
            elif str(ws[0][1]).replace("=","").strip() == "External links":
                external_links = ws[1]
            
    # append last movie data
    data.append([movie_name, summary, plot, external_links])

    df = pd.DataFrame(data, columns=['title', 'summary', 'plot', 'external_links'])
    df.to_csv(
            data_file, encoding="utf8", mode="a", index=False, header=True
        )
    
    return data_file
