#if output directories don't exist, create them. For "clean", create a DSPace ingest directory system; for "flagged" just create a CSV for John.

import requests
import json
import os

BASE_URL = "https://api.openalex.org/works?filter=authorships.institutions.lineage%3Ai"
UWINNIPEG_ID = "872945872"

def create_base_dirs():
    path = "import_package"
    if not os.path.exists(path):
        os.makedirs(path)
        print("Created new path.")
    else:
        print("Path already exists.")


def query(url, id, page):
    # add pagination for full result set
    # add date range
    full_url = url + str(id) + "&page=" + str(page)
    response = requests.get(full_url)
    if(response.ok):
        return json.loads(response.content)
    else:
        response.raise_for_status

def total_results(results):
    return results['meta']['count']

def build_record(item):
        status = "clean" # or flagged
        record = dict()
        title = item['title']
        pubdate = item['publication_date']
        doi = item['doi']
        authors = []
        location = item['best_oa_location']
        if item['best_oa_location'] is None:
            location = item['primary_location']
        else:
            location = item['best_oa_location']
        pdf_url = location['pdf_url']
        for author in item['authorships']:
            authors.append(author['author']['display_name'])
        if 'license' in item:
            license = item['license']
        else:
            license = "no license"
        type = item['type']
        keywords = []
        for word in item['keywords']:
            keywords.append(word['display_name'].lower())
        for word in item['concepts']:
            keywords.append(word['display_name'].lower())
        record = {"title": title, "pubdate": pubdate, "doi": doi, "authors": authors, "type": type, "keywords": set(keywords), "license": license, "pdf_url": pdf_url}
        return record

def parse_results(data):
    items = []
    for item in data['results']:
        items.append(build_record(item))
    return items




#print(query(BASE_URL, UWINNIPEG_ID, 1))
#create_base_dirs()
#print(total_results(query(BASE_URL, UWINNIPEG_ID, 1)))
print(parse_results(query(BASE_URL, UWINNIPEG_ID, 1)))
