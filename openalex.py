import requests
import json
import os
import shutil
import csv

# Module for harvesting metadata records from OpenAlex and, where the article is Open Access and the PDF is available, downloading the article PDF.
# "Clean" records are written to a an "import package" directory that conforms to the DSpace Simple Archive Format. "Flagged" records are written to a CSV for review.

BASE_URL = "https://api.openalex.org/works?filter=authorships.institutions.lineage%3Ai"
UWINNIPEG_ID = "872945872" #Uwinnipeg's publicly available OpenAlex ID.
DATA_PATH = "import_package"

# Create the import_package folder for DSpace ingest.
def create_base_dirs():
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
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

def check_pdf(pdf_url):
    if pdf_url is None:
        return "flagged"
    elif requests.get(pdf_url).ok:
        return "clean"
    else:
        return "flagged"

def total_results(results):
    return results['meta']['count']

def fetch_authors(item):
    authors = []
    for author in item['authorships']:
        authors.append(author['author']['display_name'])
    return authors

def fetch_keywords(item):
    keywords = []
    for word in item['keywords']:
        keywords.append(word['display_name'].lower())
    for word in item['concepts']:
        keywords.append(word['display_name'].lower())
    return set(keywords)

def build_record(item):
        record = dict()
        
        status = "clean" # or flagged; if clean output DSpace item directory; if flagged out put to CSV
        title = item['title']
        pubdate = item['publication_date']
        doi = item['doi']
        location = item['best_oa_location']
        authors = fetch_authors(item)
        keywords = fetch_keywords(item)
        type = item['type']
        pdf_url = item['best_oa_location']['pdf_url'] if item['best_oa_location'] else item['primary_location']['pdf_url']
        status = check_pdf(pdf_url) 

        if 'license' in item:
            license = item['license']
        else:
            license = 'no license'
        
        record = {"title": title, "pubdate": pubdate, "doi": doi, "authors": authors, "type": type, "keywords": keywords, "license": license, "pdf_url": pdf_url, "status": status}
        return record

def write_csv(records):
    with open('flagged_records.csv', 'w', encoding='utf8', newline='') as csvfile:
        fieldnames = ['title', 'pubdate', 'doi', 'authors', 'type', 'keywords', 'license', 'pdf_url', 'status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def parse_results(data):
    items = []
    for item in data['results']:
        items.append(build_record(item))
    return items

def write_dublin_core_file(record):
    with open("dublin_core.xml", "w") as outfile:
        doc = "<dublin_core>"
        doc += "<dcvalue element=\"title\">" + record["title"]+"</dcvalue>"
        doc += "<dcvalue element=\"date\" qualifier=\"issued\">" + record["pubdate"]+"</dcvalue>"
        if record['doi'] is not None:
            doc += "<dcvalue element=\"identifier\" qualifier=\"doi\">" + record["doi"]+"</dcvalue>"
        for author in record['authors']:
            doc += "<dcvalue element=\"author\">" + author + "</dcvalue>"
        for keyword in record['keywords']:
            doc += "<dcvalue element=\"keyword\">" + keyword + "</dcvalue>"
        doc += "<dcvalue element=\"type\">" + record['type'] + "</dcvalue>"
        doc += "<dvalue element=\"license\">" + record['license'] + "</dcvalue>"
        doc += "</dublin_core>"
        outfile.write(doc)

def write_dspace_data(data):
    
    if os.path.exists(DATA_PATH):
        shutil.rmtree(DATA_PATH) # start clean
    create_base_dirs()
    os.chdir(DATA_PATH)
    flagged_records = list()
    
    for index, record in enumerate(data):
      if record['status'] == "clean":
          path = f'item_{str(index).zfill(3)}'
          os.makedirs(path)
          os.chdir(path)
          write_dublin_core_file(record)
          fetch_pdf(record, index)
          os.chdir("..")
      else:
          flagged_records.append(record)
    write_csv(flagged_records)

    return None



def fetch_pdf(record, index):
    pdf_url = record['pdf_url']
    fname = f'item_{str(index).zfill(3)}.pdf'
    response = requests.get(pdf_url)
    with open(fname, 'wb') as f:
      f.write(response.content)
    
    return None

