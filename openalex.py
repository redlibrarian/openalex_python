import requests
import json
import os
import shutil
import csv
import openalex_record as oar

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

def total_results(results):
    return results['meta']['count']

def write_csv(records):
    with open('flagged_records.csv', 'w', encoding='utf8', newline='') as csvfile:
        fieldnames = ['title', 'pubdate', 'doi', 'authors', 'type', 'keywords', 'license', 'pdf_url', 'status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

def parse_results(data):
    items = []
    for item in data['results']:
        items.append(oar.build_record(item))
    return items

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
          oar.write_dublin_core_file(record)
          oar.fetch_pdf(record, index)
          os.chdir("..")
      else:
          flagged_records.append(record)
    write_csv(flagged_records)

    return None

