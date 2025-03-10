import requests
import json
import os
import shutil
import csv


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

def check_pdf(pdf_url):
    response = requests.get(pdf_url)
    return response.ok

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
        status = "clean" # or flagged; if clean output DSpace item directory; if flagged out put to CSV
        record = dict()
        title = item['title']
        pubdate = item['publication_date']
        doi = item['doi']
        location = item['best_oa_location']
        authors = fetch_authors(item)
        keywords = fetch_keywords(item)
        type = item['type']
        
        if item['best_oa_location'] is None:
            location = item['primary_location']
        else:
            location = item['best_oa_location']
        pdf_url = location['pdf_url']
        
        if pdf_url is None:
            is_oa = "False"
        else:
            is_oa = check_pdf(pdf_url) # currently two API calls to pdf_url; possible to refactor to just one?
        
        if is_oa == "False":
            status = "flagged"
        
        if 'license' in item:
            license = item['license']
        else:
            license = "no license"
        
        record = {"title": title, "pubdate": pubdate, "doi": doi, "authors": authors, "type": type, "keywords": keywords, "license": license, "pdf_url": pdf_url, "is_oa": is_oa, "status": status}
        return record

def write_csv(records):
    with open('flagged_records.csv', 'w', encoding='utf8', newline='') as csvfile:
        fieldnames = ['title', 'pubdate', 'doi', 'authors', 'type', 'keywords', 'license', 'pdf_url', 'is_oa', 'status']
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
    if os.path.exists("import_package"):
        shutil.rmtree("import_package") # start clean
    create_base_dirs()
    os.chdir("import_package")
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
    #pdf_url = "https://europepmc.org/articles/pmc312198?pdf=render"
    pdf_url = record['pdf_url']
    fname = f'item_{str(index).zfill(3)}.pdf'
    response = requests.get(pdf_url)
    with open(fname, 'wb') as f:
      f.write(response.content)
    
    return None





#print(query(BASE_URL, UWINNIPEG_ID, 1))
#create_base_dirs()
#print(total_results(query(BASE_URL, UWINNIPEG_ID, 1)))a
#print(parse_results(query(BASE_URL, UWINNIPEG_ID, 1)))
write_dspace_data(parse_results(query(BASE_URL, UWINNIPEG_ID, 1)))
#fetch_url('test')
