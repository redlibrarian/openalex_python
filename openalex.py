import requests
import json
import os
import shutil


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
        status = "clean" # or flagged; if # clean output DSpace item directory; if flagged out put to CSV
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
            is_oa = check_pdf(pdf_url)
        
        if is_oa == "False":
            status = "flagged"
        
        if 'license' in item:
            license = item['license']
        else:
            license = "no license"
        
        record = {"title": title, "pubdate": pubdate, "doi": doi, "authors": authors, "type": type, "keywords": keywords, "license": license, "pdf_url": pdf_url, "is_oa": is_oa, "status": status}
        return record

def parse_results(data):
    items = []
    for item in data['results']:
        items.append(build_record(item))
    return items

def write_dublin_core_file(record):
    with open("dublin_core.xml", "w") as outfile:
        doc = "<dublin_core>"
        doc += "<dcvalue element=\"title\" qualifier=\"none\">" + record["title"]+"</dcvalue>"
        doc += "<dcvalue element=\"date\" qualifier=\"none\">" + record["pubdate"]+"</dcvalue>"
        if record['doi'] is not None:
            doc += "<dcvalue element=\"identifier\" qualifier=\"none\">" + record["doi"]+"</dcvalue>"
        doc += "</dublin_core>"
        outfile.write(doc)

def write_dspace_data(data):
    #shutil.rmtree("import_package") # start clean
    create_base_dirs()
    os.chdir("import_package")

    for index, record in enumerate(data):
        path = f'item_{str(index).zfill(3)}'
        print(path)
        os.makedirs(path)
        os.chdir(path)
        write_dublin_core_file(record)
        os.chdir("..")

    return None





#print(query(BASE_URL, UWINNIPEG_ID, 1))
#create_base_dirs()
#print(total_results(query(BASE_URL, UWINNIPEG_ID, 1)))a
#print(parse_results(query(BASE_URL, UWINNIPEG_ID, 1)))
write_dspace_data(parse_results(query(BASE_URL, UWINNIPEG_ID, 1)))
