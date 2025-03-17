#Module for constructing DSpace records from OpenAlex
import requests

def check_pdf(pdf_url):
    if pdf_url is None:
        return "flagged"
    elif requests.get(pdf_url).ok:
        return "clean"
    else:
        return "flagged"

def fetch_authors(item):
    return [author['author']['display_name'] for author in item['authorships']]

def fetch_keywords(item):
    keywords = []     # list comprehension doesn't work here when one of the lists is empty.
    for word in item['keywords']:
        keywords.append(word['display_name'].lower())
    for word in item['concepts']:
        keywords.append(word['display_name'].lower())
    
    return sorted(set(keywords))

def fetch_pdf(record, index):
    pdf_url = record['pdf_url']
    fname = f'item_{str(index).zfill(3)}.pdf'
    response = requests.get(pdf_url)
    with open(fname, 'wb') as f:
        f.write(response.content)

    return None

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
    print(record)
    print()
    return record

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

