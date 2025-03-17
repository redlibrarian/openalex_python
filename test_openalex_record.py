import pytest
import json
import openalex_record as oar

with open('test_record.json') as f:
    item = json.load(f)

def test_fetch_author():
    assert oar.fetch_authors(item) == ['Daniel Gietz', 'Andrew St. Jean', 'Robin A. Woods', 'Robert H. Schiestl']

def test_fetch_keywords():
    assert oar.fetch_keywords(item) == ['biology', 'cell biology', 'computational biology', 'gene', 'genetics', 'saccharomyces cerevisiae', 'transformation (genetics)', 'yeast']

def test_build_record():
    assert oar.build_record(item) == {'title': 'Improved method for high efficiency transformation of intact yeast cells', 'pubdate': '1992-01-01', 'doi': 'https://doi.org/10.1093/nar/20.6.1425', 'authors': ['Daniel Gietz', 'Andrew St. Jean', 'Robin A. Woods', 'Robert H. Schiestl'], 'type': 'article', 'keywords': ['biology', 'cell biology', 'computational biology', 'gene', 'genetics', 'saccharomyces cerevisiae', 'transformation (genetics)', 'yeast'], 'license': 'no license', 'pdf_url': 'https://europepmc.org/articles/pmc312198?pdf=render', 'status': 'flagged'}
