#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script uses Biopython ENTREZ to retrieve and download information of interest available through NCBI.
"""
from Bio import Entrez
import os
from tqdm import tqdm
from urllib.parse import quote

def search_pubmed(searchTerm,
                  email,
                  number):
    """Use Biopython Entrez to search for study query in pubmed"""
    Entrez.email = email
    Entrez.api_key = os.environ.get("ENTREZ_API_KEY")
    handle = Entrez.read(Entrez.esearch(db="pubmed", term=searchTerm, retmax = number))
    idList = handle["IdList"]
    searchResult = []
    for idTerm in tqdm(idList):
        try:
            esummary_handle = Entrez.esummary(db="pubmed", id=idTerm, report="full")
            esummary_record = Entrez.read(esummary_handle, validate = False)
            encodedDOI = quote(esummary_record[0]["DOI"], safe='')
            esummary_record[0]["encodedDOI"] = encodedDOI
            searchResult += esummary_record
        except:
            pass
    return searchResult

if __name__ == '__main__':
    search_pubmed("", "email", 100)