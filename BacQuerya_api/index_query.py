from elasticsearch import Elasticsearch
import json
import os
import pandas as pd
import sqlite3
from tqdm import tqdm
#from secrets import ELASTIC_API_URL, ELASTIC_GENE_NAME, ELASTIC_ISOLATE_NAME, ELASTIC_ISOLATE_API_ID, ELASTIC_ISOLATE_API_KEY, ELASTIC_GENE_API_ID, ELASTIC_GENE_API_KEY, GENE_DB, STUDY_DB

def geneQuery(searchTerm, pageNumber):
    """Search for gene in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_GENE_API_ID")
    apiKEY = os.environ.get("ELASTIC_GENE_API_KEY")
    indexName = os.environ.get("ELASTIC_GENE_NAME")
    numResults = 100
    fetchData = {"size": numResults,
                "from": numResults * pageNumber,
                "query" : {
                    "multi_match" : {
                        "query" : searchTerm,
                        "fields" : [
                            "consistentNames",
                            "panarooDescriptions",
                            "pfam_descriptions"
                        ],
                        "operator": "or",
                        "fuzziness": "AUTO",
                        }
                }
            }
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    # SQLite DB to supplement gene metadata
    sqlite_connection = sqlite3.connect(os.environ.get("GENE_DB"))
    geneResult = client.search(index = indexName,
                               body = fetchData,
                               request_timeout = 60)
    # need to add metadata to elastic search results
    searchResults = []
    for result in geneResult["hits"]["hits"]:
        db_command = 'SELECT * FROM "GENE_METADATA" WHERE "ID" = "' + str(result["_source"]["gene_index"]) + '";'
        metadataResult = sqlite_connection.execute(db_command)
        result["_source"].update({"geneMetadata": metadataResult})
        for row in metadataResult:
            result["_source"].update({"geneMetadata": row[1]})
        searchResults.append(result)
    sqlite_connection.close()
    return searchResults

def specificGeneQuery(geneList):
    #### This function is not necessary. We just need to search for a single gene name when loading the gene overview from the URL, not a list of them.
    """Search for list of genes in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_GENE_API_ID")
    apiKEY = os.environ.get("ELASTIC_GENE_API_KEY")
    indexName = os.environ.get("ELASTIC_GENE_NAME")
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    sqlite_connection = sqlite3.connect(os.environ.get("GENE_DB"))
    for geneName in geneList:
        fetchData = {"size": 10,
                        "query" : {
                            "match": {
                                "consistentNames": geneName
                                }
                            }
                        }
        geneMetadata = client.search(index = indexName,
                                     body = fetchData,
                                     request_timeout = 60)
        if not len(geneMetadata["hits"]["hits"]) == 0:
            db_command = 'SELECT * FROM "GENE_METADATA" WHERE "ID" = ' + str(geneMetadata["hits"]["hits"][0]["_source"]["gene_index"]) + ';'
            metadataResult = sqlite_connection.execute(db_command)
            for row in metadataResult:
                geneMetadata["hits"]["hits"][0]["_source"].update({"geneMetadata": row[1]})
                metadata_list.append(geneMetadata["hits"]["hits"][0])
        else:
            metadata_list.append(None)
    sqlite_connection.close()
    return metadata_list

def speciesQuery(searchTerm, pageNumber):
    """Get all species results in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    numResults = 100
    fetchData = {"size": numResults,
                "from": numResults * pageNumber,
                    "query" : {
                        "match" : {
                            "Organism_name" : searchTerm
                            }
                    }
                }
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    speciesResult = client.search(index = indexName,
                                  body = fetchData,
                                  request_timeout = 60)
    return speciesResult["hits"]["hits"]

def getFilters(searchFilters):
    filterList  = []
    if searchFilters["assemblies"] and not searchFilters["reads"]:
        filterList.append({"term": { "Genome_representation": "full"}})
    if not searchFilters["assemblies"] and searchFilters["reads"]:
        filterList.append({"term": {"Genome_representation": "reads"}})
    if not searchFilters["assemblies"] and not searchFilters["reads"]:
        filterList.append({"term": {"Genome_representation": ""}})
    if not searchFilters["noContigs"] == "All":
        filterList.append({"range": {"contig_stats.sequence_count": {"lte": int(searchFilters["noContigs"])}}})
    if not int(searchFilters["minN50"]) == 0:
        filterList.append({"range": {"contig_stats.N50": {"gte": int(searchFilters["minN50"])}}})
    if not int(searchFilters["minN50"]) == 0:
        filterList.append({"term": {"Genome_representation": ""}})
    if not searchFilters["Country"].replace(" ", "") == "All":
        # elastic indexes all terms as lowercase, even though this is not returned with the results
        filterList.append({"term": {"Country": searchFilters["Country"].lower()}})
    if not searchFilters["Year"] == "Start-End":
        years = searchFilters["Year"]
        if (years[1] == "" or years[1] == "End") and not (years[0] == "" or years[0] == "Start"):
            filterList.append({"range": {"Year": {"gte": int(years[0])}}})
        if (years[0] == "" or years[0] == "Start") and not (years[1] == "" or years[1] == "End"):
            filterList.append({"range": {"Year": {"lte": int(years[1])}}})
        if not (years[0] == "" or years[0] == "Start") and not (years[1] == "" or years[1] == "End"):
            filterList.append({"range": {"Year": {"gte": int(years[0]), "lte": int(years[1])}}})
    return filterList

def isolateQuery(searchTerm, searchFilters, pageNumber):
    """Search through isolates in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    # apply filters to the elasitcsearch output
    filterList = getFilters(searchFilters)
    numResults = 100
    fetchData = {"size": numResults,
                "from": numResults * pageNumber,
                "sort" : [
                    {"rankScore" : {"order" : "desc"}}
                ],
                "query": {
                    "bool": {
                        "must" : {
                            "multi_match" : {
                            "query":      searchTerm,
                            "fields" : [
                                "isolateName",
                                "isolateNameUnderscore",
                                "Assembly_name",
                                "Infraspecific_name",
                                "GenBank_assembly_accession",
                                "RefSeq_assembly_and_GenBank_assemblies_identical",
                                "BioSample",
                                "Organism_name",
                                "In_Silico_Serotype",
                                "Country"
                            ],
                            "operator": "or",
                            "fuzziness": "AUTO",
                        }
                    }
                }
            }
        }
    if not filterList == []:
        fetchData["query"]["bool"]["filter"] = filterList
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    isolateResult = client.search(index = indexName,
                                  body = fetchData,
                                  request_timeout = 60)

    return isolateResult["hits"]["hits"]

def specificIsolateQuery(accessionList):
    """Iterate through list of isolate biosample accessions and get metadata"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    for accession in accessionList:
        fetchData = {"size": 10,
                    "query": {
                        "bool": {
                            "must": [{
                                "match": {
                                    "BioSample": accession
                                }
                        }
                    ]}
                }
                }
        isolateMetadata = client.search(index = indexName,
                                        body = fetchData,
                                        request_timeout = 60)
        if not len(isolateMetadata["hits"]["hits"]) == 0:
            metadata_list.append(isolateMetadata["hits"]["hits"][0])
        else:
            metadata_list.append(None)
    return metadata_list

def indexAccessions(filename):
    """Read csv file posted from frontend and add genomic information to SQL database"""
    accessionDF = pd.read_csv(filename)
    DOI = filename.replace(".csv", "")
    accessions = []
    for index, row in tqdm(accessionDF.iterrows()):
        if not row["BioSample_accession"] == "" or not row["BioSample_accession"] == " ":
            accession = row["BioSample_accession"]
            accessions.append(accession)
        elif not row["NCBI_GenBank_accession"] == "" or not row["NCBI_GenBank_accession"] == " ":
            accession = row["NCBI_GenBank_accession"]
            accessions.append(accession)
        elif not row["NCBI_RefSeq_accession"] == "" or not row["NCBI_RefSeq_accession"] == " ":
            accession = row["NCBI_RefSeq_accession"]
            accessions.append(accession)
        elif not row["ENA_run_accession"] == "" or not row["ENA_run_accession"] == " ":
            accession = row["ENA_run_accession"]
            accessions.append(accession)
    # add accessions to SQLite db
    sqlite_connection = sqlite3.connect(os.environ.get("STUDY_DB"))
    sqlite_connection.execute('''CREATE TABLE STUDY_ACCESSIONS
         (DOI TEXT PRIMARY KEY     NOT NULL,
          ACCESSIONS           TEXT    NOT NULL);''')
    db_command = "INSERT INTO STUDY_ACCESSIONS (DOI,ACCESSIONS) \
                VALUES (" + DOI + ", '" + ",".join(accessions) + "')"
    sqlite_connection.execute(db_command)
    sqlite_connection.commit()
    sqlite_connection.close()

def getStudyAccessions(DOI):
    sqlite_connection = sqlite3.connect(os.environ.get("STUDY_DB"))
    db_command = 'SELECT * FROM "STUDY_ACCESSIONS" WHERE "DOI" = ' + DOI + ';'
    accessionResult = sqlite_connection.execute(db_command)
    for row in accessionResult:
        accessions = row[1].split(",")
    sqlite_connection.close()
    return accessions