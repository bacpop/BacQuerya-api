import datetime
from elasticsearch import Elasticsearch
import json
import os
import pandas as pd
import pyodbc
from tqdm import tqdm
#from secrets import ELASTIC_API_URL, ELASTIC_GENE_NAME, ELASTIC_ISOLATE_NAME, ELASTIC_ISOLATE_API_ID, ELASTIC_ISOLATE_API_KEY, ELASTIC_GENE_API_ID, ELASTIC_GENE_API_KEY, SQL_SERVER, SQL_DB, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

def geneQuery(searchTerm, pageNumber):
    """Search for gene in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_GENE_API_ID")
    apiKEY = os.environ.get("ELASTIC_GENE_API_KEY")
    indexName = os.environ.get("ELASTIC_GENE_NAME")
    numResults = 100
    fetchData = {"size": numResults,
                "track_total_hits": True,
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
    geneResult = client.search(index = indexName,
                               body = fetchData,
                               request_timeout = 60)
    return geneResult["hits"]["hits"], geneResult["hits"]["total"]["value"]

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
    for i in range(3):
        try:
            conn = pyodbc.connect(os.environ.get("SQL_CONNECTION_STRING"), timeout=5)
        except:
            if i < 3 - 1:
                continue
            else:
                return None
        break
    with conn.cursor() as cursor:
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
                db_command = 'SELECT * FROM "GENE_METADATA" WHERE "GENE_ID" = ' + str(geneMetadata["hits"]["hits"][0]["_source"]["gene_index"]) + ';'
                cursor.execute(db_command)
                #row = cursor.fetchone()
                for line in cursor.fetchall():
                    geneMetadata["hits"]["hits"][0]["_source"].update({"geneMetadata": json.loads(line[1])})
                    metadata_list.append(geneMetadata["hits"]["hits"][0])
            else:
                metadata_list.append(None)
    conn.close()
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
    if not searchFilters["Country"].replace(" ", "") == "All":
        # elastic indexes all terms as lowercase, even though this is not returned with the results
        filterList.append({"term": {"Country": searchFilters["Country"].lower()}})
    searchFilters["Year"] = [str(year) for year in searchFilters["Year"]]
    if not searchFilters["Year"] == ["1985", str(datetime.datetime.now().year)]:
        if (searchFilters["Year"][1] == "" or searchFilters["Year"][1] == str(datetime.datetime.now().year)) and not (searchFilters["Year"][0] == "" or searchFilters["Year"][0] == "1985"):
            filterList.append({"range": {"Year": {"gte": int(searchFilters["Year"][0])}}})
        if (searchFilters["Year"][0] == "" or searchFilters["Year"][0] == "1985") and not (searchFilters["Year"][1] == "" or searchFilters["Year"][1] == datetime.datetime.now().year):
            filterList.append({"range": {"Year": {"lte": int(searchFilters["Year"][1])}}})
        if not (searchFilters["Year"][0] == "" or searchFilters["Year"][0] == "1985") and not (searchFilters["Year"][1] == "" or searchFilters["Year"][1] == datetime.datetime.now().year):
            filterList.append({"range": {"Year": {"gte": int(searchFilters["Year"][0]), "lte": int(searchFilters["Year"][1])}}})
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
                "track_total_hits": True,
                "from": numResults * pageNumber,
                "sort" : [
                    {"_score": {"order": "desc"}},
                    {"rankScore" : {"order" : "desc"}}
                ],
                "query": {
                    "bool": {
                        "must" : {
                            "multi_match" : {
                            "query": searchTerm,
                            "fields" : [
                                "isolateName",
                                "isolateNameUnderscore",
                                "Assembly_name",
                                "Infraspecific_name",
                                "GenBank_assembly_accession",
                                "RefSeq_assembly_and_GenBank_assemblies_identical",
                                "BioSample",
                                "read_accession",
                                "run_accession",
                                "Taxid",
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
        print(filterList)
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    isolateResult = client.search(index = indexName,
                                  body = fetchData,
                                  request_timeout = 60)
    return isolateResult["hits"]["hits"], isolateResult["hits"]["total"]["value"]

def specificIsolateQuery(accessionList):
    """Iterate through list of isolate biosample accessions and get metadata"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    for i in range(3):
        try:
            conn = pyodbc.connect(os.environ.get("SQL_CONNECTION_STRING"), timeout=5)
        except:
            if i < 3 - 1:
                continue
            else:
                return None
        break
    with conn.cursor() as cursor:
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
                db_command = 'SELECT * FROM "ISOLATE_METADATA" WHERE "ISOLATE_ID" = ' + str(isolateMetadata["hits"]["hits"][0]["_source"]["isolate_index"]) + ';'
                cursor.execute(db_command)
                for line in cursor.fetchall():
                    isolateMetadata["hits"]["hits"][0]["_source"].update(json.loads(line[1]))
                    metadata_list.append(isolateMetadata["hits"]["hits"][0])
                metadata_list.append(isolateMetadata["hits"]["hits"][0])
            else:
                metadata_list.append(None)
    conn.close()
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
    biosamples = []
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    for access in accessions:
        # apply filters to the elasitcsearch output
        numResults = 10
        fetchData = {"size": numResults,
                    "track_total_hits": True,
                    "query": {
                        "bool": {
                            "must" : {
                                "multi_match" : {
                                "query": access,
                                "fields" : [
                                    "isolateName",
                                    "isolateNameUnderscore",
                                    "Assembly_name",
                                    "Infraspecific_name",
                                    "GenBank_assembly_accession",
                                    "RefSeq_assembly_and_GenBank_assemblies_identical",
                                    "BioSample",
                                    "read_accession",
                                    "run_accession",
                                ],
                                "operator": "or",
                                "fuzziness": "AUTO",
                            }
                        }
                    }
                }
            }
        client = Elasticsearch([searchURL],
                                api_key=(apiID, apiKEY))
        isolateResult = client.search(index = indexName,
                                        body = fetchData,
                                        request_timeout = 60)
        if not len(isolateResult["hits"]["hits"]) == 0:
            bio = isolateResult["hits"]["hits"][0]["_source"]["BioSample"]
            biosamples.append(bio)
    with pyodbc.connect(os.environ.get("SQL_CONNECTION_STRING")) as conn:
        with conn.cursor() as cursor:
            db_command = '''CREATE TABLE STUDY_ACCESSIONS
                (DOI TEXT PRIMARY KEY   NOT NULL,
                ACCESSIONS  TEXT    NOT NULL);'''
            db_command = "INSERT INTO STUDY_ACCESSIONS (DOI,ACCESSIONS) \
                VALUES (" + DOI + ", '" + ",".join(biosamples) + "')"
            cursor.execute(db_command)

def getStudyAccessions(DOI):
    with pyodbc.connect(os.environ.get("SQL_CONNECTION_STRING")) as conn:
        with conn.cursor() as cursor:
            db_command = 'SELECT * FROM "STUDY_ACCESSIONS" WHERE "DOI" = ' + DOI + ';'
            cursor.execute(db_command)
            #row = cursor.fetchone()
            try:
                for row in cursor.fetchall():
                    accessions = row[1].split(",")
                    return accessions
            except:
                return False
