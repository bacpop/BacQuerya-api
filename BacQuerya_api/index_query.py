from elasticsearch import Elasticsearch
import os

from secrets import ELASTIC_API_URL, ELASTIC_GENE_NAME, ELASTIC_ISOLATE_NAME, ELASTIC_ISOLATE_API_ID, ELASTIC_ISOLATE_API_KEY, ELASTIC_GENE_API_ID, ELASTIC_GEME_API_KEY

def geneQuery(searchTerm):
    """Search for gene in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_ENDPOINT")
    apiID = os.environ.get("GENE_INDEX_API_ID")
    apiKEY = os.environ.get("GENE_INDEX_API_KEY")
    indexName = os.environ.get("GENE_INDEX_NAME")
    searchURL = ELASTIC_API_URL
    apiID = ELASTIC_GENE_API_ID
    apiKEY = ELASTIC_GEME_API_KEY
    indexName = ELASTIC_GENE_NAME
    fetchData = {"size": 10000,
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
                               body = fetchData)
    return geneResult["hits"]["hits"]

def specificGeneQuery(geneList):
    """Search for list of genes in elastic gene index"""
    earchURL = os.environ.get("ELASTIC_ENDPOINT")
    apiID = os.environ.get("GENE_INDEX_API_ID")
    apiKEY = os.environ.get("GENE_INDEX_API_KEY")
    indexName = os.environ.get("GENE_INDEX_NAME")
    searchURL = ELASTIC_API_URL
    apiID = ELASTIC_GENE_API_ID
    apiKEY = ELASTIC_GEME_API_KEY
    indexName = ELASTIC_GENE_NAME
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    for geneName in geneList:
        fetchData = {"size": 10000,
                    "query" : {
                        "match": {
                            "consistentNames": geneName
                            }
                        }
                    }
        geneMetadata = client.search(index = indexName,
                                     body = fetchData)
        if not len(geneMetadata["hits"]["hits"]) == 0:
            metadata_list.append(geneMetadata["hits"]["hits"][0])
        else:
            metadata_list.append(None)
    return metadata_list

def speciesQuery(searchTerm):
    """Get all species results in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_ENDPOINT")
    apiID = os.environ.get("ISOLATE_INDEX_API_ID")
    apiKEY = os.environ.get("ISOLATE_INDEX_API_KEY")
    indexName = os.environ.get("ISOLATE_INDEX_NAME")
    searchURL = ELASTIC_API_URL
    apiID = ELASTIC_ISOLATE_API_ID
    apiKEY = ELASTIC_ISOLATE_API_KEY
    indexName = ELASTIC_ISOLATE_NAME
    fetchData = {"size": 10000,
                "query" : {
                    "match" : {
                        "Organism_name" : searchTerm
                        }
                }
            }
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    speciesResult = client.search(index = indexName,
                                  body = fetchData)
    return speciesResult["hits"]["hits"]

def isolateQuery(searchTerm):
    """Search through isolates in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_ENDPOINT")
    apiID = os.environ.get("ISOLATE_INDEX_API_ID")
    apiKEY = os.environ.get("ISOLATE_INDEX_API_KEY")
    indexName = os.environ.get("ISOLATE_INDEX_NAME")
    searchURL = ELASTIC_API_URL
    apiID = ELASTIC_ISOLATE_API_ID
    apiKEY = ELASTIC_ISOLATE_API_KEY
    indexName = ELASTIC_ISOLATE_NAME
    fetchData = {"size": 10000,
                    "query" : {
                        "multi_match" : {
                            "query" : searchTerm,
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
                    },
                }
            }
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    isolateResult = client.search(index = indexName,
                                  body = fetchData)
    return isolateResult["hits"]["hits"]

def specificIsolateQuery(accessionList):
    """Iterate through list of isolate biosample accessions and get metadata"""
    searchURL = os.environ.get("ELASTIC_ENDPOINT")
    apiID = os.environ.get("ISOLATE_INDEX_API_ID")
    apiKEY = os.environ.get("ISOLATE_INDEX_API_KEY")
    indexName = os.environ.get("ISOLATE_INDEX_NAME")
    searchURL = ELASTIC_API_URL
    apiID = ELASTIC_ISOLATE_API_ID
    apiKEY = ELASTIC_ISOLATE_API_KEY
    indexName = ELASTIC_ISOLATE_NAME
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    for accession in accessionList:
        fetchData = {"size": 10000,
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
                                        body = fetchData)
        if not len(isolateMetadata["hits"]["hits"]) == 0:
            metadata_list.append(isolateMetadata["hits"]["hits"][0])
        else:
            metadata_list.append(None)
    return metadata_list
