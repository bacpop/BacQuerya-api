from elasticsearch import Elasticsearch
import os

#from secrets import ELASTIC_API_URL, ELASTIC_GENE_NAME, ELASTIC_ISOLATE_NAME, ELASTIC_ISOLATE_API_ID, ELASTIC_ISOLATE_API_KEY, ELASTIC_GENE_API_ID, ELASTIC_GENE_API_KEY

def geneQuery(searchTerm):
    """Search for gene in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_GENE_API_ID")
    apiKEY = os.environ.get("ELASTIC_GENE_API_KEY")
    indexName = os.environ.get("ELASTIC_GENE_NAME")
    fetchData = {"size": 1000,
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
    return geneResult["hits"]["hits"]

def specificGeneQuery(geneList):
    """Search for list of genes in elastic gene index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_GENE_API_ID")
    apiKEY = os.environ.get("ELASTIC_GENE_API_KEY")
    indexName = os.environ.get("ELASTIC_GENE_NAME")
    metadata_list = []
    client = Elasticsearch([searchURL],
                           api_key=(apiID, apiKEY))
    for geneName in geneList:
        fetchData = {"size": 1000,
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
            metadata_list.append(geneMetadata["hits"]["hits"][0])
        else:
            metadata_list.append(None)
    return metadata_list

def speciesQuery(searchTerm):
    """Get all species results in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
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

def isolateQuery(searchTerm, searchFilters):
    """Search through isolates in elastic isolate index"""
    searchURL = os.environ.get("ELASTIC_API_URL")
    apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
    apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
    indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
    # apply filters to the elasitcsearch output
    filterList = getFilters(searchFilters)
    fetchData = {"size": 1000,
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
        fetchData = {"size": 1000,
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
