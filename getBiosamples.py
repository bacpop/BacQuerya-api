from tqdm import tqdm
from elasticsearch import Elasticsearch
import os

with open("SPARC_accessions.txt", "r") as inFile:
    accessions = inFile.read().splitlines()

biosamples = []
searchURL = os.environ.get("ELASTIC_API_URL")
apiID = os.environ.get("ELASTIC_ISOLATE_API_ID")
apiKEY = os.environ.get("ELASTIC_ISOLATE_API_KEY")
indexName = os.environ.get("ELASTIC_ISOLATE_NAME")
for access in tqdm(accessions):
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
        try:
            bio = isolateResult["hits"]["hits"][0]["_source"]["BioSample"]
            biosamples.append(bio)
        except:
            print(isolateResult["hits"]["hits"][0]["_source"].keys())

print(len(biosamples))