import cobs_index as cobs
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import json
import os
import sys
from tqdm import tqdm
from types import SimpleNamespace
from urllib.parse import unquote

from paper_search import search_pubmed

# data locations
gene_dir = '/home/bacquerya-usr/' + os.getenv('GENE_FILES')

app = Flask(__name__, instance_relative_config=True)
app.config.update(
    TESTING=True,
    SCHEDULER_API_ENABLED=True,
    SECRET_KEY=os.environ.get('FLASK_SECRET_KEY')
)

CORS(app, expose_headers='Authorization')

@app.route('/sequence', methods=['POST'])
@cross_origin()
def postSeqResult():
    if not request.json:
        return "not a json post"
    if request.json:
        sequence_dict = request.json
        query_sequence = sequence_dict['searchTerm']
        # search for uploaded sequence in COBS index
        sys.stderr.write("\nSearching COBS index\n")
        index_name = os.path.join(gene_dir, "31_index.cobs_compact.json")
        index = cobs.Search(index_name)
        result = index.search(query_sequence, threshold = 0.8)
        # load metadata for identified sequences
        sys.stderr.write("\nLoading gene metadata\n")
        with open(os.path.join(gene_dir, "panarooPairs.json")) as f:
            geneJSON = f.read()
        genePairs = json.loads(geneJSON)
        query_length = len(query_sequence)
        kmer_length = int(os.path.basename(index_name).split("_")[0])
        result_metrics = []
        sys.stderr.write("\nExtracting metadata for COBS result\n")
        for res in tqdm(result):
            match_count = int(res.score)
            index = res.doc_name.split("_")[0]
            for k, v in genePairs.items():
                if v == int(index):
                    geneName = k
            match_proportion = round(match_count*100/((query_length-kmer_length)+1), 2)
            metrics = {"geneName": geneName, "numberMatching": match_proportion}
            result_metrics.append(metrics)
        sys.stderr.write("\nPosting results to frontend\n")
        response = {"resultMetrics" : result_metrics}
    return jsonify(response)

@app.route('/paper', methods=['POST'])
@cross_origin()
def paperSearch():
    if not request.json:
        return "not a json post"
    if request.json:
        searchDict = request.json
        searchTerm = searchDict["searchTerm"]
        maxResults = 100
        if searchDict["source"] == "URL":
            maxResults = 1
            searchTerm = unquote(searchTerm)
        searchResult = search_pubmed(searchTerm,
                                     "",
                                     maxResults)
        return jsonify({"result": searchResult})

if __name__ == "__main__":
    app.run(debug=False,use_reloader=False)

