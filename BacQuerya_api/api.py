import cobs_index as cobs
from flask import Flask, request, jsonify, send_file, url_for, render_template
from flask_cors import CORS, cross_origin
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
import json
import os
import shutil
import sys
import tempfile
from tqdm import tqdm
from types import SimpleNamespace
from urllib.parse import unquote

from paper_search import search_pubmed
from bulk_download import getDownloadLink, send_email

# data locations
gene_dir = '/home/bacquerya-usr/' + os.getenv('GENE_FILES')
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY')
app = Flask(__name__, instance_relative_config=True)
app.config.update(
    TESTING=True,
    SCHEDULER_API_ENABLED=True,
    SECRET_KEY=SECRET_KEY
)

CORS(app, expose_headers='Authorization')

@app.route('/sequence', methods=['POST'])
@cross_origin()
def postSeqResult():
    """Search through COBS index located in BacQuerya storage and return search results and metadata to frontend"""
    if not request.json:
        return "not a json post"
    if request.json:
        sequence_dict = request.json
        query_sequence = sequence_dict['searchTerm']
        # search for uploaded sequence in COBS index
        sys.stderr.write("\nSearching COBS index\n")
        index_name = os.path.join(gene_dir, "31_index.cobs_compact")
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
    """Post query to biopython entrez for pubmed search and post results to frontend"""
    if not request.json:
        return "not a json post"
    if request.json:
        searchDict = request.json
        searchTerm = searchDict["searchTerm"]
        maxResults = 100
        if searchDict["source"] == "URL":
            maxResults = 1
            searchTerm = unquote(searchTerm)
        sys.stderr.write("\nSearching for term in PubMed\n")
        searchResult = search_pubmed(searchTerm,
                                     "",
                                     maxResults)
        sys.stderr.write("\nPosting results to frontend\n")
        return jsonify({"result": searchResult})

@app.route('/bulkdownloads', methods=['POST'])
@cross_origin()
def bulkDownload():
    """Recieve list of sequenceURLs and download sequences and compress. Serve compresed file with dynamic download link"""
    if not request.json:
        return "not a json post"
    if request.json:
        output_dir = os.path.join(gene_dir, "genomic_sequences")
        n_cpu = 2
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        #temp_dir = tempfile.mkdtemp(dir=output_dir)
        #raw_temp_dir = tempfile.mkdtemp(dir=temp_dir)
        # currently hard coded and overwritten, will later change to tempdir that is removed after 24 hours
        temp_dir = os.path.join(output_dir, "requested_files")
        raw_temp_dir = os.path.join(temp_dir, "raw_files")
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)
        if not os.path.exists(raw_temp_dir):
            os.mkdir(raw_temp_dir)
        urlDict = request.json
        urlList = urlDict["sequenceURLs"]
        tarFilePath = getDownloadLink(urlList, output_dir, temp_dir, raw_temp_dir, n_cpu)
        s = Serializer(app.config['SECRET_KEY'], expires_in=60*60*24) # temporary URL live for 60 secs by 60 min by 24 hours
        token = s.dumps({'file_path': tarFilePath}).decode("utf-8")
        url_for('serve_file', token=token)
        downloadURL = "https://bacquerya.azurewebsites.net:443/downloads/" + token
        if not (urlDict["email"] == "Enter email" or urlDict["email"].replace(" ", "") == ""):
            send_email(urlDict["email"], downloadURL)
        shutil.rmtree(raw_temp_dir)
        return jsonify({"downloadURL": downloadURL})

@app.route("/downloads/<token>")
def serve_file(token):
    """Unserialise token, render appropriate html page and serve download link for compressed sequence folder"""
    s = Serializer(app.config['SECRET_KEY'])
    tarFilePath = None
    try:
        tarFilePath = s.loads(token)['file_path']
    except:
        return render_template('failed_download.html')
    if not tarFilePath:
        return render_template('failed_download.html')
    else:
        return render_template('successful_download.html', filepath=tarFilePath)

@app.route('/download_link/<path:filepath>')
def download_link(filepath):
    """Serve compressed genomic sequence file"""
    return send_file(os.path.join("..", gene_dir, "genomic_sequences", filepath), as_attachment=True)

if __name__ == "__main__":
    app.run(debug=False,use_reloader=False)

