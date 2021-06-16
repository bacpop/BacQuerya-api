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
from urllib.parse import unquote
from werkzeug.utils import secure_filename

from study_search import search_pubmed
from bulk_download import getDownloadLink, send_email
from index_query import geneQuery, specificGeneQuery, speciesQuery, isolateQuery, specificIsolateQuery, indexAccessions, getStudyAccessions

# data locations
gene_dir = '/home/bacquerya-usr/' + os.environ.get('GENE_FILES')
#gene_dir = "gene_test_files"
gene_database = os.path.join(gene_dir, os.environ.get('GENE_DB'))
study_database = os.path.join(gene_dir, os.environ.get('STUDY_DB'))
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY')
app = Flask(__name__, instance_relative_config=True)
app.config.update(
    TESTING=True,
    SCHEDULER_API_ENABLED=True,
    SECRET_KEY=SECRET_KEY
)

CORS(app, expose_headers='Authorization')

@app.route('/geneQuery', methods=['POST'])
@cross_origin()
def queryGeneIndex():
    """Query search term in gene elastic index"""
    if not request.json:
        return "not a json post"
    if request.json:
        searchDict = request.json
        searchTerm = searchDict["searchTerm"]
        searchType = searchDict["searchType"]
        if "pageNumber" in searchDict.keys():
            pageNumber = int(searchDict["pageNumber"]) - 1
        else:
            pageNumber = 0
        if searchType == "gene":
            searchResult = geneQuery(searchTerm, pageNumber, gene_database)
        elif searchType == "consistentNameList":
            searchResult = specificGeneQuery(searchTerm, gene_database)
        return jsonify({"searchResult": searchResult})

@app.route('/isolateQuery', methods=['POST'])
@cross_origin()
def queryIsolateIndex():
    """Query search term in isolate elastic index"""
    if not request.json:
        return "not a json post"
    if request.json:
        searchDict = request.json
        searchTerm = searchDict["searchTerm"]
        searchType = searchDict["searchType"]
        if "pageNumber" in searchDict.keys():
            pageNumber = int(searchDict["pageNumber"]) - 1
        else:
            pageNumber = 0
        if searchType == "species":
            searchResult = speciesQuery(searchTerm, pageNumber)
        elif searchType == "isolate":
            searchFilters = searchDict["searchFilters"]
            searchResult = isolateQuery(searchTerm, searchFilters, pageNumber)
        elif searchType == "biosampleList":
            searchResult = specificIsolateQuery(searchTerm)
        return jsonify({"searchResult": searchResult})

@app.route('/sequence', methods=['POST'])
@cross_origin()
def postSeqResult():
    """Search through COBS index located in BacQuerya storage and return search results and metadata to frontend"""
    if not request.json:
        return "not a json post"
    if request.json:
        sequence_dict = request.json
        query_sequence = sequence_dict['searchTerm'].replace(" ", "").upper()
        # search for uploaded sequence in COBS index
        sys.stderr.write("\nSearching COBS index\n")
        index_name = os.path.join(gene_dir, "31_index.cobs_compact")
        index = cobs.Search(index_name)
        result = index.search(query_sequence, threshold = 0.8)
        # load metadata for identified sequences
        query_length = len(query_sequence)
        kmer_length = int(os.path.basename(index_name).split("_")[0])
        sys.stderr.write("\nExtracting metadata for COBS result\n")
        # return only unique gene names and the highest match proportion for duplicate gene names in search results
        no_duplicates = {}
        for res in tqdm(result):
            match_count = int(res.score)
            geneName = res.doc_name.split("_v")[0]
            match_proportion = round(match_count*100/((query_length-kmer_length)+1), 2)
            if not geneName in no_duplicates:
                no_duplicates[geneName] = match_proportion
            else:
                if no_duplicates[geneName] < match_proportion:
                    no_duplicates[geneName] = match_proportion
        result_metrics = []
        for key, value in no_duplicates.items():
            metrics = {"geneName": key, "numberMatching": value}
            result_metrics.append(metrics)
        sys.stderr.write("\nPosting results to frontend\n")
        response = {"resultMetrics" : result_metrics}
    return jsonify(response)

@app.route('/study', methods=['POST'])
@cross_origin()
def studySearch():
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
    """Receive list of sequenceURLs and download sequences and compress. Serve compresed file with dynamic download link"""
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
        urls = urlDict["sequenceURLs"]
        urlList = []
        for url in urls:
            if isinstance(url, list):
                urlList += url
            else:
                urlList.append(url)
        if len(urlList) <= 100:
            tarFilePath = getDownloadLink(urlList, output_dir, temp_dir, raw_temp_dir, n_cpu)
        else:
            tarFilePath = os.path.join("genomic_sequences", "sequenceURLs.txt")
            with open(os.path.join(output_dir, "sequenceURLs.txt"), "w") as outSequences:
                outSequences.write("\n".join(urlList))
        s = Serializer(app.config['SECRET_KEY'], expires_in=60*60*24) # temporary URL live for 60 secs by 60 min by 24 hours
        token = s.dumps({'file_path': tarFilePath}).decode("utf-8")
        url_for('serve_file', token=token)
        downloadURL = "https://bacquerya.azurewebsites.net:443/downloads/" + token
        if not (urlDict["email"] == "Enter email" or urlDict["email"].replace(" ", "") == ""):
            send_email(urlDict["email"], downloadURL)
        shutil.rmtree(raw_temp_dir)
        return jsonify({"downloadURL": downloadURL})

@app.route("/downloads/<token>")
@cross_origin()
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
        return render_template('successful_download.html', token=token)

@app.route('/download_link/<path:token>')
@cross_origin()
def download_link(token):
    """Serve compressed genomic sequence file"""
    s = Serializer(app.config['SECRET_KEY'])
    filepath = s.loads(token)['file_path']
    return send_file(os.path.join("..", gene_dir, filepath), as_attachment=True)

@app.route('/alignment/<consistentName>', methods=['GET', 'POST'])
@cross_origin()
def alignementDownload(consistentName):
    """Send MSA for requested gene"""
    return send_file(os.path.join("..", gene_dir, "alignments", consistentName + ".aln.fas"), as_attachment=True)

@app.route('/alignmentView/<consistentName>', methods=['GET', 'POST'])
@cross_origin()
def alignementViewer(consistentName):
    """Send MSA in JSON format for requested gene"""
    consistentName = consistentName + ".aln.fas"
    with open(os.path.join(gene_dir, "alignments", consistentName)) as alnFile:
        alignment = alnFile.read()
    alignment = alignment.split(">")[1:]
    sys.stderr.write("\nConverting MSA to JSON\n")
    alignmentJSON = {}
    for line in tqdm(alignment):
        split = line.splitlines()
        title = split[0]
        sequence = split[1]
        alignmentJSON[title] = sequence.upper()
    return jsonify(alignmentJSON)

@app.route('/upload_template', methods=['GET'])
@cross_origin()
def uploadTemplate():
    """Send upload template file for study"""
    return send_file(os.path.join("..", gene_dir, "upload_template.csv"), as_attachment=True)

@app.route('/upload_accessions', methods=['POST'])
@cross_origin()
def uploadAccessions():
    """Recieve user-uploaded accession IDs for study"""
    upload_dir = os.path.join(gene_dir, "uploaded_accessions")
    if not os.path.exists(upload_dir):
        os.mkdir(upload_dir)
    uploaded_file = request.files['file']
    filename = os.path.join(upload_dir, secure_filename(uploaded_file.filename))
    uploaded_file.save(filename)
    indexAccessions(filename, study_database)

@app.route('/retrieve_accessions/<DOI>', methods=['POST'])
@cross_origin()
def retrieveAccessions(DOI):
    """Retrieve user-uploaded accession IDs for study"""
    accessions = getStudyAccessions(DOI, study_database)
    if not accessions:
        return jsonify({"studyAccessions": accessions})
    else:
        return jsonify({"studyAccessions": []})

@app.route('/population_assembly_stats', methods=['GET'])
@cross_origin()
def populationStats():
    """Send population assembly stats JSON file to frontend"""
    with open(os.path.join(gene_dir, "population_assembly_stats.json")) as statsFile:
        assemblyStats = json.loads(statsFile.read())
    return jsonify(assemblyStats)

if __name__ == "__main__":
    app.run(debug=False,use_reloader=False)