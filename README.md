# Bacquerya_api

Backend API for the BacQuerya website.

# Routes

```/geneQuery```
* Uses a ```POST``` method to search for a gene query in the elastic gene index and returns a JSON containing a list of search results. If ```searchType = gene``` the first 100 results for the query are returned. If ```searchType = consistentNameList```, a list of genes are iteratively queried in the index and the first exact match for each gene is returned. The ```pageNumber``` parameter tells elastic which chunk of the search results should be returned: ```pageNumber == 1``` returns the first 100 results, ```pageNumber == 2``` returns the second 100 results etc.

```/isolateQuery```
* Uses a ```POST``` method to search for an isolate query in the elastic isolate index. If ```searchType = isolate``` the route returns a JSON containing a list of search results. If ```searchType = species``` returns all *S. pneumoniae* isolate results, If ```searchType = biosampleList```, searches for each biosample in the index and returns the metadata for the highest ranked result for each BioSample. The ```pageNumber``` parameter tells elastic which chunk of the search results should be returned: ```pageNumber == 1``` returns the first 100 results, ```pageNumber == 2``` returns the second 100 results etc.

```/sequence```
* Searches for a query sequence in a static COBS index available through the BacQuerya storage instance. Returns a list of key value pairs where the key is the gene name and the value is the proportion of matching k-mers between the query sequence and the gene sequence in the result.

```/study```
* Searches for a query term in the PubMed database using the Biopython entrez API.

```/bulkdownloads```
* Accepts a list of sequence URLs for isolates. If there are 100 or fewer sequences, the sequences are downloaded and tarred. If there are more than 100 sequences, a list of sequence download links for all relevant isolates is returned instead. If an email is specified, an email is sent containing a download link for the created file or directory.

```/downloads/<token>```
* Uses an encoded token to check if the sequence download tarfile or txt file has successfully been created and serves a HTML page indicating success or failure.

```/download_link/<path:token>```
* Sends the requested sequence file specified by the token for download.

```/alignment/<consistentName>```
* Sends a static multiple sequence alignment for the gene of interest.

```/alignmentView/<consistentName>```
* REST route to return a JSON of the relevant multiple sequence alignment file by gene name.

```/upload_template```
* Sends the upload template for submitting the accession IDs of isolates used in studies.

```/upload_accessions```
* Recieves and saves a CSV file containing the accession IDs of isolates used in a particular study. The accession IDs are then extracted and indexed with the study DOI using a SQLite database.

```/retrieve_accessions/<DOI>```
* Queries a SQLite database to return a list of isolate accession IDs associated with a particular study DOI.

```/population_assembly_stats```
* Returns a JSON of population-wide isolate assembly statistics.

# Contributors

The [BacQuerya-api](https://github.com/bacpop/BacQuerya-api) was developed by Daniel Anderson and John Lees.
