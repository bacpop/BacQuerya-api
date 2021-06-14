# Bacquerya_api

Backend API for the BacQuerya website.

# Routes

```/geneQuery```
* Uses a ```POST``` method to search for a gene query in the elastic gene index and returns a JSON containing a list of search results.

```/isolateQuery```
* Uses a ```POST``` method to search for an isolate query in the elastic isolate index. If ```searchType = isolate ``` the route returns a JSON containing a list of search results. If ```searchType = species ``` returns all * S. pneumoniae * isolate results, If ```searchType = biosampleList ```, searches for each biosample in the index and returns the metadata for the highest ranked result for each BioSample.

```/sequence```
* 

```/study```
* 

```/bulkdownloads```
* 

```/downloads/```
* 

```/download_link/```
* 


```/alignment/```
* 

```/alignmentView/```
* 

```/upload_template```
* 

```/upload_accessions```
* 

```/population_assembly_stats```
* 
