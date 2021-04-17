from joblib import Parallel, delayed
import os
import subprocess
import sys
from tqdm import tqdm
import urllib.request

def download_sequence(url):
    url_label = os.path.basename(url)
    urllib.request.urlretrieve(url, url_label)

def getDownloadLink(urlList, output_dir, n_cpu):
    os.chdir(output_dir)
    sys.stderr.write("\nDownloading sequence files\n")
    # parallelise sequence download
    job_list = [
        urlList[i:i + n_cpu] for i in range(0, len(urlList), n_cpu)
        ]
    for job in tqdm(job_list):
        Parallel(n_jobs=n_cpu)(delayed(download_sequence)(url) for url in job)

    sys.stderr.write("\Tarring sequence files\n")
    os.chdir("..")
    subprocess.call(['tar', '-czf', "compressed_genomic_sequences.tar.gz", output_dir])