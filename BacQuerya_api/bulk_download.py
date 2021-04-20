from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from joblib import Parallel, delayed
import os
import smtplib
import subprocess
import sys
from tqdm import tqdm
import urllib.request

SOURCE_ADDRESS = os.getenv('SOURCE_ADDRESS')
SOURCE_PASSWORD = os.getenv('SOURCE_PASSWORD')

def download_sequence(url):
    """Download genomic sequence of interest using URL"""
    url_label = os.path.basename(url)
    urllib.request.urlretrieve(url, url_label)

def getDownloadLink(urlList, output_dir, temp_dir, raw_temp_dir, n_cpu):
    """Parallelises sequence downloading, tars the downloaded sequences and returns the relative path of the compressed directory"""
    current_dir = os.getcwd()
    os.chdir(raw_temp_dir)
    sys.stderr.write("\nDownloading sequence files\n")
    # parallelise sequence download
    job_list = [
        urlList[i:i + n_cpu] for i in range(0, len(urlList), n_cpu)
        ]
    for job in tqdm(job_list):
        Parallel(n_jobs=n_cpu)(delayed(download_sequence)(url) for url in job)

    sys.stderr.write("\nTarring sequence files\n")
    os.chdir("..")
    filePath = os.path.join(os.path.basename(temp_dir), "compressed_genomic_sequences.tar.gz")
    sys.stderr.write(os.path.abspath(filePath))
    subprocess.call(['tar', '-czf', filePath, os.path.basename(raw_temp_dir)])
    os.chdir(current_dir)
    return filePath

def send_email(target_email, downloadLink):
    """Automatically generates and send email containing the download link for sequences requested if the user email is specified"""
    msg = MIMEMultipart()
    message = "Hello,\n\nWe are emailing to let you know your sequences have successfully been retrieved and are available from the following link:\n\n" + downloadLink + "\n\n This link will remain live for 24 hours, after which time you will need to re-request your sequences.\n\nKind regards,\n\nThe BacQuerya team"
    msg['Subject'] = "Your recent BacQuerya sequence request"
    msg['From'] = SOURCE_ADDRESS
    msg['To'] = target_email
    msg.attach(MIMEText(message, 'plain'))
    #Setup SMTP server to send emails
    s = smtplib.SMTP(host='smtp-mail.outlook.com', port=587) #outlook
    s.starttls()
    s.login(SOURCE_ADDRESS, SOURCE_PASSWORD)
    s.send_message(msg)
    s.quit()
