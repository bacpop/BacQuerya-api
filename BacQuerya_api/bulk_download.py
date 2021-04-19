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
    return "Done downloading"

def send_email(target_email, downloadLink):
    msg = MIMEMultipart()
    message = "Hello,\nWe are emailing to let you know your sequences have successfully been retrieved! Your sequences are available from the following link:\n" + downloadLink + "\nKind regards,\nThe BacQuerya team"
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
    return
