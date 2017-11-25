# BASED ON: https://stackoverflow.com/questions/4720735/fastest-way-to-download-3-million-objects-from-a-s3-bucket

from eventlet import *
patcher.monkey_patch(all=True)

import os, sys, time, csv
import boto3

import logging

logging.basicConfig(filename="s3_download.log", level=logging.INFO)
index_file = "/mnt/c/Users/Gautam/Downloads/index_2017.csv"
BUCKET = 'irs-form-990'
download_prefix = "/mnt/c/Users/Gautam/form_990/"
filename_postfix = "_public.xml"
pool_size = 100

def download_file(key_name, client):
    # Its imp to download the key from a new connection
    # conn = S3Connection("KEY", "SECRET")

    try:
        with open(download_prefix + key_name + filename_postfix, 'wb') as data:
            client.download_fileobj(BUCKET, key_name + filename_postfix, data)
    except:
        logging.error(key_name+":"+"FAILED")

if __name__ == "__main__":
    client = boto3.client('s3')

    key_list = []
    with open(index_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            key_list.append(row['OBJECT_ID'])

    logging.info("Creating a pool")
    pool = GreenPool(size=pool_size)

    logging.info("Saving files from bucket...")
    for key in key_list:
        pool.spawn_n(download_file, key, client)
    pool.waitall()
