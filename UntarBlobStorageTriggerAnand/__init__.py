import logging
import datetime
import tarfile
import os
import urllib.request
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(tarblob: func.InputStream):           
    logging.info(f"Start untaring to Azure Blob Storage: {datetime.datetime.utcnow()}")
    logging.info(f"Blob Name: {tarblob.name} " f"Blob Size: {tarblob.length} bytes")    
    try:
        #connectionString: Define Connection to connect to azure account
        connectionString = os.environ["untarblobfunction_STORAGE"]        
        #containerName: Define container name From Config or create new? TO DO
        containerName = "temp"
        #Create Blob Service Client from connectionString
        blob_service_client = BlobServiceClient.from_connection_string(connectionString)        
        # Get Tar file, only read .tar file from settings {name}.tar             
        fileStreamData = urllib.request.urlopen(tarblob.uri)
        lockboxTarFile = tarfile.open(fileobj=fileStreamData,mode="r|*")        
        for fileName in lockboxTarFile.getnames():                                         
            #create new blob and upload   
            blob = blob_service_client.get_blob_client(container=containerName,blob=fileName)
            #Upload blob to azure                   
            blob.upload_blob(fileName, overwrite=True)
        logging.info(f"End untaring to Azure Blob Storage: {datetime.datetime.utcnow()}")
    except  Exception as e:
        logging.exception(f"Something went wrong when storing the file: { e }")