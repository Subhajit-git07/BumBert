from azure.storage.blob import BlobServiceClient, __version__
from datetime import datetime, timedelta
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
import yaml
from yaml.loader import SafeLoader

with open('config.yml') as f:
    configParser = yaml.load(f, Loader = SafeLoader)

CONNECTION_STRING =  configParser['CONNECTION_STRING']

Container = configParser['CONTAINER']
storageAccountName = configParser['storageAccountNameDemo']
storageAccountKey = configParser['storageAccountKeyDemo']

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)


class blobUrl():

    def __init__(self) -> None:
         pass

    def blob_sas_token(blob_name):
            try:
                sas = generate_blob_sas(account_name=storageAccountName,
                                    account_key=storageAccountKey,
                                    container_name=Container,
                                    blob_name=blob_name,
                                    permission=BlobSasPermissions(read=True),
                                    expiry=datetime.utcnow() + timedelta(days=31)
                                    )

                sas_url ='https://'+storageAccountName+'.blob.core.windows.net/'+Container+'/'+blob_name+'?'+sas
            except Exception as e:
                print('sas token not generating',e)
            return sas_url



