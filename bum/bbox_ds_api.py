
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient,  __version__
import yaml
from yaml.loader import SafeLoader
import logging

logger = logging.getLogger(__name__)


with open('config.yml') as f:
    configParser = yaml.load(f, Loader = SafeLoader)

blackBoxUrlPrefix = configParser['blackBoxUrlStart']
blackBoxUrlsuffix = configParser['blackBoxUrlEnd']




class blackBox():

    def __init__(self):

        self.financial_statement =None
        self.trial_balance = None
        self.coa_mapping = None
        self.path_list = None
        self.files_fs = None
        self.files_tb = None

    def blackbox(self,financial_statement, trial_balance):

        try:

            if financial_statement:
                self.files_fs = financial_statement

            if trial_balance:
                self.files_tb = trial_balance

        except Exception as e:
            logging.info('blackbox',e)

        return self.files_fs,self.files_tb


    '''
    1.Sending project id and business entity id in the the url to fetch details of project entity
    '''

    def blackbox_ds(self,projectId,businessentityid):
        #projectId = '62c7f0405c02cb51e181bbdd'

        file_path = requests.get(blackBoxUrlPrefix + projectId + "/entity/"+ businessentityid + blackBoxUrlsuffix)

        try:
            if file_path.json()['status']=="Success":
                print('Beehive broker success response for project id and business entity id',projectId,businessentityid)

                if len(file_path.json()['response']['docs'])!=0:
                    self.path_list = file_path.json()['response']['docs']

                if len(file_path.json()['response']['coaMapping'])!=0:
                    self.trial_balance = file_path.json()['response']['coaMapping']

                else:
                    self.path_list=None
            else:
                self.path_list =None

        except Exception as e:
            print("Project id sending to blackbox is not correct or Beehive broker not having details".format(projectId,businessentityid))
            logging.info("Project id sending to blackbox is not correct",e)

        return self.path_list, self.trial_balance



    '''
    1. Fetching recent financial statement from the list of files uploaded which is having financial statement tag
    '''

    def bbox_blob_extraction(self,path_list):
        financial_statement_df  = pd.DataFrame(columns =['Name','Time'])

        try:
            for path_dict in path_list:
                if path_dict['tag']== 'FINAL_FINANCIAL_STATEMENTS':
                    if path_dict['docName'].lower().endswith('.docx') or path_dict['docName'].lower().endswith('.pdf'):
                        financial_statement = path_dict['blobPath']
                        docx_filename=path_dict['docName']
                        input_str = path_dict['fileModifiedTime'].replace('T',' ').split('.')[0]
                        time = datetime.strptime(input_str, '%Y-%m-%d %H:%M:%S')
                        financial_statement_df= pd.DataFrame(np.insert(financial_statement_df.values,0 , values=[financial_statement,time], axis=0))
                        financial_statement_df = financial_statement_df.rename(columns={0:'file_path',1:'Date Time'})
                        self.financial_statement  = financial_statement_df.sort_values(by='Date Time',ascending=False).reset_index().drop('index',axis =1).iloc[0]['file_path']

        except Exception as e:
            print('financial statement failing to fetch from black box',e)
            logging.info('financial statement failing to fetch from black box')

        return self.financial_statement, docx_filename



    '''Fetching raw tb'''

    def bbox_coa_mapping(self,path_list):

        coa_mapping_df = pd.DataFrame(columns =['Name','Time'])
        try:
            for path_dict in path_list:
                if path_dict['tag'] =="TRIAL_BALANCE":
                    if path_dict['docName'].lower().endswith('.xlsx') or path_dict['docName'].lower().endswith('.csv'):
                        coa_mapping = path_dict['blobPath']
                        input_str = path_dict['fileModifiedTime'].replace('T',' ').split('.')[0]
                        time = datetime.strptime(input_str, '%Y-%m-%d %H:%M:%S')
                        coa_mapping_df= pd.DataFrame(np.insert(coa_mapping_df.values,0 , values=[coa_mapping,time], axis=0))
                        coa_mapping_df = coa_mapping_df.rename(columns={0:'file_path',1:'Date Time'})
                        self.coa_mapping  = coa_mapping_df.sort_values(by='Date Time',ascending=False).reset_index().drop('index',axis =1).iloc[0]['file_path']

        except Exception as e:
            print('coa mapping file failing to fetch from black box',e)
            logging.info('coa mapping file failing to fetch from black box')
        return self.coa_mapping




