'''Imports'''
from io import BytesIO
from io import StringIO
import warnings
import numpy as np
import pandas as pd
from docx import Document
import regex as re
import yaml
from azure.storage.blob import BlobServiceClient,  __version__
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import Spacy_parser as spacyparser
from yaml.loader import SafeLoader
from logfile import *
import bbox_ds_api as blackbox
import urllib.request as request
import logging
from mammoth import convert_to_html
import requests

warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)

with open('config.yml') as f:
    configParser = yaml.load(f, Loader=SafeLoader)

CONNECTION_STRING =  configParser['CONNECTION_STRING']

CONTAINER = configParser['CONTAINER_BBOX']

threshold = configParser['thresholdNew']

thresholdRecheck = configParser['thresholdRecheck']

thresholdInTangible = configParser['thresholdInTangible']

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

endpoint = configParser['formRecognizerUrl']
apim_key = configParser['formRecognizerKey']

document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(apim_key)
        )


class UkiTax():

    '''Class Uki Tax  having cleaning, preprocessing, reconciliation and decision function to process files'''

    tb_fs_list =[]

    def __init__(self,files_fs,files_tb,docx_filename):

        financial_statement_keywords = ['Turnover','Depreciation of tangible assets',
           'Bank charges\nInterest on right of use leases',
          'Bank interest payable','Revenue','Turnover (£’000)']

        self.financial_statement_keywords = financial_statement_keywords
        self.files_fs = files_fs
        self.files_tb = files_tb
        self.docx_filename = docx_filename
        self.response_dict ={}
        self.preprocessed_extracted_table = []
        self.trialBalanceDataFrame = self.tableRecheckMerge = self.tbFsReconciliationNorm = pd.DataFrame()


    '''Checking float'''

    def isfloat(self,num):
        try:
            float(num)
            return True

        except ValueError:
            return False


    '''Converting docx file to html and extracting tables'''

    def docx_to_html(self,financial_statement):

        response = requests.get(financial_statement)

        docx_file = BytesIO(response.content)

        result = convert_to_html(docx_file)

        html = result.value

        fs_list = pd.read_html(html)

        for idx,val in enumerate(fs_list):
            fs_list[idx] =fs_list[idx].replace(to_replace=np.nan,value ='',regex = True)

        return fs_list


    '''
    Financial statement processing and extracting tables
    '''

    def doc_processing(self):

        table_list =[]
        logger.info('Financial Statement processing started')

        try:

            fsRequest = request.Request(self.files_fs)
            financialStatement = request.urlopen(fsRequest)

            try:
                if self.files_fs.split('?')[0].endswith(".docx"):

                    document = Document((BytesIO(financialStatement.read())))

                    for table in document.tables:
                        df = [['' for i in range(len(table.columns))] for j in range(len(table.rows))]

                        for i, row in enumerate(table.rows):
                            for j, cell in enumerate(row.cells):
                                if cell.text:
                                    df[i][j] = cell.text

                        table_list.append(pd.DataFrame(df))

                    if len(table_list) != 0:
                        for i, table in enumerate(table_list):
                            if 'Turnover' in table_list[i][0].values:
                                table_list[i][2] = table_list[i][2]
                                loc = list(table_list[i][0].values).index('Turnover')
                                table_list[i][2].iloc[2:] = table_list[i][2].iloc[2:].replace(r'[^0-9]',0,regex=True)
                                li = table_list[i][2].to_list()[loc:]
                                table_value_list = list(set(li))
                                if 0 in list(set(table_value_list)) or '' in list(set(table_value_list)):
                                    table_list = self.docx_to_html(self.files_fs)

            except:
                table_list = self.docx_to_html(self.files_fs)


            if self.files_fs.split('?')[0].endswith(".pdf") or self.files_fs.split('?')[0].endswith('.PDF'):
                content=BytesIO(financialStatement.read())

                poller = document_analysis_client.begin_analyze_document("prebuilt-layout", content)
                result = poller.result()
                result = result.to_dict()

                for pageresult in result["tables"]:
                    tableList=[['' for x in range(pageresult["column_count"])] for y in range(pageresult["row_count"])]

                    for cell in pageresult['cells']:
                        tableList[cell["row_index"]][cell["column_index"]]=cell["content"]

                    df=pd.DataFrame.from_records(tableList)

                    table_list.append(pd.DataFrame(df))

        except Exception as e:
            self.response_dict.update({'Error':"Financial statement having Invalid Content please check the required criteria"})

            print("doc_processing function having Invalid Financial statement", e)

        return  table_list,self.response_dict


    '''
    1. Fetching tables having match with FS keyowrds
    '''

    def tables_extraction(self,doc_tables):

        extracted_table =[]

        try:
            if len(doc_tables)!=0:
                for idx,_val in enumerate(doc_tables):
                    doc_tables[idx][0] =doc_tables[idx][0].replace(to_replace=r'[^a-zA-Z0-9\s]',value ='',regex = True)
                    doc_tables[idx]=doc_tables[idx].replace(to_replace=r"═════|─────|=|-|\n|:unselected:",value ='',regex = True)

                    for j in doc_tables[idx][0]:
                        if j.strip() in self.financial_statement_keywords:
                            extracted_table.append(doc_tables[idx].loc[:,:])

                    if [i.strip() for i in doc_tables[idx][0].to_list() if i.strip() in ['Cost','Depreciation']]==['Cost','Depreciation']:
                        extracted_table.append(doc_tables[idx].loc[:,:])

                    if [i.strip() for i in doc_tables[idx][0].to_list() if i.strip() in ["Cost or valuation"]]==["Cost or valuation"]:
                        extracted_table.append(doc_tables[idx].loc[:,:])

                    if [i.strip() for i in doc_tables[idx][0].to_list() if i.strip() in ['Cost','Amortisation']]==['Cost','Amortisation'] or[i.strip() for i in doc_tables[idx][0].to_list() if i.strip() in ['Cost','Amortisation and impairment']]==['Cost','Amortisation and impairment']:
                        extracted_table.append(doc_tables[idx].loc[:,:])
            else:
               self.response_dict.update({'Error':'Financial Statement missing the Mandatory table to do the reconciliation'})

        except Exception as e:
            print("No data in FInancial Statement", e)
            self.response_dict.update({"Error":"Extracted table not in the proper format so please try using pdf"})

        return extracted_table,self.response_dict


    '''
    Currency normalization of Financial statements tables
    '''

    def currency_preprocessing(self,extracted_table):

        preprocessed_extracted_table = []

        for i, table in enumerate(extracted_table):

            table = table.drop([col for col in table if table[col].sum() == ""], axis=1)

            table.columns = range(table.shape[1])

            for col_val in table.columns:
                table[col_val] = table[col_val].str.strip()

            table[0] = table[0].str.strip()

            extracted_table[i] =table

            if 'Turnover' in extracted_table[i][0].values:
                loc = list(extracted_table[i][0].values).index('Turnover')

                df = extracted_table[i].replace(np.nan,"").reset_index().drop('index',axis =1)

                for val in df.values:
                    for keys,vals in enumerate(val):
                        match =re.search(r'\p{sc}.*',vals)

                        if match:
                            result = match.group()

                            if len(result) >4:
                                df.iloc[loc:][keys] = df.iloc[loc:][keys]+','+str(result[2:])
                                df[keys].iloc[loc:] = df[keys].iloc[loc:].replace(r'%','',regex=True)

                            else:
                                df.iloc[loc:][keys] = df.iloc[loc:][keys]+','+str(result[1:])
                                df[keys].iloc[loc:] = df[keys].iloc[loc:].replace(r'%','',regex=True)

                preprocessed_extracted_table.append(df)

            else:

                loc = list(extracted_table[i][0].values).index([x for x in list(extracted_table[i][0].values) if any(c.isalpha() for c in x)][0])

                for val in extracted_table[i].values:
                    for keys,vals in enumerate(val):
                        match =re.search(r'\p{sc}.*',vals)

                        if match:
                            result = match.group()

                            if len(result) >4:
                                extracted_table[i].iloc[loc:][keys] = extracted_table[i].iloc[loc:][keys]+','+str(result[2:])

                            else:
                                extracted_table[i].iloc[loc:][keys] = extracted_table[i].iloc[loc:][keys]+','+str(result[1:])

                preprocessed_extracted_table.append(extracted_table[i])

        return preprocessed_extracted_table



    '''
    1. Processing Trial Balance and creating columns which are having last two value of Beehive mapping path.
    2. Spliting Beehive mapping path to process TB
    '''

    def trial_balance_file_parsing(self):

        logger.info('Trial Balance persing started')

        mergeDataFrame = data = pd.DataFrame()

        try:

            tbFile = pd.read_csv(self.files_tb, header=None, sep='\n')

            if '?' in str(tbFile.iloc[0]):
                tbFile.iloc[0] =tbFile.iloc[0].replace('[?]','',regex=True)

            try:
                tbFileSplit = tbFile[0].str.split('"', expand=True)

            except:
                print('try block fail')

            else:
                tbFileSplit = tbFile[0].str.split(',', expand=True)

            tbFileItemName = tbFileSplit.loc[:, tbFileSplit.columns.notna()]

            tbFileItemName= tbFileItemName.replace(r'"','',regex=True)

            tbFileItemName.columns = tbFileItemName.iloc[0]

            tbFileItem = tbFileItemName.drop(index=0)

            if ',' in tbFileItemName.columns:
                tbFileItem = tbFileItemName.drop([','],axis =1).drop(0,axis=0)

            tbFileOutput = tbFileItem.loc[:, tbFileItem.columns.notna()]

            tbFileOutput = tbFileOutput.replace('',np.nan)

            tbFileOutput = tbFileOutput.loc[:, tbFileOutput.columns.notna()]

            data = tbFileOutput['Beehive Mapping Path'].str.split('/',expand=True) #expanding path to different columns

            data['path_concat'] = tbFileOutput['Beehive Mapping Path'].str.split('/')

            data = data.fillna(0)

            data = data[data['path_concat']!=0]

            for ix,val in enumerate(data['path_concat']):
                data['path_concat'].iloc[ix] =  " ".join(data['path_concat'].iloc[ix][-2:])

            for ix,val in enumerate(data['path_concat']):
                data['path_concat'].iloc[ix] = " ".join(re.findall('[a-zA-Z][^A-Z]*', val))

            data['path_concat'] = data['path_concat'].str.lower().str.capitalize()

            mergeDataFrame = pd.merge(tbFileOutput, data, left_index=True, right_index=True)

        except Exception as e:
            print("trial_balance_file_parsing function not having Trial Balance", e)
            self.response_dict.update({"Error":"Either TB is missing or not in required format"})

        return mergeDataFrame, self.response_dict



    '''
    1. Creating Root, Base and Leaf dataframe to do groupby and get commulative value of TB
    2. Path concatnate dataframe having commulative value of last two word of splitted beehive path and going to used
        for re-checking reconcile dataframe
    '''


    def trial_balance_preprocessing(self,mergeDataFrame):

        pathConcatDataframe = pd.DataFrame()
        trialBalanceGroupbyList =[]

        logger.info('Trail Balance processing started')

        try:

            if 'Adjusted Trial Balance' in mergeDataFrame.columns:
                mergeDataFrame['Trial Balance'] = mergeDataFrame['Adjusted Trial Balance']
                mergeDataFrame =  mergeDataFrame.drop('Adjusted Trial Balance', axis =1)

            mergeDataFrame['Trial Balance'] = mergeDataFrame['Trial Balance'].fillna(0)

            mergeDataFrame = mergeDataFrame[np.isfinite(pd.to_numeric(mergeDataFrame['Trial Balance'], errors="coerce"))]

            mergeDataFrame['Trial Balance'] = mergeDataFrame['Trial Balance'].astype(float)

            for i,val in enumerate(mergeDataFrame):
                if self.isfloat(mergeDataFrame['Trial Balance'].iloc[i]):
                    if 'Trial Balance' in mergeDataFrame.columns:
                        mergeDataFrame['Trial Balance'].iloc[i] = mergeDataFrame['Trial Balance'].iloc[i]

            trialBalanceBase = mergeDataFrame[( mergeDataFrame[2]!=0)&( mergeDataFrame[3]==0) &( mergeDataFrame[4]==0)].reset_index().drop('index',axis =1).fillna(np.nan)

            trialBalanceRoot =  mergeDataFrame[( mergeDataFrame[3]!=0) &(mergeDataFrame[4]==0)].reset_index().drop('index',axis =1).fillna(np.nan)

            trialBalanceLeaf =  mergeDataFrame[( mergeDataFrame[3]!=0) &( mergeDataFrame[4]!=0)].reset_index().drop('index',axis =1).fillna(np.nan)

            if np.nan not in trialBalanceBase['Trial Balance'].values:
                trialBalanceBaseSum = trialBalanceBase.groupby([2]).agg(trial_balance = ('Trial Balance',sum)).reset_index()
                trialBalanceGroupbyList.append(trialBalanceBaseSum)

            if len(trialBalanceBaseSum)==0:
                if np.nan not in trialBalanceRoot['Trial Balance'].values:
                    trialBalanceRootSum2 = mergeDataFrame.groupby([2]).agg(trial_balance = ('Trial Balance', sum)).reset_index()
                    trialBalanceGroupbyList.append(trialBalanceRootSum2)

            else:
                trialBalanceRootSum2 =pd.DataFrame()

            if np.nan not in trialBalanceRoot['Trial Balance'].values:
                trialBalanceRootSum = trialBalanceRoot.groupby([3]).agg(trial_balance = ('Trial Balance',sum)).reset_index()
                trialBalanceGroupbyList.append(trialBalanceRootSum)

            if np.nan not in trialBalanceLeaf['Trial Balance'].values:
                trialBalanceLeafSum = trialBalanceLeaf.groupby([3,4]).agg(trial_balance = ('Trial Balance',sum)).reset_index()
                trialBalanceGroupbyList.append(trialBalanceLeafSum)

            for key,val in enumerate(trialBalanceGroupbyList):
                trialBalanceGroupbyList[key] = trialBalanceGroupbyList[key].rename(columns ={trialBalanceGroupbyList[key].columns[0]:'Report_item_tb'})

                if key ==2:
                    trialBalanceGroupbyList[2] = trialBalanceGroupbyList[2].groupby('Report_item_tb').sum().reset_index()

                if key ==0:
                    self.trialBalanceDataFrame = trialBalanceGroupbyList[0]

                if key==3:
                    trialBalanceGroupbyList[3] = trialBalanceGroupbyList[3].groupby('Report_item_tb').sum().reset_index()

                if key>0:
                    self.trialBalanceDataFrame = pd.concat([self.trialBalanceDataFrame,trialBalanceGroupbyList[key]],axis =0).reset_index().drop('index',axis =1)


            if 4 in self.trialBalanceDataFrame.columns:
                self.trialBalanceDataFrame = self.trialBalanceDataFrame.drop(4,axis =1)

            self.trialBalanceDataFrame = self.trialBalanceDataFrame[self.trialBalanceDataFrame['Report_item_tb']!=0].reset_index().drop('index',axis =1)

            for idx,val in enumerate(self.trialBalanceDataFrame['Report_item_tb']):
                self.trialBalanceDataFrame['Report_item_tb'].iloc[idx] = " ".join(re.findall('[a-zA-Z][^A-Z]*', val))#spliting text based on captial letter

            self.trialBalanceDataFrame["Report_item_tb"] = self.trialBalanceDataFrame["Report_item_tb"].str.lower().str.capitalize().replace(r'Revenue','Turnover',regex =True)

            self.trialBalanceDataFrame = self.trialBalanceDataFrame.drop_duplicates(subset=['Report_item_tb'])

            mergeDataFrameCopy = mergeDataFrame.rename(columns={'path_concat':'Report_item_tb'})

            pathLeafConcat = mergeDataFrameCopy[(mergeDataFrameCopy['Report_item_tb']!=0)].reset_index().drop('index',axis =1).fillna(-0.999)

            pathConcatDataframe = pathLeafConcat[['Report_item_tb','Trial Balance']]

            pathConcatDataframe = pathConcatDataframe[pathConcatDataframe['Trial Balance']!=-0.999]

            pathConcatDataframe['Trial Balance'] = pd.to_numeric(pathConcatDataframe['Trial Balance'],errors='coerce')

        except Exception as e:
            logger.info('trial_balance_preprocessing', e)

        return self.trialBalanceDataFrame ,pathConcatDataframe



    '''
    1. Cleaning of FS value having both loss and profit in the report item column
    '''

    def fs_text_cleaning(self,preprocessed_extracted_table):

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                preprocessed_extracted_table[ix][0] = preprocessed_extracted_table[ix][0].replace(dict.fromkeys(['Note','Notes'], ''))

                preprocessed_extracted_table[ix] = preprocessed_extracted_table[ix].drop(columns= preprocessed_extracted_table[ix].columns[(preprocessed_extracted_table[ix] == 'Notes').any()]).drop(columns= preprocessed_extracted_table[ix].columns[(preprocessed_extracted_table[ix] == 'Note').any()])

                preprocessed_extracted_table[ix].columns  = range(preprocessed_extracted_table[ix].columns.size)

                preprocessed_extracted_table[ix] =  preprocessed_extracted_table[ix].rename(columns={0:'Report_item'}).replace(r',','',regex=True).replace(r'Revenue','Turnover',regex =True).replace("(",'').replace(')','')

                for keys, char in enumerate(preprocessed_extracted_table[ix]['Report_item']):
                    char = char.lower().split()
                    match = [i for i in char if i in ['loss','profit']] or [i for i in char if i in ['profit','loss']]

                    if match == ['loss','profit'] or match == ['profit','loss']:
                        char.remove('loss')
                        cleanedString = " ".join(char)
                        preprocessed_extracted_table[ix]['Report_item'].iloc[keys] = cleanedString.capitalize()

                    if match == ['loss']:
                        char.insert(char.index(match[0]),'profit')
                        char.remove('loss')
                        cleanedString = " ".join(char)
                        preprocessed_extracted_table[ix]['Report_item'].iloc[keys] = cleanedString.capitalize()

        except Exception as e:
            logger.info("fs_text_cleaning", e)

        return preprocessed_extracted_table



    '''
    1. Difference coloumn having Delta value of FS and TB
    2. Normalizing Delta to nearest thousand
    '''

    def difference_normalization(self,tableName):

        tableName['difference'] =tableName['recon'] = ''

        try:

            if list(tableName['trial_balance'].values)!=0:
                res = [idx for idx, val in enumerate(list(tableName['trial_balance'].values)) if val != 0]


                for i in res:
                    tableName[1].iloc[i] = "".join(tableName[1].iloc[i].split()).replace("(",'-').replace(')','')

                    tableName['trial_balance_pos'] = tableName['trial_balance'].astype(float)

                    if self.isfloat(tableName[1].iloc[i]):
                        tableName['difference'][i]= round((float(tableName[1].iloc[i])) - (tableName['trial_balance_pos'].iloc[i]).astype(float))

                    else:
                        tableName['difference'][i] = 0 - (tableName['trial_balance_pos'].iloc[i]).astype(float)

                    tableName = tableName.drop('trial_balance_pos',axis =1)

                    tableName['recon'][i] = np.where((abs(tableName['difference'][i])<=1000.0),True,False)

                    if tableName['difference'][i]<0:
                        tableName['difference'][i] = (np.ceil(tableName['difference'][i]/1000)*1000)

                    else:
                        tableName['difference'][i] = (np.floor(tableName['difference'][i]/1000)*1000)

        except Exception as e :
            logger.info('difference normalisation', e)

        return tableName



    '''Converting parenthesis to negative as per business rule'''

    def sign_conversation(self,tableName):

        for _i,col in enumerate(tableName.columns):

            if col!='Report_item':
                tableName[col] = tableName[col].apply(lambda x:re.sub(r'[(]','-',str(x))).apply(lambda x:re.sub(r'[)\n\s]','',str(x))).replace(r'{','-',regex=True).replace(r']','',regex=True)

        return tableName



    '''
    1. Using extracted table of FS and finding out consine similarity of sentence based tokenization.
    2. Finding out cosine similarity of the sentence which was not captured in firt phase by using last tow value of beehive
        mapping path.
    3. Process continue in different function till all extracted have reconciled.
    '''

    def fs_tb_concat_turnover(self,trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table):

        logger.info('turnover table fetching started')

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                rankTableMerge =tbFsReconciliation = pd.DataFrame()

                if 'Turnover' in  preprocessed_extracted_table[ix]['Report_item'].values:

                    tbReportItemTurnover = list(preprocessed_extracted_table[ix]['Report_item'].values)

                    fsReportItemTurnover = list(trialBalanceDataFrame['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem =tbReportItemTurnover,fsReportItem =fsReportItemTurnover,threshold=threshold)

                    rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(trialBalanceDataFrame,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    logger.info('inital merge completed')

                    tbFsReconciliation  = preprocessed_extracted_table[ix].merge(rankTableMerge,how ='left')

                    tbFsReconciliation = tbFsReconciliation.fillna(value =0)

                    turnoverRecheck = tbFsReconciliation[tbFsReconciliation['trial_balance']==0]

                    tbReportItemTurnoverRecheck = list(turnoverRecheck['Report_item'].values)

                    fsReportItemTurnoverRecheck = list(pathConcatDataframe['Report_item_tb'].values)

                    rankDataFrame = spacyparser.Spacyparser().spacy_processing(tbReportItem=tbReportItemTurnoverRecheck,fsReportItem=fsReportItemTurnoverRecheck,threshold=thresholdRecheck)

                    logger.info('Recheck merge completed')


                    if 'Report_item_tb' in rankDataFrame:
                        rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(pathConcatDataframe,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tableRecheckMerge = tbFsReconciliation.merge(rankTableMerge,how='left',on= 'Report_item')

                    for ix,val in enumerate(tableRecheckMerge['trial_balance_x'].values):
                        if tableRecheckMerge['trial_balance_x'].iloc[ix]==0:
                            tableRecheckMerge['trial_balance_x'].iloc[ix] = tableRecheckMerge['trial_balance_y'].iloc[ix]

                    tableRecheckMerge['trial_balance'] = tableRecheckMerge['trial_balance_x']

                    tableRecheckMerge= tableRecheckMerge.drop(['trial_balance_x','trial_balance_y'],axis =1)

                    tableRecheckMerge['trial_balance'] = tableRecheckMerge['trial_balance'].fillna(value =0)

                    tbFsReconciliationNorm = self.difference_normalization(tableName= tableRecheckMerge)

                    tbFsReconciliation = self.sign_conversation(tbFsReconciliationNorm)

                    tbFsReconciliation['trial_balance'] = pd.to_numeric(tbFsReconciliation['trial_balance'])

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].replace(0,np.nan)

                    tbFsReconciliation['difference'] = pd.to_numeric(tbFsReconciliation['difference'])

                    self.tb_fs_list.append(tbFsReconciliation)

        except Exception as e:
            logger.info('Merge will not happen please check turnover', e)

        return self.tb_fs_list


    '''Extracting bank interest table'''

    def fs_tb_bank_interest(self,trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table):

        logger.info('bank interest table fetching started')

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                rankTableMerge =tbFsReconciliation = pd.DataFrame()

                if  'Bank interest payable' in preprocessed_extracted_table[ix]['Report_item'].values:  #or financial_statement_keywords[-2] in preprocessed_extracted_table[ix]['Report_item'].values:

                    tbReportItemTurnover = list(preprocessed_extracted_table[ix]['Report_item'].values)

                    fsReportItemTurnover = list(trialBalanceDataFrame['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem =tbReportItemTurnover,fsReportItem =fsReportItemTurnover,threshold=threshold)

                    rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(trialBalanceDataFrame,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tbFsReconciliation  = preprocessed_extracted_table[ix].merge(rankTableMerge,how ='left')

                    tbFsReconciliation = tbFsReconciliation.fillna(value =0)

                    tbFsReconciliation['trial_balance'] = round(tbFsReconciliation['trial_balance'],2)

                    turnoverRecheck = tbFsReconciliation[tbFsReconciliation['trial_balance']==0]

                    tbReportItemTurnoverRecheck = list(turnoverRecheck['Report_item'].values)

                    fsReportItemTurnoverRecheck = list(pathConcatDataframe['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem=tbReportItemTurnoverRecheck,fsReportItem=fsReportItemTurnoverRecheck,threshold=thresholdRecheck)

                    if 'Report_item_tb' in rankDataFrame:
                        rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(pathConcatDataframe,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tableRecheckMerge = tbFsReconciliation.merge(rankTableMerge,how='left',on= 'Report_item')

                    for ix,val in enumerate(tableRecheckMerge['trial_balance_x'].values):
                        if tableRecheckMerge['trial_balance_x'].iloc[ix]==0:
                            tableRecheckMerge['trial_balance_x'].iloc[ix] = tableRecheckMerge['trial_balance_y'].iloc[ix]

                    tableRecheckMerge['trial_balance'] = tableRecheckMerge['trial_balance_x']

                    tbFsReconciliation= tableRecheckMerge.drop(['trial_balance_x','trial_balance_y'],axis =1)

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].fillna(value =0)

                    tbFsReconciliationNorm = self.difference_normalization(tableName= tbFsReconciliation)

                    tbFsReconciliation = self.sign_conversation(tbFsReconciliationNorm)

                    tbFsReconciliation['difference'] = pd.to_numeric(tbFsReconciliation['difference'])

                    tbFsReconciliation['trial_balance'] = pd.to_numeric(tbFsReconciliation['trial_balance'])

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].replace(0,np.nan)

                    self.tb_fs_list.append(tbFsReconciliation)

        except Exception as e:
            logger.info('Merge will not happen please check bank interest',e)

        return self.tb_fs_list


    '''Extracting profit loss table '''

    def fs_tb_profit_loss(self,trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table):

        logger.info('P/L table fetching started')

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                rankTableMerge =tbFsReconciliation = pd.DataFrame()

                if  self.financial_statement_keywords[1] in preprocessed_extracted_table[ix]['Report_item'].values:  #or financial_statement_keywords[-2] in preprocessed_extracted_table[ix]['Report_item'].values:

                    tbReportItemTurnover = list(preprocessed_extracted_table[ix]['Report_item'].values)

                    fsReportItemTurnover = list(trialBalanceDataFrame['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem =tbReportItemTurnover,fsReportItem =fsReportItemTurnover,threshold=threshold)

                    rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(trialBalanceDataFrame,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tbFsReconciliation  = preprocessed_extracted_table[ix].merge(rankTableMerge,how ='left')

                    tbFsReconciliation = tbFsReconciliation.fillna(value =0)

                    tbFsReconciliation['trial_balance'] = round(tbFsReconciliation['trial_balance'],2)

                    turnoverRecheck = tbFsReconciliation[tbFsReconciliation['trial_balance']==0]

                    tbReportItemTurnoverRecheck = list(turnoverRecheck['Report_item'].values)

                    fsReportItemTurnoverRecheck = list(pathConcatDataframe['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem=tbReportItemTurnoverRecheck,fsReportItem=fsReportItemTurnoverRecheck,threshold=thresholdRecheck)

                    if 'Report_item_tb' in rankDataFrame:
                        rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(pathConcatDataframe,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tableRecheckMerge = tbFsReconciliation.merge(rankTableMerge,how='left',on= 'Report_item')

                    for ix,val in enumerate(tableRecheckMerge['trial_balance_x'].values):
                        if tableRecheckMerge['trial_balance_x'].iloc[ix]==0:
                            tableRecheckMerge['trial_balance_x'].iloc[ix] = tableRecheckMerge['trial_balance_y'].iloc[ix]

                    tableRecheckMerge['trial_balance'] = tableRecheckMerge['trial_balance_x']

                    tbFsReconciliation= tableRecheckMerge.drop(['trial_balance_x','trial_balance_y'],axis =1)

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].fillna(value =0).replace(0,np.nan)

                    tbFsReconciliationNorm = self.difference_normalization(tableName= tbFsReconciliation)

                    tbFsReconciliation = self.sign_conversation(tbFsReconciliationNorm)

                    tbFsReconciliation['difference'] = pd.to_numeric(tbFsReconciliation['difference'])

                    tbFsReconciliation['trial_balance'] = pd.to_numeric(tbFsReconciliation['trial_balance'])

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].replace(0,np.nan)

                    self.tb_fs_list.append(tbFsReconciliation)


        except Exception as e:
            logger.info('Merge will not happen please check profilt and loss',e)

        return self.tb_fs_list

    '''Extracting Bank lease table'''

    def fs_tb_bank_lease(self,trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table):

        logger.info('Bank lease table fetching started')

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                rankTableMerge =tbFsReconciliation = pd.DataFrame()

                if  self.financial_statement_keywords[2] in preprocessed_extracted_table[ix]['Report_item'].values:  #or financial_statement_keywords[-2] in preprocessed_extracted_table[ix]['Report_item'].values:

                    tbReportItemTurnover = list(preprocessed_extracted_table[ix]['Report_item'].values)

                    fsReportItemTurnover = list(trialBalanceDataFrame['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem =tbReportItemTurnover,fsReportItem =fsReportItemTurnover,threshold=threshold)

                    rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(trialBalanceDataFrame,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tbFsReconciliation  = preprocessed_extracted_table[ix].merge(rankTableMerge,how ='left')

                    tbFsReconciliation = tbFsReconciliation.fillna(value =0)

                    tbFsReconciliation['trial_balance'] = round(tbFsReconciliation['trial_balance'],2)

                    turnoverRecheck = tbFsReconciliation[tbFsReconciliation['trial_balance']==0]

                    tbReportItemTurnoverRecheck = list(turnoverRecheck['Report_item'].values)

                    fsReportItemTurnoverRecheck = list(pathConcatDataframe['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem=tbReportItemTurnoverRecheck,fsReportItem=fsReportItemTurnoverRecheck,threshold=thresholdRecheck)

                    if 'Report_item_tb' in rankDataFrame:
                        rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(pathConcatDataframe,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tableRecheckMerge = tbFsReconciliation.merge(rankTableMerge,how='left',on= 'Report_item')

                    for ix,val in enumerate(tableRecheckMerge['trial_balance_x'].values):
                        if tableRecheckMerge['trial_balance_x'].iloc[ix]==0:
                            tableRecheckMerge['trial_balance_x'].iloc[ix] = tableRecheckMerge['trial_balance_y'].iloc[ix]

                    tableRecheckMerge['trial_balance'] = tableRecheckMerge['trial_balance_x']

                    tbFsReconciliation= tableRecheckMerge.drop(['trial_balance_x','trial_balance_y'],axis =1)

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].fillna(value =0).replace(0,np.nan)

                    tbFsReconciliationNorm = self.difference_normalization(tableName= tbFsReconciliation)

                    tbFsReconciliation = self.sign_conversation(tbFsReconciliationNorm)

                    tbFsReconciliation['difference'] = pd.to_numeric(tbFsReconciliation['difference'])

                    tbFsReconciliation['trial_balance'] = pd.to_numeric(tbFsReconciliation['trial_balance'])

                    tbFsReconciliation['trial_balance'] = tbFsReconciliation['trial_balance'].replace(0,np.nan)

                    self.tb_fs_list.append(tbFsReconciliation)

        except Exception as e:
            logger.info('Merge will not happen please check bank lease',e)

        return self.tb_fs_list


    '''Extracting tangible and Intangible assest table '''

    def tangible_intangible_assest(self,trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table):

        logger.info('Tangible Intangible table fetching started')

        try:

            for ix,_ in enumerate(preprocessed_extracted_table):

                _rankTableMerge =tbFsReconciliation = pd.DataFrame()

                if [i.strip() for i in  preprocessed_extracted_table[ix]['Report_item'].to_list() if i.strip() in ['Cost','Depreciation',"Cost or valuation"]]:

                    tbReportItemTurnover = list(preprocessed_extracted_table[ix]['Report_item'].values)

                    fsReportItemTurnover = list(trialBalanceDataFrame['Report_item_tb'].values)

                    rankDataFrame =spacyparser.Spacyparser().spacy_processing(tbReportItem =tbReportItemTurnover,fsReportItem =fsReportItemTurnover,threshold=thresholdInTangible)

                    if 'Report_item_tb' in rankDataFrame:
                        rankTableMerge =  rankDataFrame[['Report_item','Report_item_tb','cosine_threshold','Rank']].merge(trialBalanceDataFrame,how ='left').drop(['Report_item_tb','Rank','cosine_threshold'],axis=1).rename(columns={'Trial Balance':'trial_balance'})

                    tbFsReconciliation = preprocessed_extracted_table[ix]#.merge(rankTableMerge,how ='left')

                    tbFsReconciliation = self.difference_normalization(tbFsReconciliation)

                    self.tb_fs_list.append(tbFsReconciliation)

        except Exception as e:
            logger.info('Merge not happened please check either FS or TB',e)
            self.response_dict.update({'Error':"Reconciliation not done please check Financial Statement and TB"})

        return self.tb_fs_list, self.response_dict
