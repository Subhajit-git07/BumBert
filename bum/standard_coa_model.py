import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, __version__
import yaml
from yaml.loader import SafeLoader
import json
from io import BytesIO
import regex as re
import spacy
import logging
import torch
import warnings



warnings.filterwarnings("ignore")

nlp = spacy.load('en_core_web_md',exclude=["tagger", "parser", "attribute_ruler", "lemmatizer", "ner","tok2vec","senter"])

nlp.disable_pipes(*[pipe for pipe in nlp.pipe_names if pipe != "similarity"])


logger = logging.getLogger(__name__)

logger.info("In Pega API.py")

with open('config.yml') as f:
    configParser = yaml.load(f, Loader=SafeLoader)


CONNECTION_STRING =  configParser['CONNECTION_STRING']
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER = configParser['CONTAINER']
scoaThreshold = configParser['scoaThreshold']


device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

with open('coa_dict.json') as json_file:
    scoa_dict = json.load(json_file)


rename_dict = scoa_dict['rename_dict'][0]

label_dict = scoa_dict['label_dict'][0]


response_dict ={}



class standardCOA():


    def __init__(self,trialBalance,model,tokenizer):
        self.files_scoa = None
        self.files_tb = None
        self.trialBalance = trialBalance
        self.response_dict = {}
        self.model = model
        self.tokenizer = tokenizer



    def file_reading(self):
        reconcile_file =''
        try:

            filename = "Training data-latest.xlsx"

            blob_name = "{0}/{1}".format('COA-Mapping', filename)

            blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=blob_name)

            reconcile_file = BytesIO(blob_client.download_blob().content_as_text(encoding=None))

        except Exception as e:
            self.response_dict.update({'Error':'Azure failing to download EY COA file'})


        return reconcile_file, self.response_dict



    def sheet_name(self):
        sheetName =''
        sheetItems = {}

        if self.trialBalance.endswith('.xlsx'):
            df = pd.ExcelFile(self.trialBalance)
            sheet = df.sheet_names

            for i in range(len(sheet)):
                sheetItems.update({i:sheet[i]})

            sheetName = sheetItems[0]


        return sheetName



    def classification_model(self,trialBalanceDF,columnName):
        try:

            trialBalanceDF['Beehive Report Item Grouping'] =''
            trialBalanceDF["new_Report Item Name"] = trialBalanceDF['Report Name'].astype(str) +"-"+ trialBalanceDF[columnName]

            test_text=trialBalanceDF['new_Report Item Name']

            encoded_data_test = self.tokenizer.batch_encode_plus(
                test_text.values.tolist(),
                add_special_tokens=True,
                return_attention_mask=True,
                pad_to_max_length=True,
                max_length=100,
                return_tensors='pt',
                truncation=True
            )

            input_ids_test = encoded_data_test['input_ids']

            attention_masks_test = encoded_data_test['attention_mask']

            with torch.no_grad():
                preds = self.model(input_ids_test.to(device), attention_masks_test.to(device))

            logits=preds[0]

            prediction_list=[]

            prediction_list.append(logits)

            prediction = np.concatenate(prediction_list, axis=0)

            final_prediction = np.argmax(prediction, axis = 1).flatten()

            trialBalanceDF['Prediction_label']=final_prediction

            inv_map = {v: k for k, v in label_dict.items()}

            trialBalanceDF['Beehive Report Item Grouping']=trialBalanceDF['Prediction_label'].replace(inv_map)

            trialBalanceDF.drop(['new_Report Item Name','Prediction_label'], axis=1, inplace=True)

        except Exception as e:
            print("Classification model is failing to classify",e)
            self.response_dict.update({'Error':'Model failing to classify Account Description'})


        return trialBalanceDF, self.response_dict



    def trialBalance_mapping(self):

        trialBalanceDF =trialBalanceDF_incomeStatement= trialBalanceDF_balanceSheet = columnName = groupingColumn =''

        try:
            if self.trialBalance is not None:
                sheetName = self.sheet_name()

                if self.trialBalance.endswith('.xlsx'):
                    trialBalanceDF = pd.read_excel(self.trialBalance, sheet_name= sheetName)
                else:
                    trialBalanceDF = pd.read_csv(self.trialBalance,sep =',')

                if set(['Report Name','Report Tree Hierarchy']).issubset(trialBalanceDF.columns):
                    trialBalanceDF=trialBalanceDF.drop('Report Tree Hierarchy',axis=1)

                presentColList = ['Beehive Mapping Code','Beehive Mapping Path','Beehive Report Item Name','Beehive Report Item Grouping']

                for col in trialBalanceDF.columns:
                    for col_list in presentColList:
                        if col==col_list:
                            trialBalanceDF.rename(columns={col_list:col_list+'(PY)'},inplace=True)

                for column in trialBalanceDF.columns:
                    column = column.title()
                    if 'Account Description' in column:
                        columnName =column

                trialBalanceDF[columnName] = trialBalanceDF[columnName].replace(to_replace=r'[^a-zA-Z0-9\s]',value =' ',regex = True).replace(r'[0-9]',value ='',regex=True)

                trialBalanceDF[columnName] =trialBalanceDF[columnName].replace({pd.NA: 1})

                trialBalanceDF[columnName] = trialBalanceDF[columnName].replace(1,'na')

                trialBalanceDF[columnName] = trialBalanceDF[columnName].str.strip()


                if 'Report Item Grouping' not in trialBalanceDF.columns:
                    trialBalanceDF,_ = self.classification_model(trialBalanceDF,columnName)

                trialBalanceDF = trialBalanceDF.replace('Revenue','Turnover')

                if 'Report Item Grouping' in trialBalanceDF.columns:
                    trialBalanceDF['Beehive Report Item Grouping'] = trialBalanceDF['Report Item Grouping']

                col_list = trialBalanceDF.columns

                for col in col_list:
                    col = col.title()

                    if col=='Beehive Report Item Grouping':
                        groupingColumn = col

                        trialBalanceDF[groupingColumn] = trialBalanceDF[groupingColumn].str.title()

                        trialBalanceDF[groupingColumn] = trialBalanceDF[groupingColumn].replace(rename_dict)

                        trialBalanceDF[groupingColumn] = trialBalanceDF[groupingColumn].str.capitalize()

                        trialBalanceDF[groupingColumn] = trialBalanceDF[groupingColumn].replace(to_replace=r'[^a-zA-Z0-9\s]',value ='',regex = True).str.strip()

                if 'Report Name' in trialBalanceDF.columns:
                    trialBalanceDF['Report Name'] = trialBalanceDF['Report Name'].str.title()

                    trialBalanceDF_incomeStatement =  trialBalanceDF[trialBalanceDF['Report Name']=="Income Statement"]

                    trialBalanceDF_balanceSheet = trialBalanceDF[trialBalanceDF['Report Name']=="Balance Sheet"]

        except Exception as e:
            print('trialBalance_mapping',e)
            self.response_dict.update({'Error':'Required columns are missing in Chart of Account'})


        return trialBalanceDF_incomeStatement,trialBalanceDF_balanceSheet, groupingColumn, columnName,self.response_dict,trialBalanceDF



    def cleaning_scoa(self,scoaDataFrame):

        try:
            for col in scoaDataFrame.columns:
                if col.startswith('Unnamed'):
                    scoaDataFrame[col] =scoaDataFrame[col].replace(r"[^a-zA-Z0-9\s]"," ",regex=True)
                    scoaDataFrame[col] = scoaDataFrame[col].replace(np.nan,'na')

                    for ix,char in enumerate(scoaDataFrame[col]):
                        char = char.lower().split()
                        match = [i for i in char if i in ['profit','loss']] or [i for i in char if i in ['gains','losses']]

                        if match == ['profit','loss']:
                            char.remove('loss')
                            cleanedString = " ".join(char)
                            scoaDataFrame[col].iloc[ix] = cleanedString.capitalize()

                        if match == ['gains','losses']:
                            char.remove('losses')
                            cleanedString = " ".join(char)
                            scoaDataFrame[col].iloc[ix] = cleanedString.capitalize()

                    scoaDataFrame[col] = scoaDataFrame[col].replace('na',np.nan)

        except Exception as e:
            self.response_dict.update({"Error":"Required columns are missing in Standard Chart of Account"})


        return scoaDataFrame ,self.response_dict



    def scoa_mapping(self,standardScoa):
        sorted_path_df = scoa_IncomeStatement =scoa_balanceSheet= ''
        scoaDataFrame = pd.DataFrame()
        try:
            scoaDataFrame_original = pd.read_excel(standardScoa, sheet_name='SCOA',skiprows=0)

            sorted_path_df = pd.read_excel(standardScoa, sheet_name='sorted_path',skiprows=0)

            scoaDataFrame_original = scoaDataFrame_original.rename(columns={'Report Name':'Report Tree Hierarchy'})

            scoaDataFrame=scoaDataFrame_original[['Report Tree Hierarchy','Report Item Name','Beehive Mapping Path','Beehive Mapping Code','SCOA Groupings']]

            df=pd.DataFrame()

            scoaDataFrame['SCOA Groupings'] = scoaDataFrame['SCOA Groupings'].str.capitalize()

            scoaDataFrame['SCOA Groupings'] = scoaDataFrame['SCOA Groupings'].replace(to_replace=r'[^a-zA-Z0-9\s]',value ='',regex = True).str.strip()

            scoaDataFrame['Concat Mapping Path'] = scoaDataFrame['Mapping Path before leaf'] = ''

            #scoa_IncomeStatement =scoa_balanceSheet= ''

            try:

                exapand_path=df.append(scoaDataFrame['Beehive Mapping Path'].str.split('/',expand=True))

                exapand_path=exapand_path.replace(np.nan,'nan')

                for i in range(len(exapand_path.columns)):
                    for ix,val in enumerate(exapand_path[i]):
                        exapand_path[i].iloc[ix] = " ".join(re.findall('[a-zA-Z][^A-Z]*', val)).lower().capitalize()

                exapand_path.drop([0], axis=1,inplace=True)

                for i in range(len(exapand_path.columns)+1):
                    exapand_path=exapand_path.rename(columns={i:f'Unnamed: {i}'})

                scoaDataFrame['Expand Mapping Path'] = scoaDataFrame['Beehive Mapping Path'].str.split('/')

                scoaDataFrame['Expand Mapping Path'] = scoaDataFrame['Expand Mapping Path'].replace(np.nan,'nan')

                for ix,val in enumerate(scoaDataFrame['Expand Mapping Path']):
                    scoaDataFrame['Concat Mapping Path'].iloc[ix] =  " ".join(scoaDataFrame['Expand Mapping Path'].iloc[ix][-1:])

                for ix,val in enumerate(scoaDataFrame['Expand Mapping Path']):
                    scoaDataFrame['Mapping Path before leaf'].iloc[ix] =  " ".join(scoaDataFrame['Expand Mapping Path'].iloc[ix][-2:-1])

                for ix,val in enumerate(scoaDataFrame['Concat Mapping Path']):
                    scoaDataFrame['Concat Mapping Path'].iloc[ix] =  " ".join(re.findall('[a-zA-Z][^A-Z]*', val)).lower().capitalize()

                for ix,val in enumerate(scoaDataFrame['Mapping Path before leaf']):
                    scoaDataFrame['Mapping Path before leaf'].iloc[ix] =  " ".join(re.findall('[a-zA-Z][^A-Z]*', val)).lower().capitalize()


                scoaDataFrame, _ = self.cleaning_scoa(scoaDataFrame)

                scoaDataFrame = scoaDataFrame.replace('Revenue','Turnover')

                scoaDataFrame['Report Tree Hierarchy'] = scoaDataFrame['Report Tree Hierarchy'].str.title()

                resultDF = pd.concat([scoaDataFrame, exapand_path], axis=1)

                mergeDataFrame = resultDF.drop(['Unnamed: 1'], axis=1)

                mergeDataFrame = mergeDataFrame.replace('Revenue','Turnover')

                scoa_IncomeStatement = mergeDataFrame[mergeDataFrame['Report Tree Hierarchy']=='Income Statement']

                scoa_balanceSheet =  mergeDataFrame[mergeDataFrame['Report Tree Hierarchy']=='Balance Sheet']

            except Exception as e:
                print('scoa data fetching error',e)
                self.response_dict.update({"Error":"Required columns are missing in Standard Chart of Account"})

        except Exception as e:
            print('scoa data fetching error',e)
            self.response_dict.update({"Error":"Required columns are missing in Standard Chart of Account"})


        return scoa_IncomeStatement, scoa_balanceSheet, self.response_dict, scoaDataFrame,sorted_path_df



    def grouping_df(self,scoa,tb,groupingColumn):
        mergeDF = []

        for col in scoa.columns:
            if col.startswith('SCOA Groupings'):
                mergeDF.append(scoa.merge(tb,left_on=col, right_on = groupingColumn, how='inner'))

        return mergeDF



    def tb_scoa_merge(self,trialBalanceDF_incomeStatement,trialBalanceDF_balanceSheet,groupingColumn,columnName,scoa_IncomeStatement,scoa_balanceSheet):
        finalDataFrame =pd.DataFrame()

        try:
            merge_df_IS = self.grouping_df(scoa_IncomeStatement, trialBalanceDF_incomeStatement, groupingColumn)

            finalISMerge = pd.concat(merge_df_IS, axis =0)

            merge_df_BS = self.grouping_df(scoa_balanceSheet, trialBalanceDF_balanceSheet, groupingColumn)

            finalBSMerge = pd.concat(merge_df_BS, axis =0)

            finalDataFrame = pd.concat([finalISMerge,finalBSMerge],axis =0, ignore_index=True)

            finalDataFrame = finalDataFrame.dropna(subset=[columnName])

            finalDataFrame = finalDataFrame.replace('Nan',np.nan)

        except Exception as e:
            self.response_dict.update({'Error':"Required columns are missing in Trial Balance"})


        return finalDataFrame ,columnName, self.response_dict



    def sentence_similarity(self,finalDataFrame):

        finalDataFrame['cosine_threshold'] =''
        finalDataFrame['Rank'] =''
        finalDataFrame['cosine_threshold_v'] =''

        print('Similarity started')

        try:
            mapping_before_leaf = ''

            for key,_ in enumerate(finalDataFrame['Concat Mapping Path']):
                mapping_sentence = finalDataFrame['Concat Mapping Path'].iloc[key]

                nature_summary = finalDataFrame['Account Description'].iloc[key]

                doc1 = nlp(mapping_sentence)

                doc2 = nlp(nature_summary)

                cosine_threshold = (np.dot(doc1.vector, doc2.vector) / (np.linalg.norm(doc1.vector) * np.linalg.norm(doc2.vector)))

                finalDataFrame['cosine_threshold'].iloc[key] = cosine_threshold

            for key,_ in enumerate(finalDataFrame['cosine_threshold']):

                if finalDataFrame['cosine_threshold'].iloc[key]<scoaThreshold:
                    mapping_sentence = finalDataFrame['Mapping Path before leaf'].iloc[key]
                    mapping_before_leaf = finalDataFrame['Account Description'].iloc[key]

                    doc1 = nlp(mapping_sentence)

                    doc2 = nlp(mapping_before_leaf)

                    cosine_threshold = (np.dot(doc1.vector, doc2.vector) / (np.linalg.norm(doc1.vector) * np.linalg.norm(doc2.vector)))

                    finalDataFrame['cosine_threshold_v'].iloc[key] = cosine_threshold

        except Exception as e:
            self.response_dict.update({'Error':"No Match found please check account description"})


        return finalDataFrame, self.response_dict



    def scoa_tb_mapping(self,finalDataFrame,columnName):

        try:

            finalDataFrame['cosine_threshold_v'] = finalDataFrame['cosine_threshold_v'].replace('',np.nan,regex=True)

            finalDataFrame["agg_threshold"] = finalDataFrame['cosine_threshold_v'].fillna(finalDataFrame["cosine_threshold"])

            finalDataFrame['Rank'] = finalDataFrame.groupby(columnName)['agg_threshold'].rank(ascending=False)

            finalDataFrame = finalDataFrame.sort_values('Rank')

            finalDataFrame = finalDataFrame.drop_duplicates(keep='first',subset=['Account Code'])

            finalDataFrame  = finalDataFrame.drop(['cosine_threshold', 'Rank', 'cosine_threshold_v','agg_threshold'],axis =1)

            finalDataFrame = finalDataFrame.rename(columns = {'Concat Mapping Path':'Beehive Report Item Name'})

        except Exception as e:
            self.response_dict.update({'Error':"Please check 'Account Code'"})


        return finalDataFrame, self.response_dict



def highlight_rows(row):

    value = row.loc['Beehive Mapping Path']

    if value=='':
        color='#FFFF00'

    else:
        color = '#FFFFFF'

    return ['background-color: {}'.format(color) for r in row]



def main_scoa(trialBalance,model,tokenizer):

    rankDataFrame = pd.DataFrame()
    sorted_path_df = scoaDataFrame = pd.DataFrame()
    response_dict ={}

    out = standardCOA(trialBalance,model,tokenizer)

    standardScoa,response_dict = out.file_reading()

    trialBalanceDF_incomeStatement,trialBalanceDF_balanceSheet, groupingColumn, columnName,response_dict,trialBalanceDF =  out.trialBalance_mapping()

    scoa_IncomeStatement, scoa_balanceSheet,response_dict,scoaDataFrame,sorted_path_df = out.scoa_mapping(standardScoa)

    finalDataFrame ,columnName, response_dict = out.tb_scoa_merge(trialBalanceDF_incomeStatement,trialBalanceDF_balanceSheet,groupingColumn,columnName,scoa_IncomeStatement,scoa_balanceSheet)

    finalDataFrame_sentence,response_dict = out.sentence_similarity(finalDataFrame)

    print('parallel processing completed now moving to scoa_tb_mapping')

    finalDataFrame,response_dict = out.scoa_tb_mapping(finalDataFrame_sentence,columnName)

    try:
        scoa_IncomeStatement = scoa_IncomeStatement.rename(columns = {'Concat Mapping Path':'Beehive Report Item Name'})

        colList = list(scoa_IncomeStatement.columns)

        finalDataFrame = finalDataFrame.reset_index().drop('index',axis =1)

        finalDataFrame = trialBalanceDF.merge(finalDataFrame,how='left')

        if 'Report Item Grouping' not in finalDataFrame.columns:
            finalDataFrame['Report Item Grouping'] = np.nan

        keep_col = ['Beehive Mapping Path','Beehive Mapping Code','Beehive Report Item Name']

        colList = [i for i in colList if i not in keep_col]

        rankDataFrame= finalDataFrame.drop(colList,axis=1)

        rankDataFrame['Beehive Mapping Path'] = rankDataFrame['Beehive Mapping Path'].replace(np.nan, '')

    except Exception as e:
        print('coa mapping main function failing',e)
        response_dict.update({'Error':'Please check trial balance or its mandatory instructions to process the coa mapping'})


    return rankDataFrame, response_dict, scoaDataFrame,sorted_path_df

