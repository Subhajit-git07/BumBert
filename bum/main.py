import logging
import yaml
import io
import pandas as pd
from yaml.loader import SafeLoader
import uki_tax_main as ukiTax
from azure.storage.blob import BlobServiceClient, __version__




logger = logging.getLogger(__name__)

with open('config.yml') as f:
    configParser = yaml.load(f, Loader=SafeLoader)

CONNECTION_STRING =  configParser['CONNECTION_STRING']

CONTAINER = configParser['CONTAINER']

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)


def main(files_fs,files_tb,docx_filename):
    ukitax = ukiTax.UkiTax(files_fs,files_tb,docx_filename)

    table_list,response_dict = ukitax.doc_processing()

    extracted_table,response_dict = ukitax.tables_extraction(table_list)

    preprocessed_extracted_table_currency = ukitax.currency_preprocessing(extracted_table)

    mergeDataFrame, response_dict = ukitax.trial_balance_file_parsing()

    trialBalanceDataFrame ,pathConcatDataframe= ukitax.trial_balance_preprocessing(mergeDataFrame)

    preprocessed_extracted_table = ukitax.fs_text_cleaning(preprocessed_extracted_table_currency)

    ukitax.fs_tb_concat_turnover(trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table)

    ukitax.fs_tb_bank_interest(trialBalanceDataFrame,pathConcatDataframe,preprocessed_extracted_table)

    ukitax.fs_tb_profit_loss(trialBalanceDataFrame,pathConcatDataframe,preprocessed_extracted_table)

    ukitax.fs_tb_bank_lease(trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table)

    _,response_dict = ukitax.tangible_intangible_assest(trialBalanceDataFrame ,pathConcatDataframe,preprocessed_extracted_table)

    universal_list = ukiTax.UkiTax(files_fs,files_tb,docx_filename).tb_fs_list

    return universal_list,response_dict


'''
Wriritng list of tables and uploading in blob
'''

def writer_format(universal_list,project_id,business_entity_id):

    try:
        universal_list[0]

    except Exception as e:
        logger.info(e)

    try:
        if len(universal_list)>0:

            with io.BytesIO() as output:
                writer = pd.ExcelWriter(output, engine='xlsxwriter')

                for idx,dataframe in enumerate(universal_list):
                    dataframe.to_excel(writer, sheet_name='sheet%s' % idx, index=False)

                writer.save()
                output.seek(0)

                filename =  project_id + '/'+ business_entity_id + 'Reconciliation.xlsx'
                blob_name = "{0}/{1}".format('FSTB-Reconcile',filename)
                blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=blob_name)
                blob_client.upload_blob(output, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", overwrite = True)

    except Exception as e:
        print("FSTB reconcilitation excel upload failed in writer format",e)
        logger.info("FSTB reconcilitation excel upload failed",e)


'''
Checking whether the tables get reconciled or not
'''

def financial_recon_output(files_fs,files_tb,docx_filename):
    decision_list =[]
    response_dict ={}
    universal_list,response_dict = main(files_fs,files_tb,docx_filename)
    try:
        if len(universal_list)>0:
            for idx,_val in enumerate(universal_list):

                if len(universal_list[idx]['recon'])>0:
                    if 'False' in universal_list[idx]['recon'].values.tolist():
                        universal_list[idx] = universal_list[idx].drop('recon',axis =1)
                        decision_list.append({'Decision':"non-reconciled",'Analysis':(universal_list[idx].to_dict())})

                    else:

                        universal_list[idx] = universal_list[idx].drop('recon',axis =1)
                        decision_list.append({'Decision':"reconciled",'Analysis':(universal_list[idx].to_dict())})

                else:

                    universal_list[idx] = universal_list[idx].drop('recon',axis =1)
                    decision_list.append({'Decision':"non-reconciled",'Analysis':(universal_list[idx].to_dict())})

    except Exception as e:
        print('Please check FS or beeive docx having valid tables',e)
        response_dict.update({"Error":"Please check FS in beeive having valid tables or please try converting to pdf"})

    return decision_list, response_dict, universal_list

