
import json
import io
import requests
from multiprocessing import Process
from flask import jsonify
from requests.adapters import HTTPAdapter, Retry
from main import *
import logging
import bbox_ds_api as blackbox
from standard_coa_model import *
import blob_url_fetch as blob
import torch
import xlsxwriter
from transformers import BertTokenizer
from transformers import BertForSequenceClassification


logger = logging.getLogger(__name__)

logger.info("In GTP API.py")

with open('coa_dict.json') as json_file:
    scoa_dict = json.load(json_file)


label_dict = scoa_dict['label_dict'][0]



'''
    1. Response GTP process data extraction, reconcile to fs and tb  and sending back response to GTP as payload.
    2. Retry mechanism to try if api get failed
    3. Uploading COA Mapping files to blob
    4. Generating blob url to send in payload
'''


def response_gtpw_scoa(id_msg,project_id,coa_mapping,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf):

    print("Response to GTP initiated and details are Message id {},Project id{}, Business Entity id{}".format(id_msg,project_id,business_entity_id))

    logger.info("Process Called " + id_msg)

    payload = ''
    scoa_final_dataframe = pd.DataFrame()

    model = BertForSequenceClassification.from_pretrained("bert-base-uncased",num_labels=len(label_dict), output_attentions=False,output_hidden_states=False)

    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)

    model.load_state_dict(torch.load(r'./model/New-BERT-Model_10.model', map_location=torch.device('cpu')))

    scoa_final_dataframe,response_dict,scoaDataFrame,sorted_path_df = main_scoa(coa_mapping,model,tokenizer)

    filename = project_id + '/'+ business_entity_id + 'COAMapping.xlsx'

    headers = {
                'Content-Type': 'application/json',
                'Authorization': gtpw_token,
                'GTPW-CSRF': gtpw_csrf
                }

    session = requests.Session()

    retries = Retry(total=3, backoff_factor=1, status_forcelist=[401, 400, 444, 500, 555], method_whitelist=False)

    session.mount('http://', HTTPAdapter(max_retries=retries))

    session.mount('https://', HTTPAdapter(max_retries=retries))

    if len(response_dict)==0:
        try:

            tb=pd.DataFrame()
            data = scoaDataFrame['Beehive Mapping Path'].str.split('/', expand=True)
            data = data.drop(0,axis =1)

            mapped_expand_path=tb.append(scoa_final_dataframe['Beehive Mapping Path'].str.split('/',expand=True))
            mapped_expand_path= mapped_expand_path.drop([0],axis=1)

            for i in range(mapped_expand_path.columns[-1],len(data.columns)+1):
                if len(mapped_expand_path.columns)<len(data.columns):
                    mapped_expand_path[mapped_expand_path.columns[-1]+1]=np.nan

            columns =[]

            for i in mapped_expand_path.columns:
                x = 'Level'+' ' +str(i)
                columns.append(x)

            mapped_expand_path.columns = columns

            mapped_expand_path  = pd.concat([scoa_final_dataframe,mapped_expand_path], axis=1)#.drop('Level 0',axis =1)

            result = mapped_expand_path
            result=result.replace(np.nan,'')

            pop_path=result.pop("Beehive Mapping Path")
            result.insert(result.columns.get_loc("Level 7")+1, 'Beehive Mapping Path', pop_path)
            pop_code=result.pop("Beehive Mapping Code")
            result.insert(result.columns.get_loc("Beehive Mapping Path")+1, 'Beehive Mapping Code', pop_code)
            pop_name=result.pop("Beehive Report Item Name")
            result.insert(result.columns.get_loc("Beehive Mapping Code")+1, 'Beehive Report Item Name', pop_name)
            pop_group=result.pop("Report Item Grouping")
            result.insert(result.columns.get_loc("Beehive Report Item Name")+1, 'Report Item Grouping', pop_group)

            col_ind_level1=result.columns.get_loc("Level 1")
            col_letter_level1=xlsxwriter.utility.xl_col_to_name(col_ind_level1)
            col_ind_level2=result.columns.get_loc("Level 2")
            col_letter_level2=xlsxwriter.utility.xl_col_to_name(col_ind_level2)
            col_ind_level3=result.columns.get_loc("Level 3")
            col_letter_level3=xlsxwriter.utility.xl_col_to_name(col_ind_level3)
            col_ind_level4=result.columns.get_loc("Level 4")
            col_letter_level4=xlsxwriter.utility.xl_col_to_name(col_ind_level4)
            col_ind_level5=result.columns.get_loc("Level 5")
            col_letter_level5=xlsxwriter.utility.xl_col_to_name(col_ind_level5)
            col_ind_level6=result.columns.get_loc("Level 6")
            col_letter_level6=xlsxwriter.utility.xl_col_to_name(col_ind_level6)
            col_ind_level7=result.columns.get_loc("Level 7")
            col_letter_level7=xlsxwriter.utility.xl_col_to_name(col_ind_level7)

            col_ind_beehive=result.columns.get_loc("Beehive Mapping Path")
            col_letter_beehive=xlsxwriter.utility.xl_col_to_name(col_ind_beehive)
            next_col_letter_beehive=xlsxwriter.utility.xl_col_to_name(col_ind_beehive+1)

            with io.BytesIO() as output:

                writer = pd.ExcelWriter(output, engine='xlsxwriter')
                result.to_excel(writer, index=False, sheet_name='Sheet1')
                sorted_path_df.to_excel(writer, index=False,sheet_name='Sheet2')

                n_rows = result.shape[0]
                workbook  = writer.book
                worksheet = writer.sheets['Sheet1']
                worksheet2 = writer.sheets['Sheet2']

                header_format = workbook.add_format(
                    {
                    'bold': True,
                    'font_name': 'Calibri',
                    'font_size': 11,
                    'center_across': True,
                    'border': 1}
                    )

                scoaDataFrame['Beehive Mapping Path'].to_excel(writer,sheet_name='Sheet2', engine='xlsxwriter',index=False,startrow=0,startcol=17)

                worksheet.write(f'{next_col_letter_beehive}1','Mapping Path Exist/Not exist',header_format)

                unlocked = workbook.add_format({'locked': False})

                worksheet.set_column(f'{col_letter_level1}:{col_letter_level7}', None, unlocked)
                worksheet.data_validation(f'{col_letter_level1}2:{col_letter_level1}'+str(1+n_rows), {'validate' : 'list', 'source': ['incomeStatement','balanceSheet']})

                for i in range(n_rows):
                    #Level 2
                    worksheet.data_validation(f'{col_letter_level2}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$2:$B$5, 0, MATCH(${col_letter_level1}$'+str(2+i)+', Sheet2!$A$1:$B$1, 0))'})
                    #level 3
                    worksheet.data_validation(f'{col_letter_level3}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$8:$E$10, 0, MATCH(${col_letter_level2}$'+str(2+i)+', Sheet2!$A$7:$E$7, 0))'})
                    #Level 4
                    worksheet.data_validation(f'{col_letter_level4}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$14:$I$111, 0, MATCH(${col_letter_level3}$'+str(2+i)+', Sheet2!$A$13:$I$13, 0))'})
                    #Level 5
                    worksheet.data_validation(f'{col_letter_level5}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$115:$N$155, 0, MATCH(${col_letter_level4}$'+str(2+i)+', Sheet2!$A$114:$N$114, 0))'})
                    #Level 6
                    worksheet.data_validation(f'{col_letter_level6}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$158:$H$171, 0, MATCH(${col_letter_level5}$'+str(2+i)+', Sheet2!$A$157:$H$157, 0))'})
                    #Level 7
                    worksheet.data_validation(f'{col_letter_level7}'+str(2+i), {'validate' : 'list', 'source': f'=INDEX(Sheet2!$A$175:$M$179, 0, MATCH(${col_letter_level6}$'+str(2+i)+', Sheet2!$A$174:$M$174, 0))'})

                    for i in range(1, n_rows + 1):
                        worksheet.write_formula(i, col_ind_beehive, f'=IF({col_letter_level1}{i+1}="", "", IF({col_letter_level2}{i + 1}="", CONCATENATE("/",{col_letter_level1}{i+1}), IF({col_letter_level3}{1+i}="", CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1}), IF({col_letter_level4}{1+i}="", CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1},"/",{col_letter_level3}{1+i}), IF({col_letter_level5}{1+i}="", CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1},"/",{col_letter_level3}{1+i},"/",{col_letter_level4}{1+i}), IF({col_letter_level6}{1+i}="", CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1},"/",{col_letter_level3}{1+i},"/",{col_letter_level4}{1+i},"/",{col_letter_level5}{1+i}), IF({col_letter_level7}{1+i}="", CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1},"/",{col_letter_level3}{1+i},"/",{col_letter_level4}{1+i},"/",{col_letter_level5}{1+i},"/",{col_letter_level6}{1+i}), CONCATENATE("/",{col_letter_level1}{i+1},"/",{col_letter_level2}{i + 1},"/",{col_letter_level3}{1+i},"/",{col_letter_level4}{1+i},"/",{col_letter_level5}{1+i},"/",{col_letter_level6}{1+i},"/",{col_letter_level7}{1+i}))))))))')
                        worksheet.write_formula(i, col_ind_beehive+1, f'=IFERROR(IF(VLOOKUP({col_letter_beehive}{i+1},Sheet2!$R$2:$R$401,1,FALSE)<>"","Mapping Exists","Incorrect Mapping Path"),"Incorrect Mapping Path")')

                        # Add a format. Light red fill with dark red text.
                        format1 = workbook.add_format(
                            {
                                'bg_color': '#FFC7CE',
                                'font_color': '#9C0006'
                                }
                            )
                        # Add a format. Green fill with dark green text.
                        format2 = workbook.add_format(
                            {
                                'bg_color': '#C6EFCE',
                                'font_color': '#006100'
                                }
                            )

                        worksheet.conditional_format(f'{next_col_letter_beehive}{i+1}',
                                                     {
                                                        'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'Mapping Exists',
                                                        'format': format2
                                                        }
                                                     )

                        worksheet.conditional_format(f'{next_col_letter_beehive}{i+1}',
                                                     {
                                                        'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'Incorrect Mapping Path',
                                                        'format': format1
                                                }
                                                     )

                workbook.close()
                writer.save()
                output.seek(0)

                blob_name = "{0}/{1}".format('COA-Mapping',filename)

                blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=blob_name)

                blob_client.upload_blob(output, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", overwrite = True)

        except Exception as e:
            print('Drop down not succeeded',e)
            response_dict.update({'Error':'Drop down on COA Mapping not succeeded,Please check COA Mapping file follow all required creteria and retry'})


    if len(response_dict) != 0:

        if 'Error' in response_dict.keys():
            response_dict.update({"Error": response_dict['Error']})

            payload = json.dumps(
                                dict(messageid=id_msg,
                                    projectid=project_id,
                                    entityobjectid=entity_object_id,
                                    status="error", fileName="",
                                    filepath="",
                                    errormessage=response_dict['Error']))

            try:

                print(payload)
                logger.info("GTP API hit try")

                data = session.post(url=callback_url,headers = headers,data =payload)
                print(data.status_code)

                print("coa mapping Process completed with error for project id {}, business entity id {}".format(project_id,business_entity_id))

            except Exception as e:
                logger.info("GTP API hit Error", e)
                print("Error in Hitting GTP API")

    else:

        blob_name = "{0}/{1}".format('COA-Mapping', filename)

        blob_url = blob.blobUrl.blob_sas_token(blob_name=blob_name)

        payload = json.dumps(
                            dict(messageid=id_msg,
                                projectid=project_id,
                                entityobjectid=entity_object_id,
                                status="success",
                                fileName="COAMapping.xlsx",
                                filepath=blob_url,
                                errormessage=""))

        try:

            print(payload)
            logger.info("GTP API hit Error")

            data = session.post(url=callback_url, headers = headers,data =payload)
            print(data.status_code)

            print("coa mapping completed successfully for project id {}, business entity id {}".format(project_id,business_entity_id))

        except Exception as e:
            logger.info("GTP API hit Error", e)
            print("Error in Hitting GTP API",e)

    logger.info("Process Ended " + id_msg)
    return payload


class ProcessGTPw:

    def __init__(self, msg_id,project_id,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf):

        self.msg_id = msg_id
        self.project_id = project_id
        self.entity_object_id = entity_object_id
        self.business_entity_id = business_entity_id
        self.coa_mapping = None
        self.callback_url =callback_url
        self.gtpw_token = gtpw_token
        self.gtpw_csrf = gtpw_csrf

    ''' 1. Calling black box class to check for presence of files
        2. Using Multiprocess to call response GTP to complete extraction process
        3. In parallel sending response to GTP if we have coa
    '''

    def get_coa(self):
        try:
            logger.info("In get coa function")

            if self.project_id and self.business_entity_id:
                path_list,_ = blackbox.blackBox().blackbox_ds(projectId=self.project_id,businessentityid=self.business_entity_id)

                if path_list is not None:
                    self.coa_mapping = blackbox.blackBox().bbox_coa_mapping(path_list=path_list)

            coa = self.coa_mapping

            bidding_cb = Process(target=response_gtpw_scoa,
                                 args=(self.msg_id,self.project_id,self.coa_mapping, self.entity_object_id,self.business_entity_id,
                                       self.callback_url,self.gtpw_token, self.gtpw_csrf))
            bidding_cb.start()

            if path_list is None:
                print("path list is none for project id {}, business entity id {}".format(self.project_id,self.business_entity_id))

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="error",
                                    error={
                                            "code": "400",
                                            "message": "File not received of COA Mapping please retry"
                }
                    )

            ),400


            if coa==None:
                print("coa is none for project id {}, business entity id {}".format(self.project_id,self.business_entity_id))

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="error",
                                    error={
                                            "code": 404,
                                            "message": "COA either not in required format plrease try uploading again in Beehive"
                    }
                         )
                )


            else:
                print("scoa executed of project id {}, business entity id {}".format(self.project_id,self.business_entity_id))

                logger.info("scoa executed")

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="success",
                                    error={
                                            "code": "",
                                            "message": ""
                    }
                         )
                )


        except Exception as e:
            print("Something went wrong please try to initiate process again for project id {}, business entity id {}".format(self.project_id, self.business_entity_id))

            return jsonify(
                            dict(messageid=self.msg_id,
                                status="error",
                                error={
                                        "code": "400",
                                        "message": "Something went wrong please try to initiate process again"
                }
                     )
            ),400


def scoa_call_gtpw_api(msg_id,project_id,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf):
    obj_call = ProcessGTPw(msg_id,project_id,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf)
    res = obj_call.get_coa()
    return res


