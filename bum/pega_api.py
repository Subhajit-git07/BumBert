import requests
from multiprocessing import Process
from flask import jsonify
from requests.adapters import HTTPAdapter, Retry
from main import *
import base64
import json
import logging
import bbox_ds_api as blackbox
import blob_url_fetch as blob


#logger = logging.getLogger(__name__)
logger.info("In Pega API.py")


'''
    1. Response pega process data extraction, reconcile to fs and tb  and sending back response to pega as payload.
    2. Retry mechanism to try if api get failed
    3. Generating blob url to send in payload
'''

def response_pega(id_msg, pega_url, pega_user, pega_password,project_id,financial_statement,trial_balance,entity_object_id,business_entity_id,docx_filename):

    print("Response to pega initiated and details are Message id {},Project id{}, Business Entity id{}".format(id_msg,project_id,business_entity_id))

    logger.info("Process Called " + id_msg)

    payload = ''
    response_dict = {}

    if financial_statement is not None and trial_balance is not None:
        reconcile_decision, response_dict,universal_list = financial_recon_output(financial_statement,trial_balance,docx_filename)

        writer_format(universal_list,project_id,business_entity_id)

    elif financial_statement is None and trial_balance is not None:
        response_dict.update(
            {'Error':'Either Financial Statement is missing or not in required format (.docx or .pdf)'})

    elif financial_statement is not None and trial_balance is None:
        response_dict.update(
            {'Error':'Either Trial Balance is missing or not in required format (.csv)'})

    else:
        response_dict.update(
            {'Error':'Either Financial Statement and Trial Balance are missing or not in required format'})


    headers = {'Accept': 'application/json/msexcel', 'Content-Type': 'application/json'}

    session = requests.Session()

    retries = Retry(total=3, backoff_factor=1, status_forcelist=[401, 400, 444, 500, 555], method_whitelist=False)

    session.mount('http://', HTTPAdapter(max_retries=retries))

    session.mount('https://', HTTPAdapter(max_retries=retries))

    if len(response_dict) != 0:

        if 'Error' in response_dict.keys():
            response_dict.update({"Error": response_dict['Error']})

            payload = json.dumps(
                                dict(messageid=id_msg,
                                    projectid=project_id,
                                    entityobjectid=entity_object_id,
                                    status="error",
                                    fileName="",
                                    filepath="",
                                    errormessage=response_dict['Error']))
            try:
                print(payload)
                logger.info("Pega API hit try")

                data = session.post(url=pega_url, data=payload,
                                    headers=headers, auth=(pega_user, pega_password),verify=False)
                print(data.status_code)

                print("fstb Process completed with error for project id {}, business entity id {}".format(project_id,business_entity_id))

            except Exception as e:
                logger.info("Pega API hit Error", e)
                print("Error in Hitting Pega API",e)

    else:
        decision_list = []

        filename =  project_id + '/'+ business_entity_id + 'Reconciliation.xlsx'

        blob_name = "{0}/{1}".format('FSTB-Reconcile',filename)

        blob_url = blob.blobUrl.blob_sas_token(blob_name=blob_name)


        for val in reconcile_decision:
            for key, values in val.items():
                if key == 'Decision':
                    decision_list.append(values)

        decision_list = list(set(decision_list))

        if len(decision_list) == 1:

            payload = json.dumps(
                                dict(messageid=id_msg,
                                    projectid=project_id,
                                    entityobjectid=entity_object_id,
                                    status=decision_list[0],
                                    fileName="Reconciliation.xlsx",
                                    filepath=blob_url,
                                    errormessage=""))

            try:
                print(payload)
                logger.info("Pega API hit try")

                data = session.post(url=pega_url, data=payload,
                                    headers=headers, auth=(pega_user, pega_password),verify=False)
                print(data.status_code)

                print("fstb Process completed successfully for project id {}, business entity id {}".format(project_id,business_entity_id))

            except Exception as e:
                logger.info("Pega API hit Error", e)
                print("Error in Hitting Pega API",e)


        elif len(decision_list)>1:

            payload = json.dumps(
                                dict(messageid=id_msg,
                                    projectid=project_id,
                                    entityobjectid=entity_object_id,
                                    status="non-reconciled",
                                    fileName="Reconciliation.xlsx",
                                    filepath=blob_url,
                                    errormessage=""))

            try:
                print(payload)
                logger.info("Pega API hit Error")

                data = session.post(url=pega_url, data=payload,
                                    headers=headers, auth=(pega_user, pega_password),verify=False)
                print(data.status_code)

                print("fstb Process completed successfully for project id {}, business entity id {}".format(project_id,business_entity_id))

            except Exception as e:
                logger.info("Pega API hit Error", e)
                print("Error in Hitting Pega API",e)

        else:
            payload = json.dumps(
                                dict(messageid=id_msg,
                                    projectid=project_id,
                                    entityobjectid=entity_object_id,
                                    status="error",
                                    fileName="",
                                    filepath="",
                                    errormessage="Reconciliation not happened please check FS and TB having valid data or try converting to pdf"))

            try:
                print(payload)
                logger.info("Pega API hit Error")

                data = session.post(url=pega_url, data=payload,
                                    headers=headers, auth=(pega_user, pega_password),verify=False)
                print(data.status_code)

                print("Process completed with error for project id {}, business entity id {}".format(project_id,business_entity_id))

            except Exception as e:
                logger.info("Pega API hit Error", e)
                print("Error in Hitting Pega API",e)

    logger.info("Process Ended " + id_msg)

    return payload


class ProcessPega:

    def __init__(self, msg_id, pega_url, pega_user, pega_password,project_id,entity_object_id,business_entity_id):
        self.msg_id = msg_id
        self.pega_url = pega_url
        self.pega_user = pega_user
        self.pega_password = pega_password
        self.project_id = project_id
        self.entity_object_id = entity_object_id
        self.business_entity_id = business_entity_id
        self.financial_statement =  None
        self.trial_balance =  None
        self.docx_filename = None

    ''' 1. Calling black box class to check for presence of files
        2. Using Multiprocess to call response pega to complete extraction process
        3. In parallel sending response to pega if we have fs and tb
    '''

    def get_fs_tb(self):

        try:

            logger.info("In get FS/TB function")

            if self.project_id and self.business_entity_id:
                path_list,self.trial_balance = blackbox.blackBox().blackbox_ds(projectId=self.project_id,businessentityid=self.business_entity_id)

                if path_list is not None:
                    self.financial_statement,self.docx_filename = blackbox.blackBox().bbox_blob_extraction(path_list=path_list)

            fs = self.financial_statement
            tb =  self.trial_balance

            bidding_cb = Process(target=response_pega,
                                 args=(self.msg_id, self.pega_url, self.pega_user, self.pega_password, self.project_id,self.financial_statement,
                                       self.trial_balance, self.entity_object_id,self.business_entity_id,self.docx_filename))
            bidding_cb.start()

            if path_list is None:
                print("path list is none for project id {}, business entity id {}".format(self.project_id, self.business_entity_id))

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="error",
                                    error={
                                            "code": "400",
                                            "message": "File not received of FSTB please retry"
                }
                     )
            ),400


            if fs == None:
                print("fs is not present for project id {}, business entity id {}".format(self.project_id, self.business_entity_id))

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="error",
                                    error={
                                    "code": 404,
                                    "message": "Check Financial Statement"
                    }
                         )
                )

            if tb == None:
                print("tb is not present for project id {}, business entity id {}".format(self.project_id, self.business_entity_id))

                return jsonify(
                                dict(messageid=self.msg_id,
                                    status="error",
                                    error={
                                    "code": 404,
                                    "message": "Check Trial Balance"
                    }
                         )
                )


            else:
                print("fstb success payload executed of project id {}, business entity id {}".format(self.project_id,self.business_entity_id))

                logger.info("FS/TB function executed")

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

            logger.info("FS/TB function error - ", e)

            return jsonify(
                            dict(messageid=self.msg_id,
                                status="error",
                                error={
                                        "code": "400",
                                        "message": "Something went wrong please try to initiate the process again"
                }
                     )
            ),400


def call_pega_api(msg_id, pega_url, pega_user, pega_password,project_id,entity_object_id,business_entity_id):
    obj_call = ProcessPega(msg_id, pega_url, pega_user, pega_password,project_id,entity_object_id,business_entity_id)
    res = obj_call.get_fs_tb()
    return res


