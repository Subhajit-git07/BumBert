from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_httpauth import HTTPBasicAuth
from yaml.loader import SafeLoader
import yaml
from flask_swagger_ui import get_swaggerui_blueprint
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# from pega_api import call_pega_api
# from scoa_api import scoa_call_pega_api
from gtpw_fstb_api import call_gtpw_api
from gtpw_scoa_api import scoa_call_gtpw_api
import logging
import gtpw_auth_verification as GTPW
import requests

logger = logging.getLogger(__name__)
logger.info("Check")

with open('config.yml') as f:
    configParser = yaml.load(f, Loader=SafeLoader)


CONNECTION_STRING = configParser['CONNECTION_STRING']
CONTAINER = configParser['CONTAINER']
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)


microsoftLogin = configParser['gtpwMicrosoftLogin']
clientId = configParser['clientId']
clientSecret = configParser['clientSecret']
scope = configParser['scope']
gtpwUser = configParser['gtpwUser']
gtpwPassword = configParser['gtpwPassword']
subscriptionId = configParser['subscriptionId']
cookies = configParser['cookies']



gtpw_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Cookie': cookies
            }



gtpw_data = {
            'client_id': clientId,
            'scope': scope,
            'client_secret': clientSecret,
            'grant_type': 'client_credentials',
        }


response = requests.session()


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
#auth = HTTPBasicAuth()

SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'
SWAGGER_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL, API_URL,
    config={'app_name': 'FS-TB'})


app.register_blueprint(SWAGGER_BLUEPRINT, url_prefix=SWAGGER_URL)

users = {
        gtpwUser: gtpwPassword
    }


user2_auth = HTTPBasicAuth()

@user2_auth.verify_password
def verify_password(username, password):
    if username == gtpwUser and password == gtpwPassword:
        return username


@app.route("/GTPW/FSTB", methods=['POST'])
@user2_auth.login_required
def task_fs_tb_gtpw():
    data_json = request.get_json()
    msg_id = data_json.get('messageid')
    project_id = data_json.get('projectid')
    entity_object_id  = data_json.get("entityobjectid")
    business_entity_id = data_json.get("businessentityid")
    auth_url = data_json.get("authurl")
    callback_url = data_json.get("callbackurl")

    access_token =GTPW.GTPWapi().gtpw_microsoft_login(microsoftLogin=microsoftLogin,headers=gtpw_headers,data=gtpw_data)

    gtpw_token,gtpw_csrf = GTPW.GTPWapi().gtpw_auth(authUrl=auth_url,access_token=access_token,subscriptionId=subscriptionId)

    if gtpw_token:
        print('Sync call details received from GTP FSTB',data_json)

        response_fstb = call_gtpw_api(msg_id,project_id,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf)

    else:
        response_fstb = jsonify(
            {
                "messageid": '',

                "status": "error",
                "error": {
                    "code": "",
                    "message": "Authetication failed,Either Microsoft or Auth url failed to generate tokens"
                }

            }
        )

    return response_fstb


@app.route("/GTPW/mapping", methods=['POST'])
@user2_auth.login_required
def task_scoa_gtpw():
    data_json = request.get_json()
    msg_id = data_json.get('messageid')
    project_id = data_json.get('projectid')
    entity_object_id  = data_json.get("entityobjectid")
    business_entity_id = data_json.get("businessentityid")
    auth_url = data_json.get("authurl")
    callback_url = data_json.get("callbackurl")

    access_token =GTPW.GTPWapi().gtpw_microsoft_login(microsoftLogin=microsoftLogin,headers=gtpw_headers,data=gtpw_data)

    gtpw_token,gtpw_csrf = GTPW.GTPWapi().gtpw_auth(authUrl=auth_url,access_token=access_token,subscriptionId=subscriptionId)

    if gtpw_token:
        print('Sync call details received from GTP COA Mapping',data_json)

        response_coa = scoa_call_gtpw_api(msg_id,project_id,entity_object_id,business_entity_id,callback_url,gtpw_token,gtpw_csrf) #,model,tokenizer

    else:
        response_coa = jsonify(
            {
                "messageid": '',

                "status": "error",
                "error": {
                    "code": "",
                    "message": "Authetication failed,Either Microsoft or Auth url failed to generate tokens"
                }

            }
        )

    return response_coa


if __name__ == '__main__':
    app.run()