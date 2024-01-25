import pandas as pd
import requests
import yaml
from yaml.loader import SafeLoader


class GTPWapi():

    def __init__(self):
        self.access_token = None
        self.gtpw_token=None
        self.csrf_token = None


    def gtpw_microsoft_login(self,microsoftLogin,headers,data):

        try:
            token = requests.post(microsoftLogin,headers=headers, data=data)
            self.access_token = token.json()['access_token']

        except Exception as e:
            print("Microsoft login is failing",e)

        return self.access_token


    def gtpw_auth(self,authUrl,access_token,subscriptionId):

        try:

            auth_data = {
                    'msftToken': access_token,
                    'subscriptionId': subscriptionId,
                            }

            auth_response = requests.post(authUrl,auth_data)
            self.gtpw_token = auth_response.json()['gtpw_token']
            self.gtpw_csrf = auth_response.json()['gtpw_csrf']

        except Exception as e:
            print("Microsoft failed to generate token",e)

        return self.gtpw_token, self.gtpw_csrf





