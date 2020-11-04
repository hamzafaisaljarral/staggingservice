 
#!/bin/python
# -*-coding=utf-8-*-

import requests
import time
import hmac
import hashlib
import json
#import urllib
#from urllib.parse import urlencode  
import urllib
baseUrl = "https://openapi.digifinex.com/v3"

class digifinex():
    def __init__(self, data):
        self.appKey = data["appKey"]
        self.appSecret = data["appSecret"]

    def _generate_accesssign(self, data):
        query_string = urllib.urlencode(data)
        m = hmac.new(self.appSecret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        s = m.hexdigest()
        print("data-origin:", data)
        print("query_string:", query_string)
        print("sign:", s)
        return s

    def do_request(self, method, path, data, needSign=False):
        if needSign:
            headers = {
                "ACCESS-KEY": self.appKey,
                "ACCESS-TIMESTAMP": str(int(time.time())),
                "ACCESS-SIGN": self._generate_accesssign(data),
            }
        else:
            headers = {}
        if method == "POST":
            response = requests.request(method, baseUrl+path, data=data, headers=headers)
        else:
            response = requests.request(method, baseUrl+path, params=data, headers=headers)
        #print('response.text:',response.text)
        #print('----------------------------------')
        return response.text



