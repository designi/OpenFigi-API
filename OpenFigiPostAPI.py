# -*- coding: utf-8 -*-
"""
@author: ngarcia
OpenFigi API: Get Bloomberg Figi 
"""

import requests as rq

openfigi_url = 'https://api.openfigi.com/v2/mapping'
header = {'Content-Type': 'application/json'}

class OpenFigiPostAPI:

    def __init__(self,IdentifierMap):
        self.IdentifierMap = IdentifierMap

    def __str__(self):
        return '{}'.format(self.IdentifierMap)

    def getdata(Ids):
        #Mapping job must be a list of dicts as specified by OpenFigi.
        if isinstance(Ids,list) == True and isinstance(Ids[0],dict) == True:
            response = rq.post(url = openfigi_url,json = Ids,timeout = 10,headers = header)
        else:
            return 'Data does not conform to OpenFigi specification. The data being passed in must be a Python List of Dictionaries.'





# ==================================Example Data===============================
# IdMap = \
# [
# { "idType": "ID_ISIN", "idValue": "US4592001014" }, \
# { "idType": "ID_WERTPAPIER", "idValue": "851399", "exchCode": "US" }, \
# { "idType": "ID_BB_UNIQUE", "idValue": "EQ0010080100001000", "currency": "USD" }, \
# { "idType": "ID_SEDOL", "idValue": "2005973", "micCode": "EDGX", "currency": "USD" }, \
# { "idType":"BASE_TICKER", "idValue":"TSLA 10 C100", "securityType2":"Option", "expiration":["2018-10-01", "2018-12-01"]}, \
# { "idType":"BASE_TICKER", "idValue":"FG", "marketSecDes":"Mtge", "securityType2":"Pool", "maturity":["2019-09-01", "2020-06-01"]}, \
# { "idType":"BASE_TICKER", "idValue":"2251Q", "securityType2":"Common Stock", "includeUnlistedEquities": 'true'} \
# ]
# =============================================================================
