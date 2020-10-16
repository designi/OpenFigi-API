# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 12:15:28 2020

@author: ngarcia
"""

import requests as rq

header = {'Content-Type': 'application/json'}
example = [{"idType":"VENDOR_INDEX_CODE","idValue":"990100"}]

class OpenFigiException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return "OpenFigi api returned error {}" . format(self.parameter)

def mapIndexVndrToBB(jobs): 
    URL = 'https://api.openfigi.com/v2/mapping/'
    try:
        assert isinstance(jobs,list) and isinstance(jobs[0],dict)
        r = rq.post(url = URL,json = jobs,timeout = 10,headers = header)
        print("Requests returned status code {1} ({0}) from {2} \n".format(r.reason,r.status_code,r.url))
        return r.json()
    except AssertionError:
        print(' Lookup job needs to be a list(dict).\n For Example, {}'.format(example))
    except Exception as e:
        raise OpenFigiException(e)

def main():
    jobs = []
    code = input('Enter Index Vendor Code(s):')
    assert isinstance(code,str)
    if ',' not in code:
        print('Looking up only one code --> {}'.format(code))
        jobs.append({"idType":"VENDOR_INDEX_CODE","idValue":"{}".format(code)})
    elif ',' in code:
        codeList = code.split(',')
        for i in codeList:
            print('Looking up list of codes --> {}'.format(i))
            jobs.append({"idType":"VENDOR_INDEX_CODE","idValue":"{}".format(i)})
    results = mapIndexVndrToBB(jobs)
    if 'data' in results:
        print(results[0]['data'])
    else:
        print(results)

if __name__ == '__main__':
    main()
    
    #Mapping Jobs
# =============================================================================
#     IdMap = [
#     { "idType": "ID_ISIN", "idValue": "US4592001014" },\
#     { "idType": "ID_WERTPAPIER", "idValue": "851399", "exchCode": "US" },\
#     { "idType": "ID_BB_UNIQUE", "idValue": "EQ0010080100001000", "currency": "USD" },\
#     { "idType": "ID_SEDOL", "idValue": "2005973", "micCode": "EDGX", "currency": "USD" },\
#     { "idType":"BASE_TICKER", "idValue":"TSLA 10 C100", "securityType2":"Option", "expiration":["2018-10-01", "2018-12-01"]},\
#     { "idType":"BASE_TICKER", "idValue":"NFLX 9 P330", "marketSecDes":"Equity", "securityType2":"Option", "strike":[330,None], "expiration":["2018-07-01",None]},\
#     { "idType":"BASE_TICKER", "idValue":"FG", "marketSecDes":"Mtge", "securityType2":"Pool", "maturity":["2019-09-01", "2020-06-01"]},\
#     { "idType":"BASE_TICKER", "idValue":"IBM", "marketSecDes":"Corp", "securityType2":"Corp", "maturity":["2026-11-01",None]},\
#     { "idType":"BASE_TICKER", "idValue":"2251Q", "securityType2":"Common Stock", "includeUnlistedEquities": True}] 
# =============================================================================
    #IdMap = [{ "idType":"TICKER", "idValue":"NDV20"},{"marketSecDes":"Comdty"}]

