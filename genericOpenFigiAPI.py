""" @author: ngarcia """

import requests as rq
#import json as js

#header = {'Content-Type': 'application/json'}
header = {'Content-Type': 'application/json', 'X-OPENFIGI-APIKEY': 'e401fd30-1591-4052-b3bb-0a86eb447549'}

baseURL = 'https://api.openfigi.com/v2/'

jobs = ['mapping','search','mapping/values/','filter']
print('OpenFigi API jobs: ',[i for i in jobs])

while True:
    ui = input('Enter the OpenFigi job to run:')
    if ui in jobs:
        URL = baseURL+ui
        break
    elif ui == 'exit':
        break
    else:
        print('Invalid job.')
        continue

#Get the current list of values for the enum-like properties on Mapping Jobs
if ui =='mapping/values/':
    enums = ['idType','exchCode','micCode','currency','marketSecDes','securityType','securityType2','stateCode']
    enumMap = {v:URL+v for v in enums}

    print('Enum options:',[i for i in enums])
    userEnum = input('Enter the enum:')
    lookupStr = input('Enter a string to search for in results:')
    r = rq.get(url = enumMap[userEnum])
    print("Requests returned status code {1} ({0}) from {2} \n".format(r.reason,r.status_code,r.url))
    if lookupStr != 'skip':
        print([i.lower() for i in r.json()['values'] if lookupStr in i.lower()])
    else:
        print([i for i in r.json()['values']])
elif ui == 'mapping':
    #Mapping Jobs
    #IdMap = [{ "idType":"TICKER", "idValue":"ESH0","securityType":"Physical index future."},\
    IdMap = [{"idType":"UNIQUE_ID_FUT_OPT","idValue":"XAZ0 Comdty"}]
    #IdMap = [{"idType":"VENDOR_INDEX_CODE","idValue":"990100"}]
    #IdMap = [{"idType":"ID_EXCH_SYMBOL","idValue":"ATW","securityType2":"Comdty","exchCode":"ICE"}]

    # =============================================================================
    r = rq.post(url = URL,json = IdMap,timeout = 10,headers = header)
    print("Requests returned status code {1} ({0}) from {2} \n".format(r.reason,r.status_code,r.url))
    print(r.json()[0])
    #findStr = str(r.json()[0])
    #print("ATW" in findStr)
elif ui == 'search':
    IdMap = {"query": "ESH0 Index",'securityType': 'Physical index future.'}
    r = rq.post(url = URL,json = IdMap,timeout = 10,headers = header)
    print("{0}. Status_code={1} \n".format(r.reason,r.status_code))
    print(r.json())