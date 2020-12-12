# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 13:38:12 2020

@author: ngarcia
"""

import requests 
import sys
import optparse
import pymssql
import configparser
import pandas as pd
import logging
import time

from marketdb import Connections
from marketdb import Utilities

'''
See https://www.openfigi.com/api for more information.
'''

openfigi_url = 'https://api.openfigi.com/v2/mapping'
openfigi_apikey = None
openfigi_headers = {'Content-Type': 'application/json'}

if openfigi_apikey: 
        openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey

class OpenFigiException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return 'OpenFigi api returned error -> {0}'.format(self.parameter)

class FindFutureMapping:

        def __init__(self,db,config,connections):
                self.cursor=db.cursor()
                self.config = config
                self.connections = connections
                self.marketDB = connections.marketDB
                self.mssql = connections.qaDirect
                self.marketData = connections.marketData
                self.FutureMonthToCode=({'JAN':'F','FEB':'G','MAR':'H','APR':'J','MAY':'K','JUN':'M','JUL':'N','AUG':'Q','SEP':'U','OCT':'V','NOV':'X','DEC':'Z'})
                self.FutureCodeToMonth=dict((v,k) for k,v in self.FutureMonthToCode.items())
                self.bloombergSecurityTypes = {'Financial index future.': 'Index','Generic currency future.':'Curncy','Generic index future.':'Index','Physical commodity future.': 'Comdty','Financial commodity future.': 'Comdty','SINGLE STOCK DIVIDEND FUTURE': 'Equity','DIVIDEND NEUTRAL STOCK FUTURE': 'Equity', 'Currency future.': 'Curncy', 'Physical index future.': 'Index'}
                self.marketSecDes = list(set(self.bloombergSecurityTypes.values()))
                self.bbFutTickerSuffix = ('Comdty','Curncy','Index','Equity')
                self.ejv_bb_asset_category = {'Curncy': 'CFU', 'Index': 'EIF','Comdty':('BFU','IRF','EFU')}
                self.ejv_asset_category_descr = ({'BFS':'Bond Future Spread','BFU':'Bond Future','CFU':'Currency Future','CMF':'Commodity Future','EIF':'Equity/Index Future','IRF':'Interest Rate Future'})
        
        def readBloombergExchMicMap(self):
                query = """ SELECT BLOOMBERG_EXCHANGE_MNEMONIC,BLOOMBERG_EXCHANGE_NAME,SEGMENT_MIC,
                                OPERATING_MIC,ISO_COUNTRY_CODE,ISO_EXCH_NAME,FROM_DT,THRU_DT 
                                FROM BLOOMBERG_EXCH_MIC_MAP"""
                self.marketDB.dbCursor.execute(query)
                r = self.marketDB.dbCursor.fetchall()
                headers = ['BLOOMBERG_EXCHANGE_MNEMONIC','BLOOMBERG_EXCHANGE_NAME','SEGMENT_MIC','OPERATING_MIC','ISO_COUNTRY_CODE','ISO_EXCH_NAME','FROM_DT','THRU_DT']
                return pd.DataFrame(r,columns=headers)

        def getTicker(self,jobs):
                if 'idValue' in jobs[0]:
                        return jobs[0].get('idValue')
                raise ValueError(' No idValue in lookup job.')

        def parseBloombergTicker(self,Ticker):
                # Future contract month and contract year are translated from Bloomberg Ticker
                # For Ticker=XAZ0 Comdty, we assume the following results
                # contract_month = 'DEC'
                # contract_year = 0 
                # asset_category_cd in ('BFU','CFU','IRF','EFU')

                EJV_Inputs = dict()
                tickerLen = len(Ticker.split(' '))
                if tickerLen == 0 or tickerLen > 2:
                        # Assuming Tickers are formated in two parts, i.e. "XAZ0 Comdty"
                        print('\nUnexpected IdType. Expected Bloomberg Ticker format "<Ticker> <marketSector>". For example: "FGRZ0 Index"')
                        print(' Received Ticker {0}\n'.format(Ticker))
                        raise ValueError
                TickerPrefix = Ticker.split(' ')[0]
                contract_year = TickerPrefix[len(TickerPrefix)-1:len(TickerPrefix)]
                try:
                        contract_year = int(contract_year)
                except:
                        # Assuming last character of Bloomberg Tickers are integers
                        print('Failed to convert contract year {0} to integer in Ticker={1}'.format(contract_year,Ticker))
                        raise ValueError

                EJV_Inputs['BBprefix'] = TickerPrefix
                EJV_Inputs['contract_month'] = self.FutureCodeToMonth.get(TickerPrefix[len(TickerPrefix)-2:len(TickerPrefix)-1])
                EJV_Inputs['contract_year'] = contract_year

                if tickerLen == 1:
                        EJV_Inputs['BBsuffix'] = None
                        EJV_Inputs['asset_category_cd'] = None
                        return EJV_Inputs
                TickerSuffix = Ticker.split(' ')[1]
                EJV_Inputs['BBsuffix'] = TickerSuffix
                EJV_Inputs['asset_category_cd'] = self.ejv_bb_asset_category.get(TickerSuffix)
                return EJV_Inputs
                
        def getOpenFigiData(self,jobs):
                # The response is an Array of Objects where the Object at index i contains the results for the Mapping Job at index i in the request.
                # Each Object has one of the following properties:
                # 'data' is present when FIGI(s) are found for the associated Mapping Job. 
                # 'error' is present when no FIGI is found or there was an error when processing the associated Mapping Job.                       
                too_many_mapping_jobs = len(jobs) > (100 if openfigi_apikey else 5)
                assert not too_many_mapping_jobs, 'Too many mapping jobs. Mapping jobs cannot excede 100/min with api key, else 5/min.'
                i=0
                output=[]
                while i <= 100 : # try 100 times
                        i+=1
                        try:
                                r = requests.post(openfigi_url, json=jobs, headers=openfigi_headers)
                                if r.status_code == 429:
                                # 429 returned when limitation is reached in time window.
                                        logging.info('OpenFIGI thinks we have submitted more than 100 jobs in a minute. \n Sleeping 10 seconds.')
                                        time.sleep(10)
                                        logging.info('Retrying now')
                                        continue
                                if r.status_code == 504:
                                        logging.info('OpenFIGI Gateway Time-out. \n Sleeping 10 seconds. ')
                                        time.sleep(30)
                                        logging.info('Retrying now.')
                                        continue
                        except requests.exceptions.ChunkedEncodingError:
                                logging.info('Connection broken error detected. Retrying in 10 seconds.')
                                time.sleep(10)
                                continue
                        break
                if r.status_code == 200:
                        res = r.json()
                        if 'data' in res[0]:
                                # each tuple is a job to result mapping
                                output=list(zip([j['idValue'] for j in jobs],res))
                if r.status_code != 200 and r.status_code != 429 and r.status_code != 504:
                        logging.info('OpenFigi returned status code {1} ({0})'.format(r.reason,r.status_code))
                        logging.error('OpenFigi failed: {}'.format(r.text))
                        raise OpenFigiException(r.text)                                                                
                return output

        def getOpenFigiDataUsingBaseTicker(self,jobs):
                Ticker = self.getTicker(jobs)
                baseTicker = Ticker.split(' ')[0]
                jobs = [{"idType":"TICKER", "idValue":baseTicker}]
                return jobs

        def filterBloombergFutures(self,jobs):
                # Filters api results on futures.
                results = self.getOpenFigiData(jobs)
                if len(results) == 0:
                        job = self.getOpenFigiDataUsingBaseTicker(jobs)
                        results = self.getOpenFigiData(job)
#                        raise ValueError(' No results from API search.')
                output = []
                for job in results:
                        #raise Exception
                        # job is a tuple mapping of lookup to response
                        if 'data' in job[1]:
                                # successful responses
                                try:
                                        for row in job[1]['data']:                                            
                                                try:
                                                        if row['securityType2'] == 'Future' and row['marketSector'] in self.marketSecDes:
                                                                output.append((job[0],row))
                                                                logging.info(' Keeping {} because OpenFigi says it is a Future.'.format(job[0]))
                                                except KeyError as e:
                                                        logging.error(' Unable to find {} in OpenFigi results. OpenFigi likely changed API response properties.'.format(e))
                                                        sys.exit(1)
                                except KeyError as e:
                                        logging.error(' Unable to find key {0} in results in API response: \n {1}.'.format(e,results)) 
                                        sys.exit(1)
                                except Exception as e:
                                        logging.error(' Unable to parse API results. \n {}'.format(e))  
                                        sys.exit(1)
                return output

        def createLookupResultsDataFrame(self,jobs):
                # Transforms the API response into DataFrame 
                lookupResults = self.filterBloombergFutures(jobs)
                i=0
                dfList = []
                for data in lookupResults:
                        df = pd.DataFrame(data[1],index=[i])
                        df['Lookup_Ticker'] = data[0]
                        dfList.append(df)
                        i+=1
                try:
                        combined_df = pd.concat(dfList)
                        combined_df.reset_index(inplace=True,drop=True)
                except ValueError:
                        print(' API does not have any results for job {}'.format(jobs))
                        exit(1)
                return combined_df

        def mapMicToBloomberTicker(self,jobs):
                api_results_df = self.createLookupResultsDataFrame(jobs)
                Ticker = jobs[0].get('idValue')
                bloombergTickerInfo = self.parseBloombergTicker(Ticker)
                bbExchMicMap = self.readBloombergExchMicMap()

                BloombergExchCode = api_results_df['exchCode'].values
                df_match = bbExchMicMap[bbExchMicMap['BLOOMBERG_EXCHANGE_MNEMONIC'] == BloombergExchCode[0]]
                operating_mic = list(df_match.OPERATING_MIC)
                market_mic = list(df_match.SEGMENT_MIC)
                res_dict = {'Ticker':Ticker,'bbExch':BloombergExchCode[0],'operating_mic':operating_mic[0],'market_mic':market_mic[0]}
                res_dict.update(bloombergTickerInfo)
                return res_dict

        def findEJVDerivfutures(self,jobs):
                ''' Takes API results and finds Futures in EJV_Derivs with similar characteristics'''
                TickerInfo = self.mapMicToBloomberTicker(jobs)
                operating_mic = TickerInfo['operating_mic']
                market_mic = TickerInfo['market_mic']
                contract_month = TickerInfo['contract_month']
                contract_year = TickerInfo['contract_year']
                asset_category_cd = TickerInfo['asset_category_cd']

                # Add query supplements as needed to identify asset type
                # assuming all asset_category_cds in the GCODES Database are 3 characters long
                # This is a somewhat hacky solution and could be refined
                if TickerInfo['asset_category_cd'] is None:
                        assetTypeQuery = ''
                        logging.warning(' Could not find corresponding asset_category_cd.')
                elif len(list(asset_category_cd)) <= 3:
                        assetTypeQuery = "and asset_category_cd = '" + asset_category_cd + "'" 
                else:
                        assetTypeQuery = 'and asset_category_cd in {}'.format(asset_category_cd)

                query = """
                SELECT distinct ric_root,exchange_ticker,market_mic,operating_mic,series_desc FROM EJV_derivs.dbo.quote_xref
                WHERE expiration_dt > getdate()
                and ric not like '%c[0-9][10-99]'
                and ric not like '%c[0-9]'
                and trading_status = 1
                and (market_mic = '{0}' or operating_mic = '{1}')
                and left(contract_month_year,3) = '{2}'
                and isnumeric(right(ric,1)) = 1
                and right(ric,1) = {3}
                and rcs_cd = 'FUT'
                and put_call_indicator is null 
                {4}
                """.format(market_mic,operating_mic,contract_month,contract_year,assetTypeQuery)
                if asset_category_cd == 'EIF':
                        query = query + " and series_desc not like '%Dividend Index Future%' and series_desc not like '%Dividend Future%' and series_desc not like '%Equity Future%' and series_desc not like '%Equity Total Return Future%' and series_desc not like '%Single Stock Future%' "
                query = query + " order by ric_root "
                print(' Running this query on EJV_Derivs:\n {}'.format(query))
                self.marketData.dbCursor.execute(query)
                r = self.marketData.dbCursor.fetchall()
                return r 
                
        def lookupEJVDerivFuturesOnOpenFigi(self,jobs):
                EJVresults = self.findEJVDerivfutures(jobs)
                # Bloomberg Ticker we want to map to RIC
                Ticker = self.getTicker(jobs)
                baseTicker = Ticker.split(' ')[0]
                marketSecDes = Ticker.split(' ')[1]
                if marketSecDes not in self.bbFutTickerSuffix:
                        logging.warning(' marketSecDes={} in lookup but not mapped in this scripts definitions.'.format(marketSecDes))
                # OpenFigi API results for EJV Exchange Ticker  
                logging.info(' Running EJV results on OpenFigi...')
                for res in EJVresults: 
                        job = [{"idType":"ID_EXCH_SYMBOL","idValue":res[1],"securityType2":"Future","marketSecDes":marketSecDes,'micCode':res[2]}]
                        print(res)
                        r = self.filterBloombergFutures(job)
                        for i in r:
                                # search for OpenFigi results matching original Bloomberg ticker and return the first match.
                                # If direct match is found we exit since API results are unique. 
                                if i[1].get('uniqueIDFutOpt') == Ticker:
                                        print(i)
                                        df = pd.DataFrame(i[1],index=[i[0]])
                                        df['RIC_ROOT'] = res[0]
                                        return df
                # Search again using TICKER instead of ID_EXCH_SYMBOL
                for res in EJVresults:
                        job = self.getOpenFigiDataUsingBaseTicker(jobs)
                        r = self.filterBloombergFutures(job)
                        for i in r:
                                if i[1].get('ticker') == baseTicker:
                                        print(i)
                                        df = pd.DataFrame(i[1],index=[i[0]])
                                        df['RIC_ROOT'] = res[0]
                                        return df
                print(' Could not find matching RIC or exchange ticker for {0} using OpenFigi.'.format(Ticker))
                return None

        def findDataSteamFutures(self,ExchTicker):
                query=""" select cr.ContrCode,cr.ContrTypeCode,cr.ContrName,cr.ExchTickerSymb, ft.Desc_ as exchange, 
                ft2.Desc_ as underlying, cls.TrdPlatformCode, cls.TrdStatCode
                from dbo.DSFutContr cr,  
                dbo.DSFutcode ft, 
                dbo.DSFutcode ft2, 
                dbo.DSFutClass cls
                where cr.ExchTickerSymb='{0}' and 
                ft.Code=cr.SrcCode and ft.Type_=1 
                and ft2.Code=cr.UndrInstrCode and ft2.Type_=2 
                and cls.ContrCode=cr.ContrCode""".format(ExchTicker)
                self.mssql.dbCursor.execute(query)
                r = self.mssql.dbCursor.fetchall()
                headers = ['ContrCode','ContrTypeCode','ContrName','ExchTickerSymb','exchange','underlying','TrdPlatformCode','TrdStatCode']
                df = pd.DataFrame(r,columns=headers) 
                return df

        def checkDataStreamLinkage(self,ejv_api_results):
                if ejv_api_results is not None:
                        r = self.findDataSteamFutures(ejv_api_results.index.item())
                        if len(r) == 0:
                                logging.info(' No DataStream matches.')
                                return None
                        logging.info(' DataStream results:')
                        return r
                return None
        
        def findFutures(self,jobs):
                # Single security lookup
                Ticker = jobs[0].get('idValue') 
                query = """SELECT * from Xref WHERE SecurityIdentifierType='Ticker' AND SecurityIdentifier='{}'""".format(Ticker)
                print(' Checking Xref ')
                self.marketData.dbCursor.execute(query)
                r = self.marketData.dbCursor.fetchall()
                if len(r) > 0:
                        print(' Found AxiomaDataId')
                        headers = ['AxiomaDataId','SecurityIdentifierType','SecurityIdentifier','FromDate','ToDate','Lud','Lub']
                        df = pd.DataFrame(r,columns=headers) 
                        return df
                print(' AxiomaDataId not found')
                return None

if __name__ == '__main__':
        usage = "usage: %prog [options] <config file name> <Bloomberg Ticker> --user <user> --passwd <passwd> --report-file <report-file> --database <database>"
        cmdlineParser = optparse.OptionParser(usage=usage)
        Utilities.addDefaultCommandLine(cmdlineParser)
                    
        cmdlineParser.add_option("--user", action="store",
                                 default='DummyUsername', dest="user",
                                 help="DB user name")
        cmdlineParser.add_option("--passwd", action="store",
                                 default='DummyPassword', dest="passwd",
                                 help="DB password name")
        cmdlineParser.add_option("--database", action="store",
                                 default='MarketData', dest="database",
                                 help="DB name")
        cmdlineParser.add_option("--host", action="store",
                                 default='DummyDatabase', dest="host",
                                 help="DB server name")
        cmdlineParser.add_option("--check-instrumentxref", action="store",
                                 dest="checkXref", default=False,
                                 help="Check MarketData DB coverage")
        (options_, args_) = cmdlineParser.parse_args()
        configFile_ = open(args_[0])
        config_ = configparser.ConfigParser()
        config_.read_file(configFile_)
        configFile_.close()
        connections = Connections.createConnections(config_)
        dbConn=pymssql.connect(user=options_.user, password=options_.passwd, host=options_.host, database=options_.database)
        findmapping = FindFutureMapping(dbConn,config_, connections)
        Bloomberg_Ticker = args_[1]
        data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":Bloomberg_Ticker}]
        if options_.checkXref:
                checkMarketData = findmapping.findFutures(data)
                print(checkMarketData)
        checkEJV = findmapping.lookupEJVDerivFuturesOnOpenFigi(data)
        print(checkEJV)
        checkDS = findmapping.checkDataStreamLinkage(checkEJV)
        print(checkDS)

        #### Test cases ####
        #data = [{"idType":"UNIQUE_ID_FUT_OPT", "idValue":"ACCZ0 Index"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":'MWBZ0 Index'}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":'LFOZ0 Index'}]
        #data = [{"idType":"TICKER", "idValue":"ACCZ0"}] 
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":'HWRZ0 Index'}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":'HJAV0 Index'}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":'FGRZ0 Index'}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"FGRZ00 Index"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"XAZ0 Comdty"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"RSWZ0 Index"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"2LZ2 Comdty"}]
