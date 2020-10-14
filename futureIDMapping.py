import requests 
import sys
import optparse
import pymssql
import configparser
import pymssql
import numpy as np
import pandas as pd
import logging
import time

from marketdb import Utilities

'''
See https://www.openfigi.com/api for more information.
'''

openfigi_url = 'https://api.openfigi.com/v2/mapping'
openfigi_apikey = 'dummy_api_key' # to be replaced
openfigi_headers = {'Content-Type': 'application/json'}

if openfigi_apikey: 
        openfigi_headers['X-OPENFIGI-APIKEY'] = openfigi_apikey

class OpenFigiException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return 'OpenFigi api returned error {0}'.format(self.parameter)

class FindFutureMapping:
        
        def __init__(self,db):
                self.cursor=db.cursor()
                self.FutureMonthMap=({'JAN':'F','FEB':'G','MAR':'H','APR':'J','MAY':'K','JUN':'M','JUL':'N','AUG':'Q','SEP':'U','OCT':'V','NOV':'X','DEC':'Z'})
                self.bloombergSecurityTypes = {'Financial index future.': 'Index','Generic currency future.':'Curncy','Generic index future.':'Index','Physical commodity future.': 'Comdty','Financial commodity future.': 'Comdty','SINGLE STOCK DIVIDEND FUTURE': 'Equity','DIVIDEND NEUTRAL STOCK FUTURE': 'Equity', 'Currency future.': 'Curncy', 'Physical index future.': 'Index'}
                self.marketSecDes = list(set(self.bloombergSecurityTypes.values()))
                self.bbFutTickerSuffix = ('Comdty','Curncy','Index','Equity')
                self.ejv_asset_category_cd = ({'BFS:Comdty','BFU:Comdty','CFU:Curncy','CMF:Comdty','EIF:Index','IRF:Comdty'})
                self.ejv_asset_category_descr = ({'BFS':'Bond Future Spread','BFU':'Bond Future','CFU':'Currency Future','CMF':'Commodity Future','EIF':'Equity/Index Future','IRF':'Interest Rate Future'})

                self.TRBBGFutType={('Commodity Future','Commodity Future'):'Commodity Future',
                                   ('Commodity Future','Transportation Future'):'Commodity Future',
                                   ('Commodity Future','Weather Future'):'Commodity Future',
                                   ('Commodity Future','Energy Future'):'Commodity Future',
                                   ('Index Future','Equity/Index Future'):'Index Future',
                                   ('Equity Future','Equity/Index Future'):'Equity Future',
                                   ('Currency Future','Commodity Future'):'Currency Future',
                                   ('Bond Future/IR Future','Bond Future'):'Bond Future',
                                   ('Bond Future/IR Future','Interest Rate Future'):'Interest Rate Future'}

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
                        # each tuple is a job<>result mapping
                        output=list(zip(jobs,r.json()))
                if r.status_code != 200 and r.status_code != 429 and r.status_code != 504:
                        logging.info('OpenFigi returned status code {1} ({0})'.format(r.reason,r.status_code))
                        logging.error('OpenFigi failed: {}'.format(r.text))
                        raise OpenFigiException(r.text)                                                                
                return output

        def filterBloombergFutures(self,jobs):
                # Filters api results on futures.
                results=self.getOpenFigiData(jobs)
                if len(results) == 0:
                        return 'No results found for {}'.format(jobs)
                output = []
                for job in results:
                        # job is a tuple mapping lookup to response
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
                        else:
                                # Jobs encountering OpenFigi errors
                                err = 'No identifier found.'
                                err_msg = job[1].get('error')
                                if job[1]['error'] != err: 
                                        logging.error(' Error processing id: {}. Skipping the job.'.format(job[0]) + '\n' + err_msg)
                                else:
                                        logging.info(' {0} Skipping {1}'.format(err,job[0]))
                return output
        
        def EJVRIC(self,Input,ID,DT):
                if Input=='RIC-DT':
                        query="""select distinct substring(ric,1,len(ric_root)+2) as CleanRIC,
                        ric_root,rcs_cd,asset_category_cd,
                        case when asset_category_cd='EFU' then 'Energy Future'
                        when asset_category_cd='BFU' then 'Bond Future'
                        when asset_category_cd='CFU' then 'Currency Future'
                        when asset_category_cd='CMF' then 'Commodity Future'
                        when asset_category_cd='EIF' then 'Equity/Index Future'
                        when asset_category_cd='IRF' then 'Interest Rate Future'
                        when asset_category_cd='TFU' then 'Transportation Future'
                        when asset_category_cd='WFU' then 'Weather Future'
                        else 'Not Identified' end TRCategory,
                        exchange_ticker,description,series_desc,
                        asset_category_cd,contract_month_year,trading_status,trading_style,periodicity,
                        currency_cd,delivery_method,exchange_cd,exercise_style_cd,convert(varchar,expiration_dt,110) as expiration_dt,
                        convert(varchar,first_trade_dt,110) as first_trade_dt, convert(varchar,last_trading_dt,110) as last_trading_dt,
                        underlying_ric,unscaled_strike_px,
                        lot_size,lot_units_cd,ric,ric_root,sec_asset_underlying_ric,series_desc,tick_value
                        from [ejv_derivs].dbo.quote_xref where ric like '%s' and rcs_cd in ('fut','bondfut') and ric not like '%s'
                        and expiration_dt>='%s' """%(ID+'%','%-%',DT)                                        
                elif Input=='RICRoot-MonYear':
                        query="""select distinct substring(ric,1,len(ric_root)+2) as CleanRIC,ric_root,rcs_cd,asset_category_cd,
                        case when asset_category_cd='EFU' then 'Energy Future'
                        when asset_category_cd='BFU' then 'Bond Future'
                        when asset_category_cd='CFU' then 'Currency Future'
                        when asset_category_cd='CMF' then 'Commodity Future'
                        when asset_category_cd='EIF' then 'Equity/Index Future'
                        when asset_category_cd='IRF' then 'Interest Rate Future'
                        when asset_category_cd='TFU' then 'Transportation Future'
                        when asset_category_cd='WFU' then 'Weather Future'
                        else 'Not Identified' end TRCategory,
                        exchange_ticker,description,series_desc,
                        asset_category_cd,contract_month_year,trading_status,trading_style,periodicity,
                        currency_cd,delivery_method,exchange_cd,exercise_style_cd,convert(varchar,expiration_dt,110) as expiration_dt,
                        convert(varchar,first_trade_dt,110) as first_trade_dt, convert(varchar,last_trading_dt,110) as last_trading_dt,
                        underlying_ric,unscaled_strike_px,
                        lot_size,lot_units_cd,sec_asset_underlying_ric,series_desc,tick_value from [ejv_derivs].dbo.quote_xref where
                        ric_root='%s' and contract_month_year='%s' and underlying_contract is null and ric not like '%s'
                        and rcs_cd in('fut','bondfut')
                        """%(ID,DT,'%-%')
                self.cursor.execute(query)
                openfigi_headers=[r[0] for r in self.cursor.description]
                result=self.cursor.fetchall()
                EJVOutput=[]
                for i in result:
                        dict={}
                        for j in range(len(openfigi_headers)):
                                dict[openfigi_headers[j]]=i[j]
                        EJVOutput.append(dict)
                return EJVOutput

        def EJVRICRoot(self,ricroot,MonYear):
                query="""select distinct substring(ric,1,len(ric_root)+2) as CleanRIC,ric_root,rcs_cd,asset_category_cd,exchange_ticker,description,series_desc,
                asset_category_cd,contract_month_year,trading_status,trading_style,periodicity,
                currency_cd,delivery_method,exchange_cd,exercise_style_cd,convert(varchar,expiration_dt,110) as expiration_dt,
                convert(varchar,first_trade_dt,110) as first_trade_dt, convert(varchar,last_trading_dt,110) as last_trading_dt,
                underlying_ric,unscaled_strike_px,
                lot_size,lot_units_cd,sec_asset_underlying_ric,series_desc,tick_value from [ejv_derivs].dbo.quote_xref where 
                ric_root='%s' and contract_month_year='%s' and underlying_contract is null and ric not like '%s'
                and rcs_cd in('fut','bondfut')
                """%(ricroot,MonYear,'%-%')
                self.cursor.execute(query)
                openfigi_headers=[r[0] for r in self.cursor.description]
                result=self.cursor.fetchall()
                return openfigi_headers,result
        
        def findDataSteamFutures(self,ExchTicker):
                query=""" select cr.ContrCode,cr.ContrTypeCode,cr.ContrName,cr.ExchTickerSymb, ft.Desc_ as exchange, 
                ft2.Desc_ as underlying,ft.Desc_ as exchange, cls.TrdPlatformCode, cls.TrdStatCode
                from PROD_VNDR_DB.qai.dbo.DSFutContr cr,  
                PROD_VNDR_DB.qai.dbo.DSFutcode ft, 
                PROD_VNDR_DB.qai.dbo.DSFutcode ft2, 
                PROD_VNDR_DB.qai.dbo.DSFutClass cls
                where cr.ExchTickerSymb='{0}' and 
                ft.Code=cr.SrcCode and ft.Type_=1 
                and ft2.Code=cr.UndrInstrCode and ft2.Type_=2 
                and cls.ContrCode=cr.ContrCode""".format(ExchTicker)
                self.cursor.execute(query)
                return self.cursor.fetchall()

        def FutureMap(self,BBGTicker,RIC,Dt):
                RIC=self.EJVRIC('RIC-DT',RIC,Dt)
                RICdf=pd.DataFrame(RIC[1],columns=RIC[0])
                BBGOutput=self.getOpenFigiData('TICKER',BBGTicker)
                exchticker=[]
                for i in RIC[1]:
                        if i[2] not in exchticker:
                                exchticker.append(i[2])
                BBGExchTicker=dict()
                BBGMap={}
                result=pd.DataFrame()
                for ticker in exchticker:
                        BBGExchTicker[ticker]=self.getOpenFigiData('ID_EXCH_SYMBOL',ticker)
                        if len(BBGExchTicker[ticker])==0:
                                result['Message']=['No BBG record found!']
                        else:
                                BBGMap[ticker]=BBGOutput.merge(BBGExchTicker[ticker],on='figi',how='inner')
                                if len(BBGMap[ticker]) ==1:
                                        result=pd.concat([RICdf[RICdf['exchange_ticker']==ticker],BBGMap[ticker]],axis=1)
                                        result['Message']=['One Mapping Found!']
                                        logging.info('One Mapping Found!')
                                        break
                                elif len(BBGMap[ticker]) >1:
                                        result['Message']=['Multiple Mapping Found!']
                                else:
                                        result['Message']=['No Mapping Found!']
                return result

        def FutureMapCheck(self,BBGTicker,Input,RIC,Dt):
                 RIC=self.EJVRIC(Input,RIC,Dt)
                 BBGOutput=self.getBBGFuture('TICKER',BBGTicker)
                 output=dict()
                 if len(RIC)==0 and len(BBGOutput)==0:
                         output['Comment']='No RIC and No BBG-ID Found!'
                 elif len(RIC)==0 and len(BBGOutput)>0 and BBGOutput[0]=='N':
                         output['Comment']=BBGOutput
                 elif len(RIC)==0 and len(BBGOutput)>0 and BBGOutput[0]!='N':
                         for bbg in BBGOutput:
                                 output=bbg
                                 output['Comment']='Only BBG-ID Found!'
                 elif len(RIC)>0 and len(BBGOutput)==0:
                         for ric in RIC:
                                 output=RIC
                                 output['Comment']='Only RIC Found!'
                 elif len(RIC)>0 and len(BBGOutput)>0 and BBGOutput[0]!='N':
                         for ric in RIC:
                                 for bbg in BBGOutput:
                                         if ric.get('CleanRIC')[-2:]==bbg.get('ticker')[-2:]:
                                                 if (bbg.get('BBGFutureType'),ric.get('TRCategory')) in list(self.TRBBGFutType.keys()):
                                                         output=dict(list(ric.items())+list(bbg.items()))
                                                         output['FutureType']=self.TRBBGFutType.get((bbg.get('BBGFutureType'),ric.get('TRCategory')))
                                                         output['Comment']='Mapping found!'
                                                 else:
                                                         output=dict(list(ric.items())+list(bbg.items()))
                                                         output['Comment']='Mapping Not Match!'
                                                         output['FutureType']='Future Type Not Match!'

                 elif len(RIC)>0 and len(BBGOutput)>0 and BBGOutput[0]=='N':
                         for ric in RIC:
                                 output=ric
                                 output['Comment']='Only RIC Found!'
                 
                 return output
        
        def testfunction(self,CategoryEnum,dt):
                CategoryEnumlist=['Commodity Future','Currency Future','Equity Future','Equity Index Future','Interest Rate Future','Bond Future',
                                  'Equity Volatility Index Future','Equity Index Future - Derivs','Equity Index Future - Axioma','Commodity Future - Axioma','Commodity Future - Derivs']
                query2="""select ce.reportingSubCategoryName,ce.ReportingCategoryName,
                dsmp.DataScope_RicRoot,dsmp.BloombergTickerPreface,dsmp.Description 
                from marketdata.dbo.datastreamfuturesmapping dsmp, Metadata.dbo.CategoryEnum ce where dsmp.CategoryEnum=ce.CategoryEnum
                and ce.reportingSubCategoryName in ('%s') and BloombergTickerPreface<>''"""%(CategoryEnum)
                self.cursor.execute(query2)
                result2=self.cursor.fetchall()
                Map={}
                for i in result2:
                        RICRoot=i[2]
                        query3="""select  RIC_root,contract_month_year from EJV_Derivs.dbo.quote_xref where ric_root='%s' and rcs_cd in ('fut','bondfut') and expiration_dt in
                        (select max(expiration_dt) from EJV_Derivs.dbo.quote_xref where ric_root='%s' and expiration_dt<='%s')"""%(RICRoot,RICRoot,dt)
                        self.cursor.execute(query3)
                        result3=self.cursor.fetchall()
                        for j in result3:
                                suffix=self.FutureMonthMap.get(j[1][:3])+j[1][-1]
                                BBG=i[3]+suffix
                                test=self.FutureMapCheck(BBG,'RICRoot-MonYear',RICRoot,j[1])
                                print(CategoryEnum,RICRoot,j[1], BBG, test.get('Comment'),test.get('ric'),test.get('ric_root'),test.get('ticker'),test.get('FutureType'))
                return ''
         
        #delete?# 
        def CheckEJVBBGMap(self,ricroot,MonYear):
                RICRoot=self.EJVRICRoot(ricroot,MonYear)
                RICdf=pd.DataFrame(RICRoot[1],columns=RICRoot[0])
                exchticker=[]
                for i in  RICRoot[1]:
                        if i[2] not in exchticker:
                                exchticker.append(i[2])
                BBGExchTicker=dict()
                for ticker in exchticker:
                        BBGExchTicker[ticker]=self.getOpenFigiData('ID_EXCH_SYMBOL',ticker)
                        BBG=BBGExchTicker[ticker][(BBGExchTicker[ticker]['marketSector']=='Index')&(BBGExchTicker[ticker]['securityType2']=='Future')]
                        for i in BBG['ticker']:
                                if i[-2:]==self.FutureMonthMap.get(MonYear[0:3])+MonYear[-1]:
                                        newBBG=BBG[BBG['ticker']==i]
                                        newBBG['exchange_ticker']=ticker
                                        RIC=RICdf[RICdf['exchange_ticker']==ticker]
                                        result=RIC.merge(newBBG,on='exchange_ticker',how='outer')
                                        break
                                else:
                                        result=[]                                        
                return result

if __name__ == '__main__':
        usage = "usage: %prog [options] --user <user> --passwd <passwd> --report-file <report-file> --database <database> DATE"
        cmdlineParser = optparse.OptionParser(usage=usage)
        Utilities.addDefaultCommandLine(cmdlineParser)
                    
        cmdlineParser.add_option("--user", action="store",
                                 default='dummy_un', dest="user",
                                 help="DB user name")
        cmdlineParser.add_option("--passwd", action="store",
                                 default='dummy_pw', dest="passwd",
                                 help="DB password name")
        cmdlineParser.add_option("--database", action="store",
                                 default='MarketData', dest="database",
                                 help="DB name")
        cmdlineParser.add_option("--host", action="store",
                                 default='prod-mac-mkt-db', dest="host",
                                 help="DB server name")
        (options_, args_) = cmdlineParser.parse_args()

        dbConn=pymssql.connect(user=options_.user, password=options_.passwd, host=options_.host, database=options_.database)
        findmapping=FindFutureMapping(dbConn)
        #data = [{ "idType": "ID_ISIN", "idValue": "US4592001014" }, { "idType": "TICKER", "idValue": "MDRZ0" ,"marketSecDes":"CURNCY"}, { "idType":"TICKER", "idValue":"NDV20","marketSecDes":"Comdty"}, { "idType": "ID_WERTPAPIER", "idValue": "851399", "exchCode": "US" }, { "idType": "ID_BB_UNIQUE", "idValue": "EQ0010080100001000", "currency": "USD" }, { "idType": "ID_SEDOL", "idValue": "2005973", "micCode": "EDGX", "currency": "USD" }, { "idType":"BASE_TICKER", "idValue":"TSLA 10 C100", "securityType2":"Option", "expiration":["2018-10-01", "2018-12-01"]}, { "idType":"BASE_TICKER", "idValue":"NFLX 9 P330", "marketSecDes":"Equity", "securityType2":"Option", "strike":[330,None], "expiration":["2018-07-01",None]}, { "idType":"BASE_TICKER", "idValue":"FG", "marketSecDes":"Mtge", "securityType2":"Pool", "maturity":["2019-09-01", "2020-06-01"]}, { "idType":"BASE_TICKER", "idValue":"IBM", "marketSecDes":"Corp", "securityType2":"Corp", "maturity":["2026-11-01",None]}, { "idType":"BASE_TICKER", "idValue":"2251Q", "securityType2":"Common Stock", "includeUnlistedEquities": True}]
        #data =  [{"idType":"ID_EXCH_SYMBOL","idValue":"ATW","securityType2":"Future","exchCode":"ICF"}]
        #data = [{ "idType":"BASE_TICKER", "idValue":"XAZ0 Comdty"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"XAZ0 Comdty"},{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"2LZ2 Comdty"},{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"0LZ1 Comdty"}]
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"A2LV=Z2 GR Equity"}] #sinlge stock dividend future
        #data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"XAZ0 Comdty"}]
        data = [{ "idType":"UNIQUE_ID_FUT_OPT", "idValue":"SFIH2 Comdty"}]
        #data = [{ "idType":"BASE_TICKER", "idValue":"IBM", "marketSecDes":"Corp", "securityType2":"Corp", "maturity":["2026-11-01",None]}]
        #data = [{ "idType":"TICKER", "idValue":"NDV20","marketSecDes":"Comdty"},{ "idType": "TICKER", "idValue": "MDRZ0" ,"marketSecDes":"Curncy"},{ "idType": "TICKER", "idValue": "MDRZ0" ,"marketSecDes":"CURNCY"}]
        BBGFutureMappings = findmapping.filterBloombergFutures(data)
        print(BBGFutureMappings)
