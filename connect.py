import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import PyWinDDE
import tejapi
import string
from datetime import datetime, timedelta, time,date
from pandas_market_calendars import get_calendar
from ratio_class import Save_date
from config import Config
import time as t
path='//192.168.60.83/Wellington/Allen/Index'
twn_path='//192.168.60.83/Wellington/Allen/Index/BloombergTWNI/'
twn_path = '//192.168.60.81/Wellington/Faker/富台權重/'
index_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index')
TWN_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index\BloombergTWNI')
TWN_list = os.listdir(r'\\192.168.60.81\Wellington\Faker\富台權重')
class TEJ_data :
    def __init__(self,config,current_date) -> None:
        self.config = config
        self.current_date = current_date
    def tej_data(self):
        tejapi.ApiConfig.api_key = self.config.TEJ_API_KEY
        data=tejapi.get('TWN/AMT', paginate=True)
        table_info = tejapi.table_info('TWN/AMT') #公司股東會資料資料庫
        #Ytable_info.to_csv('table_info.csv')
        cname_mapping_dict=table_info['columns']
        data.columns=[cname_mapping_dict[i]['cname'] for i in data.columns]
        data["會議日期"]=pd.to_datetime(data["會議日期"].apply(lambda x:str(x)[0:10]))
        data["除息日"]=pd.to_datetime(data["除息日"].apply(lambda x:str(x)[0:10]))
        data["除權日(配股)"]=pd.to_datetime(data["除權日(配股)"].apply(lambda x:str(x)[0:10]))
        data['公司']=data['公司'].astype(str)
        data.to_csv('data.csv')
        下市data=tejapi.get('TWN/AIND', paginate=True)
        table_info = tejapi.table_info('TWN/AIND')
        print("下市data",table_info)
        cname_mapping_dict=table_info['columns']
        下市data.columns=[cname_mapping_dict[i]['cname'] for i in 下市data.columns]
        下市股票 = 下市data[['公司簡稱','下市日期']].dropna().reset_index(drop = True)
        下市股票['下市年分'] =下市股票.下市日期.apply(lambda x : int(str(x)[0:4]))
        print(下市股票)
        下市股票.to_csv('下市股票.csv')
        return self.delete_Delisting_data(下市股票,data)
    def delete_Delisting_data(self,下市股票,data):
        delist_dic = dict(zip(下市股票.公司簡稱.unique(), 下市股票.下市年分))
        data = data.sort_values(by='會議日期')
        set_data = data.set_index(['會議日期', '公司']).copy().sort_index()
        除息_use_data = set_data[set_data['現金股利(元)'].fillna(0) != 0].sort_index() #已經確認的除息data 並且有發放現金股利 才會影響點數
        除息_use_data['開會年度'] = 除息_use_data['開會年度'].astype(str).str[:4].astype(int)

        開會條件 = (除息_use_data['開會年度'].isin([self.current_date.year - 1, self.current_date.year]))
        print("開會條件",開會條件)
        確認除息_use_data = 除息_use_data[~除息_use_data['除息日'].isna() & 開會條件]

        今年開會公司 = 除息_use_data[開會條件].reset_index().sort_values(['公司', '會議日期']).reset_index(drop=True).groupby('公司').tail(1) #最新一期的開會

        今年預估除息點數 = 今年開會公司[今年開會公司['除息日'].isna()].reset_index(drop=True)
        今年預估除息點數['下市年度'] = 今年預估除息點數['公司'].map(delist_dic)
        今年預估除息點數 = 今年預估除息點數[今年預估除息點數['下市年度'].isna()] #過濾下市
        今年預估除息點數 = 今年預估除息點數[今年預估除息點數['常會YN／董事會D'] != "D"] #過濾董事會
        今年預估除息點數.to_csv('今年預估除息點數.csv')
        去年除息日期 = 除息_use_data[除息_use_data['開會年度'] != self.current_date.year]
        predict_exist_company = []
        predict_exist_calculate_day = []
        for i in 去年除息日期.index:
            if pd.isna(去年除息日期['除息日'].loc[i][0]): # 去年除息日沒資料的話拿除權日來補
                去年除息日期['除息日'].loc[i][0]=去年除息日期['除權日(配股)'].loc[i][0]
        去年除息日期.loc[:,'相差日期']=(去年除息日期.reset_index()['除息日']-去年除息日期.reset_index()['會議日期']).values
        去年除息日期=去年除息日期.reset_index().set_index('公司')
        去年除息日期 = 去年除息日期[去年除息日期['相差日期'].notna()]
        今年預估除息點數=今年預估除息點數.reset_index().set_index('公司')
        for i in 今年預估除息點數.index:
            if i[-1] in string.ascii_uppercase:
                continue
            elif i in 去年除息日期.index:
                predict_exist_company.append(i)
                predict_exist_calculate_day.append(今年預估除息點數.loc[i]["會議日期"])
        return predict_exist_company,predict_exist_calculate_day,去年除息日期,今年預估除息點數,確認除息_use_data
    def calculate_dividend_date(self,predict_exist_company,predict_exist_calculate_day,去年除息日期,今年預估除息點數,確認除息_use_data):
        predict_company=set(predict_exist_company) # 歷年有資料的再估計
        predict_start = predict_exist_calculate_day
        predict_date_list=[]
        dividend_list=[]
        total_firm_df = 去年除息日期['相差日期'].dropna()
        calculate_df = 去年除息日期[去年除息日期['相差日期'].notna()]
        #台指前30 = ["2330","2317","2454","2412","6505","2308","2881","2882","2303","1303","1301","2002","2886","3711","2891","1326","1216","5880","5871","2884","2892","2207","3045","2603","2880","2382","3008","2395","2885","2912"]
        #富台前30 = ["2330","2317","2454","2308","2303","2881","2412","1303","2891","2882","2002","2886","3711","2884","1301","5871","1216","2892","2885","5880","1326","1101","2880","3034","3008","2327","2382","2883","2207","2357"]
        前30大權值股 = self.config.TOP_30_RATIO_STOCK#["2330","2317","2454","2412","6505","2308","2881","2303","2882","1303","1301","2002","3711","2886","2891","1326","1216","5880","5871","2884","2892","3045","2207","2603","3008","2382","2880","2885","2912","2395","1101","3034","2327","2883","2357","2609","4904","5876","2615","6415","1590","2890","3037","2379","2887","1605","4938","2801","2408","2301","2345","2409","2474","3529","8069"]
        前30大預估權值股 = []
        predict_company_sort = []
        predict_company_complete = []
        print("total firm df \n",total_firm_df)
        int_list_filter = [int(pc) for pc in predict_company if pc.isdigit() and len(pc) >= 4]
        int_list_filter.sort()
        predict_company_sort = [str(i) for i in int_list_filter]
        print("predict_company_sort :",predict_company_sort)
        for num, company_name in enumerate(list(predict_company_sort)):
            if company_name in total_firm_df.index:
                if isinstance(total_firm_df.loc[company_name], pd.core.series.Series):
                    if len(total_firm_df.loc[company_name]) > 1:
                        for j in range(len(去年除息日期.loc[company_name])):
                            if pd.notna(total_firm_df.loc[company_name][j]) and total_firm_df.loc[company_name][j] >= pd.to_timedelta('1D'):
                                if "原會議日" in calculate_df.loc[company_name].臨時會開會目的[j]:
                                    continue
                                else :
                                    use_days = total_firm_df.loc[company_name][j]
                                    參考日期 = str(去年除息日期[去年除息日期['相差日期'].notna()].loc[company_name].會議日期[j])[0:10]
                                    除息日期 = str(去年除息日期[去年除息日期['相差日期'].notna()].loc[company_name].除息日[j])[0:10]

                if isinstance(total_firm_df.loc[company_name],pd._libs.tslibs.timedeltas.Timedelta) :
                        use_days = total_firm_df.loc[company_name]
                        參考日期 = str(去年除息日期[去年除息日期['相差日期'].notna()].loc[company_name].會議日期)[0:10]
                        除息日期 = str(去年除息日期[去年除息日期['相差日期'].notna()].loc[company_name].除息日)[0:10]

            if isinstance(今年預估除息點數.loc[company_name], pd.core.frame.DataFrame) and use_days >= pd.to_timedelta('1D'):
                predict_date = 今年預估除息點數.loc[company_name]['會議日期'][-1] + use_days
                dividend = 今年預估除息點數.loc[company_name]['現金股利(元)'][-1]
            if isinstance(今年預估除息點數.loc[company_name], pd.core.series.Series) and use_days >= pd.to_timedelta('1D'):
                predict_date = 今年預估除息點數.loc[company_name]['會議日期'] + use_days
                dividend = 今年預估除息點數.loc[company_name]['現金股利(元)']


            if use_days < pd.to_timedelta('1D'):
                print("%s 最新一筆除息日期為負，因此不納入除息預估" %(company_name))
                print("\n")
            elif use_days >= pd.to_timedelta('1D'):
                predict_date_list.append(predict_date)
                dividend_list.append(dividend)
                predict_company_complete.append(company_name)

            if company_name in 前30大權值股 and use_days >= pd.to_timedelta('1D'):
                print("預估公司: %s (此股為台指/富台前三十大成分股)，預估除息日間隔: %s，今年會議日期: %s，今年預估除息日期: %s，參考日期: %s，參考除息日期: %s" %(company_name, use_days, str(predict_start[num])[0:10], str(predict_date)[0:10], 參考日期, 除息日期))
                前30大預估權值股.append(company_name)
            elif use_days >= pd.to_timedelta('1D'):
                print("預估公司: %s，預估除息日間隔: %s，今年會議日期: %s，今年預估除息日期: %s，參考日期: %s，參考除息日期: %s" %(company_name, use_days, str(predict_start[num])[0:10], str(predict_date)[0:10], 參考日期, 除息日期))
            print("\n")

        print("\n除息點數")
        predict_date_df=pd.DataFrame((predict_date_list,predict_company_complete,dividend_list)).T
        predict_date_df.columns=['除息日','公司','現金股利(元)']
        predict_date_df['確認/預估']='預估' #都先預估
        for special_ratio_stock in self.config.SPECIAL_STOCK_DIVIDEND_DATE:
            if special_ratio_stock in predict_date_df.公司.to_list() :
                predict_date_df.loc[predict_date_df[predict_date_df['公司'] == special_ratio_stock].index[0],"除息日"] = self.config.SPECIAL_STOCK_DIVIDEND_DATE[special_ratio_stock]    
        predict_date_time_list=[]
        for i in predict_date_df['除息日']:
            if i<pd.Timestamp(self.current_date.date()): # 預估除息日小於今天 則再加14天
                predict_date_time_list.append(pd.to_datetime(self.current_date.date()+pd.to_timedelta('14D'))) #最短14天
            else:
                predict_date_time_list.append(i)

        predict_date_df['除息日']=predict_date_time_list


        確認除息公司=確認除息_use_data.reset_index()[['除息日','公司','現金股利(元)']]
        確認除息公司['確認/預估']='確認'
        print("確認除息公司 : \n",確認除息公司)

        Total_dividend_df=pd.concat([predict_date_df,確認除息公司], join='inner').set_index(['除息日'])
        #.set_index(['除息日'])
        print(Total_dividend_df)
        Total_dividend_df=Total_dividend_df.sort_index()
        Total_dividend_df['公司']=Total_dividend_df['公司'].apply(lambda x:x.split(' ')[0])
        print("Total dividend : ",Total_dividend_df)
        Total_dividend_df['除息日_str'] = Total_dividend_df.index
        Total_dividend_df['台指合約月份'] = Total_dividend_df.除息日_str.apply(lambda x : TXF_contract_month(str(x)[:10]))
        Total_dividend_df['富台合約月份'] = Total_dividend_df.除息日_str.apply(lambda x : TWN_contract_month(str(x)[:10]))
        Total_dividend_df = Total_dividend_df.drop(['除息日_str'], axis=1)
        print(Total_dividend_df)
        return Total_dividend_df,predict_date_df,前30大預估權值股
class Calculate_constitute_date :
    TWNI_price = 0
    TWII_price = 0
    TF_price = 0
    TWN_price = 0
    TX_price = 0
    MTW_price = 0
    XI_price = 0
    TX_use_data = None
    TWN_use_data = None
    MTW_use_data = None
    加權指數成分股 = 電子指數成分股 = 金融指數成分股 = 非金電指數成分股 = 摩根data = 富台指數成分股 = None
    def __init__(self,DQ_stock_dde,DQ_index_dde,DQ_future_dde,today_date,config):
        self.last_price(DQ_stock_dde,DQ_index_dde,DQ_future_dde,today_date)
        self.save_data = Save_date(today_date,config)
        self.Init_constitute_data(path)
    def last_price(self,DQ_stock_dde,DQ_index_dde,DQ_future_dde,today_date):

        self.TWII_price=float(DQ_stock_dde.request('#001.129').decode()) # 加權昨收
        self.TF_price=float(DQ_stock_dde.request('#010.129').decode()) # 金融股現貨昨收
        self.TE_price=float(DQ_stock_dde.request('#020.129').decode()) # 電子股現貨昨收
        self.TX_price=float(DQ_future_dde.request('WTX&.129').decode())
        self.TWN_price=float(DQ_future_dde.request('STWN&.129').decode())
        self.XI_price= float(DQ_future_dde.request('WXI00.129').decode())

        if datetime.now().time() < time(hour = 9): # 如果小於早上9點 
            self.TWNI_price = float(DQ_index_dde.request('FTCRTWTN.125').decode()) # 收昨天成交價
        else :
            self.TWNI_price = float(DQ_index_dde.request('FTCRTWTN.129').decode()) # 收昨天收盤價
        print("\n")
        print("富台現貨昨收 : %s" %(self.TWNI_price))
        print("台指現貨昨收 : %s" %(self.TWII_price))
        print("金融股現貨昨收 : %s" %(self.TF_price))
        print("電子股現貨昨收 : %s" %(self.TE_price))
        print("富台期昨收 : %s" %(self.TWN_price))
        print("台指期貨昨收 : %s" %(self.TX_price))
        print("\n")
        today_readmode=today_date.strftime('%b %d %Y')
        today_readmode = today_date.strftime('%Y-%m-%d')
        self.TX_use_data=sorted([i for i in index_list if ('NU' in i and 'C.XLS' in i)],reverse=True)[0]
        print("TXN_use_data : ",TWN_list)
        print(today_readmode)
        self.TWN_use_data=[i for i in TWN_list if today_readmode in i][0]
        self.MTW_use_data=sorted([i for i in index_list if 'tw_per' in i],reverse=True)[0]
        print('TX_Use:{} \nTWN_Use:{} \nMTW_Use:{}'.format(self.TX_use_data,self.TWN_use_data,self.MTW_use_data))
        self.MTW_price=pd.read_excel(path+'/'+self.MTW_use_data,header=0,index_col=0).dropna(thresh=3,axis=0).iloc[1][1]
    def Init_constitute_data(self,path): # 抓成分股有啥
        self.加權指數成分股=pd.read_html(path+'/'+self.TX_use_data,encoding='cp950',header=0)[-1]

        self.加權指數成分股.columns=self.加權指數成分股.iloc[0]
        self.加權指數成分股=self.加權指數成分股.iloc[1:]
        self.加權指數成分股['Sector Code']=self.加權指數成分股['Sector Code'].astype(int) #產業類別代碼轉成int
        self.加權指數成分股=self.加權指數成分股.set_index('Local Code') #股票代碼設成index

        self.電子指數成分股=self.加權指數成分股[(self.加權指數成分股['Sector Code']>=24) & (self.加權指數成分股['Sector Code']<=31)]
        self.金融指數成分股=self.加權指數成分股[(self.加權指數成分股['Sector Code']==17)]

        self.非金電指數成分股=self.加權指數成分股[(self.加權指數成分股['Sector Code']<24) | (self.加權指數成分股['Sector Code']>31)]
        self.非金電指數成分股=self.非金電指數成分股[(self.非金電指數成分股['Sector Code']!=17)]

        self.摩根data=pd.read_excel(path+'/'+self.MTW_use_data,header=0,index_col=0).dropna(thresh=10,axis=0)
        self.摩根data.columns=self.摩根data.iloc[0]
        self.摩根data=self.摩根data.iloc[1:]
        self.摩根data['Reuters Code (RIC)']=self.摩根data['Reuters Code (RIC)'].apply(lambda x:x.split('.')[0])
        self.摩根data=self.摩根data.set_index('Reuters Code (RIC)')

        # self.富台指數成分股=pd.read_excel(twn_path+'/'+self.TWN_use_data,header=0,index_col=0, engine='openpyxl')[["股數","價格"]]
        # self.富台指數成分股.index=[i.split(" ")[0] for i in self.富台指數成分股.index]
        # self.富台指數成分股.columns=['Shares',"Close"]
        print(twn_path+'/'+self.TWN_use_data)
        print(pd.read_csv(twn_path+'/'+self.TWN_use_data, header=0, skiprows=2))
        
        #print(pd.read_csv(twn_path+'/'+self.TWN_use_data,header=0,index_col=0, on_bad_lines='skip'))
        self.富台指數成分股=pd.read_csv(twn_path+'/'+self.TWN_use_data, header=0, skiprows=2)[["BBG Ticker","Index Shares","Price"]]
        print(self.富台指數成分股['BBG Ticker'])
        self.富台指數成分股.index=[str(i).split(" ")[0] for i in self.富台指數成分股['BBG Ticker']]
        print(self.富台指數成分股)
        del self.富台指數成分股['BBG Ticker']
        self.富台指數成分股.columns=['Shares',"Close"]
    def calculate_ratio(self,current_date,DQ_stock_dde,Total_dividend_df,全部結算日):

        富台股數list=[]
        台指股數list=[]
        電子股數list=[]
        金融股數list=[]
        摩根股數list=[]
        非金電股數list=[]
        current_date_list = date_range(current_date)
        print(self.加權指數成分股.columns)
        print(self.加權指數成分股)
        台指股數list=sort_data(Total_dividend_df,self.加權指數成分股['Number of Shares in Issue(Unit:1000 Shares)'])
        電子股數list=sort_data(Total_dividend_df,self.電子指數成分股['Number of Shares in Issue(Unit:1000 Shares)'])
        金融股數list=sort_data(Total_dividend_df,self.金融指數成分股['Number of Shares in Issue(Unit:1000 Shares)'])
        非金電股數list=sort_data(Total_dividend_df,self.非金電指數成分股['Number of Shares in Issue(Unit:1000 Shares)'])
        富台股數list=sort_data(Total_dividend_df,self.富台指數成分股['Shares'])
        摩根股數list=sort_data(Total_dividend_df,self.摩根data['Shares FIF Adjusted'])
            

        Total_dividend_df['台指股數']=台指股數list
        Total_dividend_df['電子股數']=電子股數list
        Total_dividend_df['金融股數']=金融股數list
        Total_dividend_df['非金電股數']=非金電股數list
        Total_dividend_df['富台股數']=富台股數list
        Total_dividend_df['摩根股數']=摩根股數list


        加權指數總市值=(self.加權指數成分股['Closing Price'].astype(float)*self.加權指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        電子指數總市值=(self.電子指數成分股['Closing Price'].astype(float)*self.電子指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        金融指數總市值=(self.金融指數成分股['Closing Price'].astype(float)*self.金融指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        非金電指數總市值=(self.非金電指數成分股['Closing Price'].astype(float)*self.非金電指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        富台指數總市值=(self.富台指數成分股.astype(float)['Shares']*self.富台指數成分股.astype(float)['Close']).sum()
        摩根指數總市值=(self.摩根data['Price']*self.摩根data['Shares FIF Adjusted']).sum()



        Total_dividend_df['台指影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['台指股數'])/加權指數總市值*self.TWII_price
        Total_dividend_df['電子影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['電子股數'])/電子指數總市值*self.TE_price
        Total_dividend_df['金融影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['金融股數'])/金融指數總市值*self.TF_price
        Total_dividend_df['非金電影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['非金電股數'])/非金電指數總市值*self.XI_price
        Total_dividend_df['富台影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['富台股數'])/富台指數總市值*self.TWNI_price
        Total_dividend_df['摩根影響點數']=(Total_dividend_df['現金股利(元)']*Total_dividend_df['摩根股數'])/摩根指數總市值*self.MTW_price

        原始ratio=(self.TWN_price/(self.TWNI_price)/(self.TX_price/(self.TWII_price)))*10000-10000

        new_ratio_1_2=((self.TWN_price/(self.TWNI_price-Total_dividend_df['富台影響點數']))/(self.TX_price/(self.TWII_price)))*10000-10000

        new_ratio_2_1=((self.TWN_price/(self.TWNI_price))/(self.TX_price/(self.TWII_price-Total_dividend_df['台指影響點數'])))*10000-10000

        new_ratio_1_1 = ((self.TWN_price/(self.TWNI_price-Total_dividend_df['富台影響點數']))/(self.TX_price/(self.TWII_price-Total_dividend_df['台指影響點數'])))*10000-10000

        影響ratio_台富近月=(new_ratio_1_1-原始ratio)
        影響ratio_台近富遠=(new_ratio_2_1-原始ratio)
        影響ratio_台遠富近=(new_ratio_1_2-原始ratio)


        Total_dividend_df['台富影響Ratio_同合約月']=影響ratio_台富近月
        Total_dividend_df['台富影響Ratio_台近富遠']=影響ratio_台近富遠
        Total_dividend_df['台富影響Ratio_台遠富近']=影響ratio_台遠富近




        
        台指影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['台指股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        電子影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['電子股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        金融影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['金融股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        非金電影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['非金電股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        富台影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['富台股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        摩根影響市值=(Total_dividend_df['現金股利(元)']*Total_dividend_df['摩根股數']).reset_index().groupby('除息日')[0].sum().astype(float)
        print('台指影響市值',台指影響市值)
        print("加權指數總市值",加權指數總市值)
        #台指影響市值.to_csv('台指影響市值_allen.csv')
        台指除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['台指股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        電子除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['電子股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        金融除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['金融股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        非金電除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['非金電股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        富台除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['富台股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        摩根除息家數=(Total_dividend_df['現金股利(元)']*Total_dividend_df['摩根股數']).astype(bool).astype(int).reset_index().groupby('除息日')[0].sum()
        print("台指除息家數",台指除息家數)
        print("香除結果 ",台指影響市值.astype(float)/加權指數總市值*self.TWII_price)
        print("TWII",self.TWII_price)
        
        台指除息點數=pd.Series((台指影響市值.astype(float)/加權指數總市值)*self.TWII_price,index=current_date_list).fillna(0)
        電子除息點數=pd.Series((電子影響市值.astype(float)/電子指數總市值)*self.TE_price,index=current_date_list).fillna(0)
        金融除息點數=pd.Series((金融影響市值.astype(float)/金融指數總市值)*self.TF_price,index=current_date_list).fillna(0)
        非金電除息點數=pd.Series((非金電影響市值.astype(float)/非金電指數總市值)*self.XI_price,index=current_date_list).fillna(0)
        富台除息點數=pd.Series((富台影響市值.astype(float)/富台指數總市值)*self.TWNI_price,index=current_date_list).fillna(0)
        摩根除息點數=pd.Series((摩根影響市值.astype(float)/摩根指數總市值)*self.MTW_price,index=current_date_list).fillna(0)
        print("台指除息點數",台指除息點數)
        #台指除息點數.to_csv('台指除息點數_allen.csv')
        台指除息家數=pd.Series(台指除息家數,index=current_date_list).fillna(0)
        電子除息家數=pd.Series(電子除息家數,index=current_date_list).fillna(0)
        金融除息家數=pd.Series(金融除息家數,index=current_date_list).fillna(0)
        非金電除息家數=pd.Series(非金電除息家數,index=current_date_list).fillna(0)
        富台除息家數=pd.Series(富台除息家數,index=current_date_list).fillna(0)
        摩根除息家數=pd.Series(摩根除息家數,index=current_date_list).fillna(0)




        富台前十大成分股=round(100*((self.富台指數成分股['Shares']*self.富台指數成分股['Close']).sort_values(ascending=False).head(10)/富台指數總市值),2)
        台指前十大成分股=round(100*((self.加權指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)*self.加權指數成分股['Closing Price'].astype(float)).sort_values(ascending=False).head(10)/加權指數總市值),2)



        加權指數總市值=(self.加權指數成分股['Closing Price'].astype(float)*self.加權指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        電子指數總市值=(self.電子指數成分股['Closing Price'].astype(float)*self.電子指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        金融指數總市值=(self.金融指數成分股['Closing Price'].astype(float)*self.金融指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        非金電指數總市值=(self.非金電指數成分股['Closing Price'].astype(float)*self.非金電指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)).sum()
        富台指數總市值=(self.富台指數成分股.astype(float)['Shares']*self.富台指數成分股.astype(float)['Close']).sum()
        摩根指數總市值=(self.摩根data['Price']*self.摩根data['Shares FIF Adjusted']).sum()


        富台權重佔比=round(100*((self.富台指數成分股['Shares']*self.富台指數成分股['Close']).sort_values(ascending=False)/富台指數總市值),2)
        台指權重佔比=round(100*((self.加權指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)*self.加權指數成分股['Closing Price'].astype(float)).sort_values(ascending=False)/加權指數總市值),2)
        電子權重佔比=round(100*((self.電子指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)*self.電子指數成分股['Closing Price'].astype(float)).sort_values(ascending=False)/電子指數總市值),2)
        金融權重佔比=round(100*((self.金融指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)*self.金融指數成分股['Closing Price'].astype(float)).sort_values(ascending=False)/金融指數總市值),2)
        非金電權重佔比=round(100*((self.非金電指數成分股['Number of Shares in Issue(Unit:1000 Shares)'].astype(float)*self.非金電指數成分股['Closing Price'].astype(float)).sort_values(ascending=False)/非金電指數總市值),2)
        摩根權重佔比=round(100*((self.摩根data['Price']*self.摩根data['Shares FIF Adjusted']).astype(float).sort_values(ascending=False)/摩根指數總市值),2)

        future_代碼=pd.read_html('https://www.taifex.com.tw/cht/2/stockLists')[1].set_index('證券代號')['股票期貨、選擇權商品代碼']
        future_代碼=future_代碼.dropna()+'F'
        future_代碼.index=future_代碼.index.astype(int).astype(str)
        future_代碼=future_代碼.sort_index()
        future_代碼=future_代碼[future_代碼.index.duplicated('last')==False]
        future_代碼='W'+future_代碼+'&'

        new_future_index=pd.Series(list(future_代碼.index)).replace('50','0050').replace('56','0056')
        future_代碼.index=new_future_index.astype(str)
        future_代碼=future_代碼[future_代碼.index.duplicated('first')==False]


        future_代碼_list=[]
        for company_future_code in Total_dividend_df['公司'].values:
            try:
                future_代碼_list.append(future_代碼.loc[company_future_code])
            except:
                future_代碼_list.append(np.nan)


        Total_dividend_df['期貨代碼']=future_代碼_list

        stock_last_price_list=[]
        for company in Total_dividend_df['公司']:
            if company in self.加權指數成分股.index:
                stock_last_price_list.append(self.加權指數成分股['Closing Price'].loc[company])
            elif company in self.摩根data.index:
                stock_last_price_list.append(self.摩根data['Price'].loc[company])
            elif company in self.富台指數成分股.index:
                stock_last_price_list.append(self.富台指數成分股['Close'].loc[company])
            else:
                stock_last_price_list.append(DQ_stock_dde.request('{}.129'.format(company)).decode())
        
        #store csv
        self.save_data.save_total_dividend_df_csv(Total_dividend_df,台指權重佔比,電子權重佔比,金融權重佔比,非金電權重佔比,富台權重佔比,摩根權重佔比,future_代碼)
        self.save_data.save_total_table_csv(台指除息點數,電子除息點數,金融除息點數,非金電除息點數,台指除息家數,電子除息家數,金融除息家數,非金電除息家數,富台除息點數,摩根除息點數,富台除息家數,摩根除息家數,全部結算日)
def Create_TX_closeday_func(current_date):
    # 設定結算日，台指期貨的結算日為每季的第三個週三
    settlement_day = 2  # 星期三為2
    start_date = datetime(current_date.year, current_date.month, current_date.day)
    end_date = start_date.replace(year=start_date.year + 1)
    # 儲存結果的list
    settlement_dates = []

    # 迴圈從起始日期到結束日期，每次加一個月
    date = start_date
    while date <= end_date:
        # 找出當月第一天是星期幾
        first_day = datetime(date.year, date.month, 1)
        weekday_of_first_day = first_day.weekday()
        # 計算第三個週三的日期
        if weekday_of_first_day <= settlement_day:
            settlement_date = first_day + timedelta(days=settlement_day-weekday_of_first_day, weeks=2)
        else:
            settlement_date = first_day + timedelta(days=settlement_day-weekday_of_first_day, weeks=3)
        # 如果結算日在當月，就將日期加入list
        if settlement_date.year == date.year and settlement_date.month == date.month:
            settlement_dates.append(settlement_date.date())
        # 加一個月
        if date.month == 12:
            date = datetime(date.year+1, 1, 1)
        else:
            date = datetime(date.year, date.month+1, 1)

    # 將結果轉換成Pandas DataFrame
    台指結算日 = pd.DataFrame(index = settlement_dates)
    台指結算日.index.name='結算日'
    return 台指結算日
def Create_foreign_closeday_func(current_date):
    tw_calendar = get_calendar('XSES')
    start_date = datetime(current_date.year, current_date.month, current_date.day)
    end_date = start_date.replace(year=start_date.year + 1,month= current_date.month-1)
    dates = tw_calendar.valid_days(start_date, end_date)
    monthly_second_last_trading_days = []

    for month in pd.unique(dates.month):
        month_dates = dates[dates.month == month]
        second_last_trading_day = month_dates[-2]
        monthly_second_last_trading_days.append(second_last_trading_day.date())
    海外結算日=pd.DataFrame(index=monthly_second_last_trading_days)
    海外結算日.index.name='結算日'
    return 海外結算日
def connect_dde():
    print("連接DQ2報價 如超過10秒無連線完成文字即表示DQ2報價系統連線異常 請點Request_dq2.bat 並重新執行此檔案")
    DQ_stock_dde=PyWinDDE.DDEClient("DQII", "TWSE")
    DQ_index_dde=PyWinDDE.DDEClient("DQII", "INDX")
    DQ_future_dde=PyWinDDE.DDEClient("DQII", "FUSA")
    print("連線完成!")
    return DQ_stock_dde,DQ_index_dde,DQ_future_dde
# def third_wen(date_str):
#     date = datetime.strptime(date_str[:10], "%Y-%m-%d")
#     day = (21 - (date.weekday() + 4) % 7) % 7
#     return (date + timedelta(days=day)).date()
# def TXF_contract_month(date_str):
#     close_day = third_wen(date_str)
#     date = datetime.strptime(date_str, "%Y-%m-%d").date()
#     next_month = (date + timedelta(days=32)).replace(day=1)
#     if date <= close_day:  # 開盤前除息 仍算在該月合約
#         return date_str[:7] + '合約'
#     return next_month.strftime("%Y-%m") + '合約'
def third_wen(date_str):
    y = int(date_str[0:4])
    m = int(date_str[5:7])
    day=21-(date(y,m,1).weekday()+4)%7  
    return date(y,m,day)  

def TXF_contract_month(date_str):
    close_day = third_wen(date_str)
    y = int(date_str[0:4])
    m = int(date_str[5:7])
    if pd.to_datetime(date_str) <= pd.to_datetime(close_day) : # 開盤前除息 仍算在該月合約
        return date_str[0:7]+'合約'
    if pd.to_datetime(date_str) > pd.to_datetime(close_day) : 
        if m == 12 :
            return str(y+1)+'-01合約'
        else :
            return str(y)+'-0'+str(m+1)+'合約'
def TWN_contract_month(date_str) :
    TWN_ex_list= ['2022-01-25','2022-02-24','2022-03-30','2022-04-28','2022-05-30','2022-06-29','2022-07-28','2022-08-30','2022-09-29','2022-10-28','2022-11-29','2022-12-29'
        ,'2023-01-30','2023-02-23','2023-03-30','2023-04-27','2023-05-30','2023-06-29','2023-07-28','2023-08-30','2023-09-28','2023-10-30','2023-11-29','2023-12-28'] # TWN 期貨到期日
    y = int(date_str[0:4])
    m = int(date_str[5:7])
    yyyymm = date_str[0:7]
    結算日 = pd.to_datetime([x for x in TWN_ex_list if x.startswith(yyyymm)== True][0])
    if pd.to_datetime(date_str) <= 結算日 : # 開盤前除息 仍算在該月合約
        return date_str[0:7]+'合約'
    if pd.to_datetime(date_str) > pd.to_datetime(結算日) : 
        if m == 12 :
            return str(y+1)+'-01合約'
        else :
            return str(y)+'-0'+str(m+1)+'合約'
def date_range(current_date):
    start_date = datetime(current_date.year, current_date.month, current_date.day)
    end_date = start_date.replace(year=start_date.year + 1)
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.date())
        current += timedelta(days=1)
    return dates
def sort_data(div_data,shares_data):
    use_list=[]
    shares_data=shares_data.astype(float)
    shares_data.index=shares_data.index.astype(str)
    print(shares_data)
    for i in div_data['公司']:
        try:
            use_list.append(shares_data.loc[str(i)])
        except:
            use_list.append(0)
            pass
    return use_list
def main(current_date,config):
    while True :
        try :
            DQ_stock_dde,DQ_index_dde,DQ_future_dde = connect_dde()
            台指結算日 = Create_TX_closeday_func(current_date) 
            海外結算日 = Create_foreign_closeday_func(current_date)
            全部結算日 = {"台指結算日" : 台指結算日, "海外結算日" : 海外結算日}
            TEJ = TEJ_data(config,current_date)
            Calculate_Data = Calculate_constitute_date(DQ_stock_dde,DQ_index_dde,DQ_future_dde,current_date,config)
            predict_exist_company,predict_exist_calculate_day,去年除息日期,今年預估除息點數,確認除息_use_data = TEJ.tej_data()
            Total_dividend_df,predict_date_df,前30大預估權值股 = TEJ.calculate_dividend_date(predict_exist_company,predict_exist_calculate_day,去年除息日期,今年預估除息點數,確認除息_use_data)
            
            Calculate_Data.calculate_ratio(current_date,DQ_stock_dde,Total_dividend_df,全部結算日)
            Calculate_Data.save_data.save_top30_data_csv(predict_date_df,前30大預估權值股)
            print("程式執行完成 請按任意鍵結束程式")
            break
        except Exception as e:
            print(e)
            print("程式異常")
            t.sleep(10)
            continue   
    os.system("pause")
if __name__ == "__main__":
    config = Config()
    current_date = datetime.now().today()
    #current_date = datetime(2023,7,10)
    main(current_date,config)