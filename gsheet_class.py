import pandas as pd
import numpy as np
import pygsheets
from pandas.tseries.offsets import BDay
from datetime import datetime, timedelta,time,date
import PyWinDDE
import requests
import os
import time as sleep_time
def line_notify(token):
    # 我的:COboFPxISDbIGKH4VkTZ7VN0bUAIR6Yp9Zq1faDVPEp
    # 公司:nc15Po2hiGmEMNBYnMP2FBf2lCgf0FsKhq4UJOXt4cd
        message = "除權息 Google Sheet 更新成功!"

        line_url = "https://notify-api.line.me/api/notify"
        line_header = {
            "Authorization": 'Bearer ' + token,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        line_data = {
            "message": message
        }

        requests.post(url=line_url, headers=line_header, data=line_data)
def bday_counter(date,n) :
    
    tw_holiday_list = ["2023-04-03","2023-04-04","2023-04-05","2023-05-01","2023-06-23","2023-06-22","2023-09-29","2023-10-09","2023-10-10"] #包含補班日的假日
    date = str(date)[0:10]
    yesterday = str(pd.to_datetime(date)-BDay(n))[0:10] #計算上一個的工作日
    while yesterday in tw_holiday_list :
        yesterday = str(pd.to_datetime(yesterday)-BDay(n))[0:10]
    else :
        return yesterday           
def third_wen(date_str):
    y = int(date_str[0:4])
    m = int(date_str[5:7])
    day=21-(date(y,m,1).weekday()+4)%7  
    return date(y,m,day)  


def TWN_contract_month(date_str) :
    TWN_ex_list= ['2022-01-25','2022-02-24','2022-03-30','2022-04-28','2022-05-30','2022-06-29','2022-07-28','2022-08-30','2022-09-29','2022-10-28','2022-11-29','2022-12-29'
        ,'2023-01-30','2023-02-23','2023-03-30','2023-04-27','2023-05-30','2023-06-29','2023-07-28','2023-08-30','2023-09-28','2023-10-30','2023-11-29','2023-12-28']
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

class Google_Sheets_Post :
    富台合約_list = []
    台指合約_list = []
    today = str((datetime.today().date()))
    yesterday = bday_counter(today[0:10],1)
    path = '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+today+"/"
    path_y =  '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+yesterday+"/"
    gc = pygsheets.authorize(service_file='dividend_gspread.json')
    survey_url = 'https://docs.google.com/spreadsheets/d/1GksJ5z94MOzLNcIEct_x_aWoWzBi4CEiHJUHjU7l0IQ/edit#gid=0' # Allen版 台指/富台除息預估表
    sh = gc.open_by_url(survey_url)
    台指今日預估公司列表 = 台指昨日預估公司列表 = 富台今日預估公司列表 = 富台昨日預估公司列表 = []
    台指今日減少預估或確認除息_df = 富台今日減少預估或確認除息_df = 台指遠月今日減少預估或確認除息_df = 富台遠月今日減少預估或確認除息_df = None
    def __init__(self,config) -> None:
        self.config = config
        self.今日除息總表 = pd.read_csv(self.path+"預估除息總表_"+self.today+".csv")
        self.昨日除息總表 = pd.read_csv(self.path_y+"預估除息總表_"+self.yesterday+".csv")
        self.權指股前五筆開會 = pd.read_excel(self.path+"前三十大權值股近五筆開會資訊_"+self.today+".xlsx",header=1).drop(['Unnamed: 0'],axis = 1)
        self.前三十大權值股預估 = pd.read_excel(self.path+"前30大預估權值股_"+self.today+".xlsx",header=1).drop(['Unnamed: 0'],axis = 1)
        self.台指合約月份 = TXF_contract_month(self.today)
        self.富台合約月份 = TWN_contract_month(self.today)
        self.今日預估公司 = pd.read_csv(self.path+"全除息個股序列總表_"+self.today+".csv")
        self.昨日預估公司 = pd.read_csv(self.path_y+"全除息個股序列總表_"+self.yesterday+".csv")
        self.合約list()
        self.台指遠月合約月份 = self.台指合約_list[self.台指合約_list.index(self.台指合約月份) + 1 ] #拿到目前合約月份的遠月合約
        self.富台遠月合約月份 = self.富台合約_list[self.富台合約_list.index(self.富台合約月份) + 1 ] #拿到目前富台合約月份的遠月合約
        self.除息總表差別 = (self.今日除息總表.set_index(self.today).T - self.昨日除息總表.set_index(self.yesterday).T).dropna(axis = 1).T

    def 合約list(self):
        for i in ['2023-01-30','2023-02-23','2023-03-30','2023-04-27','2023-05-30','2023-06-29','2023-07-28','2023-08-30','2023-09-28','2023-10-30','2023-11-29','2023-12-28'] :
            self.富台合約_list.append(TWN_contract_month(i))
            self.台指合約_list.append(TXF_contract_month(i))
        print("台指合約list : ",self.台指合約_list) 
        print("富台合約list : ",self.富台合約_list)
    def 近五筆會議狀況(self):
        ws = self.sh.worksheet_by_title('近五筆會議狀況')
        ws.clear()
        ws.update_value('A1:J1', "台指/富台 前30大權指股 近五筆會議狀況 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws.set_dataframe(self.權指股前五筆開會.replace("nan",""), 'A2', copy_index=False, nan='')
        print("近五筆會議狀況 worksheet done")
    def 預估權值股除息列表(self):
        ws_1 = self.sh.worksheet_by_title('預估權值股除息列表')
        ws_1.clear()
        ws_1.update_value('A1:J1', "台指/富台 前30大權指股 預估除息日 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_1.set_dataframe(self.前三十大權值股預估.replace("nan",""), 'A2', copy_index=False, nan='')
        self.台指今日預估公司列表 = [x for x in self.今日預估公司[(self.今日預估公司['台指合約月份']== self.台指合約月份) & (self.今日預估公司['台指影響點數'] > 0)].公司]
        self.台指昨日預估公司列表 = [x for x in self.昨日預估公司[(self.昨日預估公司['台指合約月份']== self.台指合約月份) & (self.昨日預估公司['台指影響點數'] > 0)].公司]

        self.富台今日預估公司列表 = [x for x in self.今日預估公司[(self.今日預估公司['富台合約月份']== self.富台合約月份) & (self.今日預估公司['富台影響點數'] > 0)].公司]
        self.富台昨日預估公司列表 = [x for x in self.昨日預估公司[(self.昨日預估公司['富台合約月份']== self.富台合約月份) & (self.昨日預估公司['富台影響點數'] > 0)].公司]

        台指今日新增預估或確認除息 = set(self.台指今日預估公司列表)-set(self.台指昨日預估公司列表)
        富台今日新增預估或確認除息 = set(self.富台今日預估公司列表)-set(self.富台昨日預估公司列表)

        台指今日新增預估或確認除息_df = self.今日預估公司[(self.今日預估公司['公司'].isin(台指今日新增預估或確認除息)) & (self.今日預估公司['台指合約月份']== self.台指合約月份)][['除息日','公司','確認/預估','台指影響點數']]
        富台今日新增預估或確認除息_df = self.今日預估公司[(self.今日預估公司['公司'].isin(富台今日新增預估或確認除息)) & (self.今日預估公司['富台合約月份']== self.富台合約月份)][['除息日','公司','確認/預估','富台影響點數']]


        


        台指遠月今日預估公司列表 = [x for x in self.今日預估公司[(self.今日預估公司['台指合約月份']== self.台指遠月合約月份) & (self.今日預估公司['台指影響點數'] > 0)].公司]
        台指遠月昨日預估公司列表 = [x for x in self.昨日預估公司[(self.昨日預估公司['台指合約月份']== self.台指遠月合約月份) & (self.昨日預估公司['台指影響點數'] > 0)].公司]

        富台遠月今日預估公司列表 = [x for x in self.今日預估公司[(self.今日預估公司['富台合約月份']== self.富台遠月合約月份) & (self.今日預估公司['富台影響點數'] > 0)].公司]
        富台遠月昨日預估公司列表 = [x for x in self.昨日預估公司[(self.昨日預估公司['富台合約月份']== self.富台遠月合約月份) & (self.昨日預估公司['富台影響點數'] > 0)].公司]

        台指遠月今日新增預估或確認除息 = set(台指遠月今日預估公司列表)-set(台指遠月昨日預估公司列表) #遠月今日預估公司列表 - 遠月昨日預估公司列表
        富台遠月今日新增預估或確認除息 = set(富台遠月今日預估公司列表)-set(富台遠月昨日預估公司列表)

        台指遠月今日新增預估或確認除息_df = self.今日預估公司[(self.今日預估公司['公司'].isin(台指遠月今日新增預估或確認除息)) & (self.今日預估公司['台指合約月份']== self.台指遠月合約月份)][['除息日','公司','確認/預估','台指影響點數']]
        富台遠月今日新增預估或確認除息_df = self.今日預估公司[(self.今日預估公司['公司'].isin(富台遠月今日新增預估或確認除息)) & (self.今日預估公司['富台合約月份']== self.富台遠月合約月份)][['除息日','公司','確認/預估','富台影響點數']]

        台指今日除息公司 = self.今日預估公司[self.今日預估公司['除息日'].apply(lambda x :pd.to_datetime(x)) == pd.to_datetime(self.today)]
        富台今日除息公司 = self.今日預估公司[self.今日預估公司['除息日'].apply(lambda x :pd.to_datetime(x)) == pd.to_datetime(self.today)]
        台指今日除息公司 = 台指今日除息公司[台指今日除息公司['台指影響點數']>0][['除息日','公司','確認/預估','台指影響點數']]
        富台今日除息公司 = 富台今日除息公司[富台今日除息公司['富台影響點數']>0][['除息日','公司','確認/預估','富台影響點數']]

        print("預估權值股除息列表 worksheet done")
        self.今日新增預估公司(台指今日新增預估或確認除息_df,富台今日新增預估或確認除息_df,台指遠月今日新增預估或確認除息_df,富台遠月今日新增預估或確認除息_df,台指今日除息公司,富台今日除息公司,台指遠月今日預估公司列表,台指遠月昨日預估公司列表,富台遠月今日預估公司列表,富台遠月昨日預估公司列表)
    def 今日新增預估公司(self,台指今日新增預估或確認除息_df,富台今日新增預估或確認除息_df,台指遠月今日新增預估或確認除息_df,富台遠月今日新增預估或確認除息_df,台指今日除息公司,富台今日除息公司,台指遠月今日預估公司列表,台指遠月昨日預估公司列表,富台遠月今日預估公司列表,富台遠月昨日預估公司列表):
        ws_2 = self.sh.worksheet_by_title('今日新增預估公司')
        ws_2.clear()
        ws_2.update_value('A1:I1', "台指/富台 今天近月新增 確定/預估除息日 之公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('A2:D2', "目前台指近月合約月份 : %s " %(self.台指合約月份))
        ws_2.update_value('F2:I2', "目前富台近月合約月份 : %s " %(self.富台合約月份))
        ws_2.set_dataframe(台指今日新增預估或確認除息_df.replace("nan",""), 'A3', copy_index=False, nan='')
        ws_2.set_dataframe(富台今日新增預估或確認除息_df.replace("nan",""), 'F3', copy_index=False, nan='')
        ws_2.update_value('K1:S1', "台指/富台 今天遠月新增 確定/預估除息日 之公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('K2:N2', "目前台指遠月合約月份 : %s " %(self.台指遠月合約月份))
        ws_2.update_value('P2:S2', "目前富台遠月合約月份 : %s " %(self.富台遠月合約月份))
        ws_2.set_dataframe(台指遠月今日新增預估或確認除息_df.replace("nan",""), 'K3', copy_index=False, nan='')
        ws_2.set_dataframe(富台遠月今日新增預估或確認除息_df.replace("nan",""), 'P3', copy_index=False, nan='')
        ws_2.update_value('U1:AA1', " %s 預估除息總表 & 與前日差額 更新時間 : %s " %(self.today,str(datetime.today())[0:19]))
        ws_2.update_value('U5', " 差額 " )
        ws_2.update_value('U4:AA4', " %s 與 %s 預估除息總表差額 " %(self.today,self.yesterday))
        ws_2.update_value('U12:AA12', " %s 預估除息總表 " %(self.today))
        ws_2.update_value('U20:AA20', " %s 預估除息總表 " %(self.yesterday))
        ws_2.set_dataframe(self.除息總表差別.replace("nan",""), 'U5', copy_index=True, nan='')
        ws_2.set_dataframe(self.今日除息總表.replace("nan",""), 'U13', copy_index=False, nan='')
        ws_2.set_dataframe(self.昨日除息總表.replace("nan",""), 'U21', copy_index=False, nan='')
        ws_2.update_value('U5', " 差額 " )
        ws_2.update_value('E2', "台指總影響點數")
        ws_2.update_value('E3', "=sum(D4:D500)")
        ws_2.update_value('J2', "富台總影響點數")
        ws_2.update_value('J3', "=sum(I4:I500)") 
        ws_2.update_value('O2', "台指總影響點數")
        ws_2.update_value('O3', "=sum(N4:N500)")
        ws_2.update_value('T2', "富台總影響點數")
        ws_2.update_value('T3', "=sum(S4:S500)")
        ws_2.update_value('AC1:AK1', "台指/富台 今天除息公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('AC2:AF2', "目前台指近月合約月份 : %s " %(self.台指合約月份))
        ws_2.update_value('AH2:AK2', "目前富台近月合約月份 : %s " %(self.富台合約月份))
        ws_2.set_dataframe(台指今日除息公司.replace("nan",""), 'AC3', copy_index=False, nan='')
        ws_2.set_dataframe(富台今日除息公司.replace("nan",""), 'AH3', copy_index=False, nan='')
        ws_2.update_value('AG2', "台指總影響點數")
        ws_2.update_value('AG3', "=sum(AF4:AF500)")
        ws_2.update_value('AL2', "富台總影響點數")
        ws_2.update_value('AL3', "=sum(AK4:AK500)") 
        #前30大權值股_int = [2330,2317,2454,2412,6505,2308,2881,2303,2882,1303,1301,2002,3711,2886,2891,1326,1216,5880,5871,2884,2892,3045,2603,2207,2382,2880,3008,2885,2912,1101,2395,3034,6415,5876,2609]
        前30大權值股_str = self.config.TOP_30_RATIO_STOCK
        前30大公司預估_df = self.今日預估公司[(self.今日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(self.today)) & self.今日預估公司.公司.isin(前30大權值股_str)]
        除息日_dic = {}
        台指影響點數_dic = {}
        富台影響點數_dic = {}
        台富影響Ratio_同合約月_dic = {}
        台富影響Ratio_台近富遠_dic = {}
        台富影響Ratio_台遠富近_dic = {}
        for company in 前30大公司預估_df["公司"].unique() :
            除息日_dic[company] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].除息日.values[0]
            台指影響點數_dic[(除息日_dic[company],company)] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].台指影響點數.values[0]
            富台影響點數_dic[(除息日_dic[company],company)] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].富台影響點數.values[0]
            台富影響Ratio_同合約月_dic[(除息日_dic[company],company)] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].台富影響Ratio_同合約月.values[0]
            台富影響Ratio_台近富遠_dic[(除息日_dic[company],company)] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].台富影響Ratio_台近富遠.values[0]
            台富影響Ratio_台遠富近_dic[(除息日_dic[company],company)] = 前30大公司預估_df[前30大公司預估_df["公司"] == company].台富影響Ratio_台遠富近.values[0]
        ws_3 = self.sh.worksheet_by_title('預估除權息日期_依照市值排序')
        df = ws_3.get_as_df()
        df['今年預估/確認除息日'] = df['股票代碼'].apply(lambda x : str(x)).map(除息日_dic).fillna("")
        df['今年台指合約月份'] = df['今年預估/確認除息日'].apply(lambda x : TXF_contract_month(x) if x != '' else "")
        df['今年富台合約月份'] = df['今年預估/確認除息日'].apply(lambda x : TWN_contract_month(x) if x != '' else "")
        df['台指影響點數'] = df.apply(lambda row: 台指影響點數_dic.get((row['今年預估/確認除息日'], str(row['股票代碼'])), None), axis=1)
        df['富台影響點數'] = df.apply(lambda row: 富台影響點數_dic.get((row['今年預估/確認除息日'], str(row['股票代碼'])), None), axis=1)
        df['台富影響Ratio 同合約月'] = df.apply(lambda row: 台富影響Ratio_同合約月_dic.get((row['今年預估/確認除息日'], str(row['股票代碼'])), None), axis=1)
        df['台富影響Ratio 台近富遠'] = df.apply(lambda row: 台富影響Ratio_台近富遠_dic.get((row['今年預估/確認除息日'], str(row['股票代碼'])), None), axis=1)
        df['台富影響Ratio 台遠富近'] = df.apply(lambda row: 台富影響Ratio_台遠富近_dic.get((row['今年預估/確認除息日'], str(row['股票代碼'])), None), axis=1)



        ws_3.set_dataframe(pd.DataFrame(df['今年預估/確認除息日'].replace("nan","")), 'D1:D56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['今年台指合約月份'].replace("nan","")), 'L1:L56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['今年富台合約月份'].replace("nan","")), 'M1:M56', copy_index=False, nan='')

        ws_3.set_dataframe(pd.DataFrame(df['台指影響點數'].replace("nan","")), 'O1:O56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['富台影響點數'].replace("nan","")), 'P1:P56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['台富影響Ratio 同合約月'].apply(lambda x : np.around(x,4)).replace("nan","")), 'Q1:Q56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['台富影響Ratio 台近富遠'].apply(lambda x : np.around(x,4)).replace("nan","")), 'R1:R56', copy_index=False, nan='')
        ws_3.set_dataframe(pd.DataFrame(df['台富影響Ratio 台遠富近'].apply(lambda x : np.around(x,4)).replace("nan","")), 'S1:S56', copy_index=False, nan='')


        
        台指今日減少預估或確認除息 = set(self.台指昨日預估公司列表)-set(self.台指今日預估公司列表)
        富台今日減少預估或確認除息 = set(self.富台昨日預估公司列表)-set(self.富台今日預估公司列表)

        self.台指今日減少預估或確認除息_df = self.昨日預估公司[(self.昨日預估公司['公司'].isin(台指今日減少預估或確認除息)) & (self.昨日預估公司['台指合約月份']== self.台指合約月份)][['除息日','公司','確認/預估','台指影響點數']]
        self.富台今日減少預估或確認除息_df = self.昨日預估公司[(self.昨日預估公司['公司'].isin(富台今日減少預估或確認除息)) & (self.昨日預估公司['富台合約月份']== self.富台合約月份)][['除息日','公司','確認/預估','富台影響點數']]

        台指遠月今日減少預估或確認除息 = set(台指遠月昨日預估公司列表)-set(台指遠月今日預估公司列表)
        富台遠月今日減少預估或確認除息 = set(富台遠月昨日預估公司列表)-set(富台遠月今日預估公司列表)

        self.台指遠月今日減少預估或確認除息_df = self.昨日預估公司[(self.昨日預估公司['公司'].isin(台指遠月今日減少預估或確認除息)) & (self.昨日預估公司['台指合約月份']== self.台指遠月合約月份)][['除息日','公司','確認/預估','台指影響點數']]
        self.富台遠月今日減少預估或確認除息_df = self.昨日預估公司[(self.昨日預估公司['公司'].isin(富台遠月今日減少預估或確認除息)) & (self.昨日預估公司['富台合約月份']== self.富台遠月合約月份)][['除息日','公司','確認/預估','富台影響點數']]
    def 今日減少公司(self):
        print("今日減少預估公司 worksheet")
        ws_2 = self.sh.worksheet_by_title('今日減少公司')
        ws_2.clear()
        ws_2.update_value('A1:I1', "台指/富台 今天近月減少 確定/預估除息日 之公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('A2:D2', "目前台指近月合約月份 : %s " %(self.台指合約月份))
        ws_2.update_value('F2:I2', "目前富台近月合約月份 : %s " %(self.富台合約月份))
        ws_2.set_dataframe(self.台指今日減少預估或確認除息_df.replace("nan",""), 'A3', copy_index=False, nan='')
        ws_2.set_dataframe(self.富台今日減少預估或確認除息_df.replace("nan",""), 'F3', copy_index=False, nan='')
        ws_2.update_value('K1:S1', "台指/富台 今天遠月減少 確定/預估除息日 之公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('K2:N2', "目前台指遠月合約月份 : %s " %(self.台指遠月合約月份))
        ws_2.update_value('P2:S2', "目前富台遠月合約月份 : %s " %(self.富台遠月合約月份))
        ws_2.set_dataframe(self.台指遠月今日減少預估或確認除息_df.replace("nan",""), 'K3', copy_index=False, nan='')
        ws_2.set_dataframe(self.富台遠月今日減少預估或確認除息_df.replace("nan",""), 'P3', copy_index=False, nan='')
        ws_2.update_value('U1:AA1', " %s 預估除息總表 & 與前日差額 更新時間 : %s " %(self.today,str(datetime.today())[0:19]))
        ws_2.update_value('U5', " 差額 " )
        ws_2.update_value('U4:AA4', " %s 與 %s 預估除息總表差額 " %(self.today,self.yesterday))
        ws_2.update_value('U12:AA12', " %s 預估除息總表 " %(self.today))
        ws_2.update_value('U20:AA20', " %s 預估除息總表 " %(self.yesterday))
        ws_2.set_dataframe(self.除息總表差別.replace("nan",""), 'U5', copy_index=True, nan='')
        ws_2.set_dataframe(self.今日除息總表.replace("nan",""), 'U13', copy_index=False, nan='')
        ws_2.set_dataframe(self.昨日除息總表.replace("nan",""), 'U21', copy_index=False, nan='')
        ws_2.update_value('U5', " 差額 " )
        ws_2.update_value('E2', "台指總影響點數")
        ws_2.update_value('E3', "=sum(D4:D500)")
        ws_2.update_value('J2', "富台總影響點數")
        ws_2.update_value('J3', "=sum(I4:I500)") 
        ws_2.update_value('O2', "台指總影響點數")
        ws_2.update_value('O3', "=sum(N4:N500)")
        ws_2.update_value('T2', "富台總影響點數")
        ws_2.update_value('T3', "=sum(S4:S500)")
        print("今日減少預估公司 worksheet done")
    def 今日新增Ratio影響公司(self):
        print("今日新增ratio影響公司 worksheet")
        con1_t = self.今日預估公司['台富影響Ratio_同合約月'] != 0
        con2_t = self.今日預估公司['台富影響Ratio_台近富遠'] != 0
        con3_t = self.今日預估公司['台富影響Ratio_台遠富近'] != 0

        con4_t = self.今日預估公司['台指合約月份'] == self.台指合約月份
        con5_t = self.今日預估公司['台指合約月份'] == self.台指遠月合約月份
        con6_t = self.今日預估公司['富台合約月份'] == self.富台合約月份
        con7_t = self.今日預估公司['富台合約月份'] == self.富台遠月合約月份

        今日ratio影響公司 = self.今日預估公司[(self.今日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(self.today)) & (con1_t | con2_t | con3_t)]
        #今日ratio影響公司 = 今日ratio影響公司[(con4_t|con5_t|con6_t|con7_t)]
        今日ratio影響公司 = 今日ratio影響公司.loc[con4_t | con5_t | con6_t | con7_t]

        con1_y = self.昨日預估公司['台富影響Ratio_同合約月'] != 0
        con2_y = self.昨日預估公司['台富影響Ratio_台近富遠'] != 0
        con3_y = self.昨日預估公司['台富影響Ratio_台遠富近'] != 0

        con4_y = self.昨日預估公司['台指合約月份'] == self.台指合約月份
        con5_y = self.昨日預估公司['台指合約月份'] == self.台指遠月合約月份
        con6_y = self.昨日預估公司['富台合約月份'] == self.富台合約月份
        con7_y = self.昨日預估公司['富台合約月份'] == self.富台遠月合約月份

        昨日ratio影響公司 = self.昨日預估公司[(self.昨日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(self.yesterday)) & (con1_y | con2_y | con3_y)]
        #昨日ratio影響公司 = 昨日ratio影響公司[(con4_y|con5_y|con6_y|con7_y)]
        昨日ratio影響公司 = 昨日ratio影響公司.loc[con4_y | con5_y | con6_y | con7_y]


        今日ratio影響公司列表 = [x for x in 今日ratio影響公司.公司]
        昨日ratio影響公司列表 = [x for x in 昨日ratio影響公司.公司]

        ratio影響新增公司列表 = set(今日ratio影響公司列表)-set(昨日ratio影響公司列表)
        ratio影響新增公司_df = 今日ratio影響公司[今日ratio影響公司['公司'].isin(ratio影響新增公司列表)][['除息日','公司','確認/預估','台指合約月份','富台合約月份','台指影響點數','富台影響點數',
                                                                            '台富影響Ratio_同合約月','台富影響Ratio_台近富遠','台富影響Ratio_台遠富近']]

        ratio影響新增公司_df['台指影響點數'] = ratio影響新增公司_df['台指影響點數'].apply(lambda x : np.around(x,4))
        ratio影響新增公司_df['富台影響點數'] = ratio影響新增公司_df['富台影響點數'].apply(lambda x : np.around(x,4))
        ratio影響新增公司_df['台富影響Ratio_同合約月'] = ratio影響新增公司_df['台富影響Ratio_同合約月'].apply(lambda x : np.around(x,4))
        ratio影響新增公司_df['台富影響Ratio_台近富遠'] = ratio影響新增公司_df['台富影響Ratio_台近富遠'].apply(lambda x : np.around(x,4))
        ratio影響新增公司_df['台富影響Ratio_台遠富近'] = ratio影響新增公司_df['台富影響Ratio_台遠富近'].apply(lambda x : np.around(x,4))

        ws_6 = self.sh.worksheet_by_title('每日新增Ratio影響')

        ws_6.clear()

        ws_6.update_value('A1:J1', "台指/富台 今天新增 近月&遠月 影響ratio公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_6.set_dataframe(ratio影響新增公司_df.replace("nan",""), 'A2', copy_index=False, nan='')

        print("今日新增ratio影響公司 worksheet done")
    
    def post_to_line(self):
        now = datetime.now().time() 
        target_time_am = time(hour=8, minute=45)
        if now < target_time_am :
            token_company = "pD9ODB7gtrmYiM4So3UmCoaPigaCE8UpzXUsG8Mdh7k"
            line_notify(token_company)
            
        print("指數權重")
    def 每日台指富台前十大產業權重(self):
        path='//192.168.60.83/Wellington/Allen/Index'
        twn_path='//192.168.60.83/Wellington/Allen/Index/BloombergTWNI/'
        twn_path = '//192.168.60.81/Wellington/Faker/富台權重/'

        index_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index')
        TWN_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index\BloombergTWNI')
        TWN_list = os.listdir(r'\\192.168.60.81\Wellington\Faker\富台權重')

        今日資料日期=((datetime.today())).date()
        # 今日資料日期 = datetime.date(2023,3,30)
        today_readmode = 今日資料日期.strftime('%Y-%m-%d')
        TX_use_data = sorted([i for i in index_list if ('NU' in i and 'C.XLS' in i)],reverse=True)[0]
        TWN_use_data = [i for i in TWN_list if today_readmode in i][0]
        MTW_use_data = sorted([i for i in index_list if 'tw_per' in i],reverse=True)[0]

        print('TX_Use:{} \nTWN_Use:{} \nMTW_Use:{}'.format(TX_use_data,TWN_use_data,MTW_use_data))

        MTW=pd.read_excel(path+'/'+MTW_use_data,header=0,index_col=0).dropna(thresh=3,axis=0).iloc[1][1]

        加權指數成分股=pd.read_html(path+'/'+TX_use_data,encoding='cp950',header=0)[-1]
        加權指數成分股.columns=加權指數成分股.iloc[0]
        加權指數成分股=加權指數成分股.iloc[1:]
        加權指數成分股['Sector Code']=加權指數成分股['Sector Code'].astype(int)
        加權指數成分股=加權指數成分股.set_index('Local Code')

        # 富台指數成分股=pd.read_excel(twn_path+'/'+TWN_use_data,header=0,index_col=0)[["股數","價格"]]
        # 富台指數成分股.index=[i.split(" ")[0] for i in 富台指數成分股.index]
        # 富台指數成分股.columns=['Shares',"Close"]
        富台指數成分股=pd.read_csv(twn_path+'/'+TWN_use_data, header=0, skiprows=2)[["BBG Ticker","Index Shares","Price"]]
        富台指數成分股.index=[str(i).split(" ")[0] for i in 富台指數成分股['BBG Ticker']]
        del 富台指數成分股['BBG Ticker']
        富台指數成分股.columns=['Shares',"Close"]
        # 富台指數成分股=pd.read_csv(twn_path+'/'+TWN_use_data, header=0, skiprows=2)[["BBG Ticker","Index Shares","Price"]]
        # print(富台指數成分股['BBG Ticker'])
        # 富台指數成分股.index=[str(i).split(" ")[0] for i in 富台指數成分股['BBG Ticker']]
        # print(富台指數成分股)
        # 富台指數成分股['BBG Ticker']
        # 富台指數成分股.columns=['Shares',"Close"]
        industry = pd.read_excel("./個股產業別.xlsx")

        TX總市值 = 加權指數成分股['Market Cap. (Unit: NT$Thousands)'].apply(lambda x : float(x)).sum()
        加權指數成分股['weight_%'] = 加權指數成分股['Market Cap. (Unit: NT$Thousands)'].apply(lambda x : float(x))/TX總市值
        富台指數成分股['weight_%'] = ((富台指數成分股.Shares)*(富台指數成分股.Close))/((富台指數成分股.Shares)*(富台指數成分股.Close)).sum()

        加權指數成分股['Code'] = 加權指數成分股.index
        富台指數成分股['Code'] = 富台指數成分股.index

        industry['Code'] = industry['Code'].apply(lambda x :str(x))

        加權指數成分股 = 加權指數成分股.reset_index(drop = True)
        富台指數成分股 = 富台指數成分股.reset_index(drop = True)

        加權指數成分股['Code'] = 加權指數成分股['Code'].apply(lambda x :str(x))
        富台指數成分股['Code'] = 富台指數成分股['Code'].apply(lambda x :str(x))

        加權指數成分股 = 加權指數成分股[['weight_%','Code']].merge(industry).fillna("其他")
        富台指數成分股 = 富台指數成分股[['weight_%','Code']].merge(industry).fillna("其他")

        加權減富台 = pd.DataFrame(加權指數成分股.groupby('TSE_Industry')['weight_%'].sum() - 富台指數成分股.groupby('TSE_Industry')['weight_%'].sum()).dropna().sort_values('weight_%')

        加權前十 = pd.DataFrame(加權指數成分股.groupby('TSE_Industry')['weight_%'].sum()).sort_values('weight_%',ascending = False).head(10)
        富台前十 = pd.DataFrame(富台指數成分股.groupby('TSE_Industry')['weight_%'].sum()).sort_values('weight_%',ascending = False).head(10)
        富台台指差別 = (富台前十 - 加權前十).sort_values('weight_%',ascending = False)
        加權前五十 = pd.DataFrame(加權指數成分股).sort_values('weight_%',ascending = False).head(50).reset_index(drop = True)
        富台前五十 = pd.DataFrame(富台指數成分股).sort_values('weight_%',ascending = False).reset_index(drop = True)
        加權前五十 = 加權前五十.set_index('Code')
        富台前五十 = 富台前五十.set_index('Code')
        富台前五十 = 富台前五十.reindex(加權前五十.index).head(50)
        df3 = pd.DataFrame({f"相差權重": 加權前五十['weight_%'].sub(富台前五十['weight_%'])},index=加權前五十.index)
        print(加權前五十)
        print(富台前五十)
        print(df3)
        # 組合新的DataFrame
        df_new = pd.DataFrame({
                               'TSE_industry' : 加權前五十['TSE_Industry'],
                               '台指權重 (%)' : 加權前五十['weight_%'],
                               '富台權重 (%)' : 富台前五十['weight_%'],
                                 '相差權重 (%)' : df3['相差權重']},index=加權前五十.index)
        
        df_new['台指權重 (%)'] = df_new['台指權重 (%)'].apply(lambda x : np.around(x,4)*100).replace("nan","")
        df_new['富台權重 (%)'] = df_new['富台權重 (%)'].apply(lambda x : np.around(x,4)*100).replace("nan","")
        df_new['相差權重 (%)'] = df_new['相差權重 (%)'].apply(lambda x : np.around(x,4)*100).replace("nan","")

        print(df_new)
        ws = self.sh.worksheet_by_title('每日台指富台前十大產業權重')

        ws.clear()

        ws.update_value('A1:B1', "台指前十大產業權重")
        ws.update_value('D1:E1', "富台前十大產業權重")
        ws.update_value('G1:H1', "富台 - 台指")

        ws.update_value('J1:N1', "前50大成分股權重")

        ws.set_dataframe(加權前十.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'A2', copy_index=True, nan='')
        ws.set_dataframe(富台前十.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'D2', copy_index=True, nan='')
        ws.set_dataframe(富台台指差別.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'G2', copy_index=True, nan='')
        ws.set_dataframe(df_new, 'J2', copy_index=True, nan='')
        print("除息監控")
        ### 除息監控 
        DQ_stock_dde=PyWinDDE.DDEClient("DQII", "TWSE")
        DQ_index_dde=PyWinDDE.DDEClient("DQII", "INDX")
        DQ_future_dde=PyWinDDE.DDEClient("DQII", "FUSA")

        path = '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+self.today+"/"
        今日預估公司 = pd.read_csv(path+"全除息個股序列總表_"+self.today+".csv")
        監督合約 = self.台指合約_list[self.台指合約_list.index(TWN_contract_month(self.today)):self.台指合約_list.index(TWN_contract_month(self.today))+3]
        今日預估公司 = 今日預估公司[(今日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(self.today)) & 
                        (今日預估公司['台指合約月份'].isin(監督合約)) &今日預估公司['台指影響點數']>0].reset_index(drop = True)
        單純合約除息 = {}
        累積合約除息 = {}
        for 合約月份 in 監督合約 :
            單純合約除息["單純{}月合約".format(合約月份[6:7])] = np.around(今日預估公司[今日預估公司["台指合約月份"] == 合約月份]["台指影響點數"].sum(),4) 
        單純合約除息_df = pd.DataFrame(單純合約除息,index = ['除息點數']).T
        for 合約月份 in 監督合約 :
            累積合約除息["累積{}月合約".format(合約月份[6:7])] = np.around(今日預估公司.loc[:今日預估公司[今日預估公司["台指合約月份"] == 合約月份].index[-1]]["台指影響點數"].sum(),4)
        累積合約除息_df = pd.DataFrame(累積合約除息,index = ['除息點數']).T
        期貨價格 = {}
        print("監督合約",監督合約)
        期貨合約代碼 = ['WTXN3.129','WTXQ3.129','WTXU3.129']
        for ind , 合約月份 in enumerate(監督合約) :
            while True :
                try :
                    print(DQ_future_dde.request(期貨合約代碼[ind]).decode())
                    期貨價格["台指{}月期貨昨收".format(合約月份[6:7])] = float(DQ_future_dde.request(期貨合約代碼[ind]).decode()) # 5
                    break
                except Exception as e:
                    print(e)
                    sleep_time.sleep(3)
                    continue
        期貨價格_df = pd.DataFrame(期貨價格,index = ['昨收']).T
        期貨價差 = {}
        for ind , 合約月份 in enumerate(監督合約[1:]) :
            期貨價差["{}月{}月價差".format(監督合約[0][6:7],合約月份[6:7])] = 期貨價格_df.昨收[0] - 期貨價格_df.昨收[ind+1] 
        期貨價差_df = pd.DataFrame(期貨價差,index = ['價差']).T
        ws = self.sh.worksheet_by_title('除權息監控')
        ws.clear()
        ws.update_value('A1:E1', "近三個合約月 除息監控 更新時間 : {}".format(str(datetime.today())[0:19]))
        ws.set_dataframe(單純合約除息_df.replace("nan",""), 'A2', copy_index=True, nan='')
        ws.set_dataframe(累積合約除息_df.replace("nan",""), 'A7', copy_index=True, nan='')
        ws.set_dataframe(期貨價格_df.replace("nan",""), 'D2', copy_index=True, nan='')
        ws.set_dataframe(期貨價差_df.replace("nan",""), 'D7', copy_index=True, nan='')
    def 預估除息公司影響點數(self):
        data = pd.read_csv(self.path + f'/全除息個股序列總表_{self.today}.csv',encoding='utf-8')
        estimated_txf_df = data[(data['台指合約月份'] == self.台指合約月份) & (data['富台合約月份'] == self.富台合約月份) & (data['確認/預估'] == '預估') & (data['台指影響點數'] != 0)]
        estimated_twn_df = data[(data['台指合約月份'] == self.台指合約月份) & (data['富台合約月份'] == self.富台合約月份) & (data['確認/預估'] == '預估') & (data['富台影響點數'] != 0)]
        台指預估剩餘公司表_df = estimated_txf_df[['除息日','公司','確認/預估','台指影響點數']]
        富台預估剩餘公司表_df = estimated_twn_df[['除息日','公司','確認/預估','富台影響點數']]
        #除息日+14天影響合約
        #台指預估剩餘公司表_df['除息日+14天影響合約'] = 台指預估剩餘公司表_df['除息日'].apply(lambda x : TXF_contract_month(str(pd.to_datetime(x) + timedelta(days=14))[:10]))
        台指預估剩餘公司表_df.loc[:, '除息日+14天影響合約'] = 台指預估剩餘公司表_df['除息日'].apply(lambda x: TXF_contract_month(str(pd.to_datetime(x) + timedelta(days=14)))[:10])

        #富台預估剩餘公司表_df['除息日+14天影響合約'] = 富台預估剩餘公司表_df['除息日'].apply(lambda x : TWN_contract_month(str(pd.to_datetime(x) + timedelta(days=14))[:10]))
        富台預估剩餘公司表_df.loc[:, '除息日+14天影響合約'] = 富台預估剩餘公司表_df['除息日'].apply(lambda x: TXF_contract_month(str(pd.to_datetime(x) + timedelta(days=14)))[:10])

        ws_2 = self.sh.worksheet_by_title('合約內預估公司影響點數')
        ws_2.clear()
        ws_2.update_value('A1:I1', "台指/富台 今天近月減少 確定/預估除息日 之公司 更新時間 : %s " %(str(datetime.today())[0:19]))
        ws_2.update_value('A2:E2', "目前台指近月合約月份 : %s " %(self.台指合約月份))
        ws_2.update_value('G2:K2', "目前富台近月合約月份 : %s " %(self.富台合約月份))
        ws_2.set_dataframe(台指預估剩餘公司表_df.replace("nan",""), 'A3', copy_index=False, nan='')
        ws_2.set_dataframe(富台預估剩餘公司表_df.replace("nan",""), 'F3', copy_index=False, nan='')
        ws_2.update_value('F2', "台指總影響點數")
        ws_2.update_value('F3', "=sum(D4:D500)")
        ws_2.update_value('L2', "富台總影響點數")
        ws_2.update_value('L3', "=sum(I4:I500)") 
 

def main():
    from config import Config
    Dividend_Monitor = Google_Sheets_Post(Config)
    Dividend_Monitor.近五筆會議狀況()
    Dividend_Monitor.預估權值股除息列表()
    #Dividend_Monitor.今日新增Ratio影響公司()
    #Dividend_Monitor.post_to_line()
    #Dividend_Monitor.預估除息公司影響點數()
    #Dividend_Monitor.今日減少公司()
    Dividend_Monitor.每日台指富台前十大產業權重()
    
if __name__ == "__main__":
    main()

