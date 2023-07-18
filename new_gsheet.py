#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import pygsheets
from pandas.tseries.offsets import BDay
import datetime as dt
import PyWinDDE
import holidays
# def bday_counter(date,n) :
    
#     tw_holiday_list = ["2023-04-03","2023-04-04","2023-04-05","2023-05-01","2023-06-23","2023-06-22","2023-09-29","2023-10-09","2023-10-10"]
#     date = str(date)[0:10]
#     yesterday = str(pd.to_datetime(date)-BDay(n))[0:10]
    
#     while yesterday in tw_holiday_list :
#         yesterday = str(pd.to_datetime(yesterday)-BDay(n))[0:10]
#     else :
#         return yesterday
def bday_counter(date, n):
    holiday_list = holidays.TW(years=range(date.year, date.year + 2))  # Generate a list of holidays for the current year and the next year
    date = date.date()  # Convert datetime to date
    yesterday = date - pd.DateOffset(days=n)  # Calculate the initial yesterday date
    while yesterday.strftime("%Y-%m-%d") in holiday_list:
        yesterday -= pd.DateOffset(days=n)  # Subtract n days from yesterday
    return yesterday.strftime("%Y-%m-%d")              

def third_wen(date_str):
    
    y = int(date_str[0:4])
    m = int(date_str[5:7])
    
    
    day=21-(dt.date(y,m,1).weekday()+4)%7  
    return dt.date(y,m,day)  


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


富台合約_list = []
台指合約_list = []
for i in ['2023-01-30','2023-02-23','2023-03-30','2023-04-27','2023-05-30','2023-06-29','2023-07-28','2023-08-30','2023-09-28','2023-10-30','2023-11-29','2023-12-28'] :
    富台合約_list.append(TWN_contract_month(i))
    台指合約_list.append(TXF_contract_month(i))



today = str((dt.datetime.today().date()))
# today = "2023-06-08"
yesterday = bday_counter(today[0:10],1)

print("今日使用資料 : {}          昨日使用資料 : {} \n".format(today,yesterday))

# yesterday = "2023-04-20"


path = '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+today+"/"
path_y =  '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+yesterday+"/"

# In[33]:


gc = pygsheets.authorize(service_account_file='dividend_gspread.json')

survey_url = 'https://docs.google.com/spreadsheets/d/1GksJ5z94MOzLNcIEct_x_aWoWzBi4CEiHJUHjU7l0IQ/edit#gid=0'
sh = gc.open_by_url(survey_url)


# In[53]:


path = '//192.168.60.81/Wellington/Wayne/除息預估/除息總整理/'+today+"/"


今日除息總表 = pd.read_csv(path+"預估除息總表_"+today+".csv")
昨日除息總表 = pd.read_csv(path_y+"預估除息總表_"+yesterday+".csv")
除息總表差別 = (今日除息總表.set_index(today).T - 昨日除息總表.set_index(yesterday).T).dropna(axis = 1).T


# In[55]:

print("讀取權指股前五筆開會資料")
權指股前五筆開會 = pd.read_excel(path+"前三十大權值股近五筆開會資訊_"+today+".xlsx",header=1).drop(['Unnamed: 0'],axis = 1)


# In[56]:


權指股前五筆開會['會議日期'] = 權指股前五筆開會['會議日期'].apply(lambda x : str(x)[:10])
權指股前五筆開會['董事會日期'] = 權指股前五筆開會['董事會日期'].apply(lambda x : str(x)[:10])
權指股前五筆開會['除息日'] = 權指股前五筆開會['除息日'].apply(lambda x : str(x)[:10])


# In[59]:
print("讀取前三十大權值股預估")

前三十大權值股預估 = pd.read_excel(path+"前30大預估權值股_"+today+".xlsx",header=1).drop(['Unnamed: 0'],axis = 1)


# In[60]:


前三十大權值股預估['會議日期'] = 前三十大權值股預估['會議日期'].apply(lambda x : str(x)[:10])
前三十大權值股預估['董事會日期'] = 前三十大權值股預估['董事會日期'].apply(lambda x : str(x)[:10])
前三十大權值股預估['除息日'] = 前三十大權值股預估['除息日'].apply(lambda x : str(x)[:10])

print("前三十大權值股資料處理完成")


# In[61]:


前三十大權值股預估


# In[62]:
print("近五筆會議狀況 worksheet")


ws = sh.worksheet_by_title('近五筆會議狀況')

ws.clear()

ws.update_value('A1:J1', "台指/富台 前30大權指股 近五筆會議狀況 更新時間 : %s " %(str(dt.datetime.today())[0:19]))

# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws.set_dataframe(權指股前五筆開會.replace("nan",""), 'A2', copy_index=False, nan='')

print("近五筆會議狀況 worksheet done")
# In[63]:

print("預估權值股除息列表 worksheet")
ws_1 = sh.worksheet_by_title('預估權值股除息列表')

ws_1.clear()

ws_1.update_value('A1:J1', "台指/富台 前30大權指股 預估除息日 更新時間 : %s " %(str(dt.datetime.today())[0:19]))

# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_1.set_dataframe(前三十大權值股預估.replace("nan",""), 'A2', copy_index=False, nan='')


台指合約月份 = TXF_contract_month(today)
富台合約月份 = TWN_contract_month(today)

今日預估公司 = pd.read_csv(path+"全除息個股序列總表_"+today+".csv")
昨日預估公司 = pd.read_csv(path_y+"全除息個股序列總表_"+yesterday+".csv")

台指今日預估公司列表 = [x for x in 今日預估公司[(今日預估公司['台指合約月份']== 台指合約月份) & (今日預估公司['台指影響點數'] > 0)].公司]
台指昨日預估公司列表 = [x for x in 昨日預估公司[(昨日預估公司['台指合約月份']== 台指合約月份) & (昨日預估公司['台指影響點數'] > 0)].公司]

富台今日預估公司列表 = [x for x in 今日預估公司[(今日預估公司['富台合約月份']== 富台合約月份) & (今日預估公司['富台影響點數'] > 0)].公司]
富台昨日預估公司列表 = [x for x in 昨日預估公司[(昨日預估公司['富台合約月份']== 富台合約月份) & (昨日預估公司['富台影響點數'] > 0)].公司]

台指今日新增預估或確認除息 = set(台指今日預估公司列表)-set(台指昨日預估公司列表)
富台今日新增預估或確認除息 = set(富台今日預估公司列表)-set(富台昨日預估公司列表)

台指今日新增預估或確認除息_df = 今日預估公司[(今日預估公司['公司'].isin(台指今日新增預估或確認除息)) & (今日預估公司['台指合約月份']== 台指合約月份)][['除息日','公司','確認/預估','台指影響點數']]
富台今日新增預估或確認除息_df = 今日預估公司[(今日預估公司['公司'].isin(富台今日新增預估或確認除息)) & (今日預估公司['富台合約月份']== 富台合約月份)][['除息日','公司','確認/預估','富台影響點數']]


台指遠月合約月份 = 台指合約_list[台指合約_list.index(台指合約月份) + 1 ]
富台遠月合約月份 = 富台合約_list[富台合約_list.index(富台合約月份) + 1 ]


台指遠月今日預估公司列表 = [x for x in 今日預估公司[(今日預估公司['台指合約月份']== 台指遠月合約月份) & (今日預估公司['台指影響點數'] > 0)].公司]
台指遠月昨日預估公司列表 = [x for x in 昨日預估公司[(昨日預估公司['台指合約月份']== 台指遠月合約月份) & (昨日預估公司['台指影響點數'] > 0)].公司]

富台遠月今日預估公司列表 = [x for x in 今日預估公司[(今日預估公司['富台合約月份']== 富台遠月合約月份) & (今日預估公司['富台影響點數'] > 0)].公司]
富台遠月昨日預估公司列表 = [x for x in 昨日預估公司[(昨日預估公司['富台合約月份']== 富台遠月合約月份) & (昨日預估公司['富台影響點數'] > 0)].公司]

台指遠月今日新增預估或確認除息 = set(台指遠月今日預估公司列表)-set(台指遠月昨日預估公司列表)
富台遠月今日新增預估或確認除息 = set(富台遠月今日預估公司列表)-set(富台遠月昨日預估公司列表)

台指遠月今日新增預估或確認除息_df = 今日預估公司[(今日預估公司['公司'].isin(台指遠月今日新增預估或確認除息)) & (今日預估公司['台指合約月份']== 台指遠月合約月份)][['除息日','公司','確認/預估','台指影響點數']]
富台遠月今日新增預估或確認除息_df = 今日預估公司[(今日預估公司['公司'].isin(富台遠月今日新增預估或確認除息)) & (今日預估公司['富台合約月份']== 富台遠月合約月份)][['除息日','公司','確認/預估','富台影響點數']]

台指今日除息公司 = 今日預估公司[今日預估公司['除息日'].apply(lambda x :pd.to_datetime(x)) == pd.to_datetime(today)]
富台今日除息公司 = 今日預估公司[今日預估公司['除息日'].apply(lambda x :pd.to_datetime(x)) == pd.to_datetime(today)]
台指今日除息公司 = 台指今日除息公司[台指今日除息公司['台指影響點數']>0][['除息日','公司','確認/預估','台指影響點數']]
富台今日除息公司 = 富台今日除息公司[富台今日除息公司['富台影響點數']>0][['除息日','公司','確認/預估','富台影響點數']]

print("預估權值股除息列表 worksheet done")


print("今日新增預估公司 worksheet")

ws_2 = sh.worksheet_by_title('今日新增預估公司')

ws_2.clear()

ws_2.update_value('A1:I1', "台指/富台 今天近月新增 確定/預估除息日 之公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))
ws_2.update_value('A2:D2', "目前台指近月合約月份 : %s " %(台指合約月份))
ws_2.update_value('F2:I2', "目前富台近月合約月份 : %s " %(富台合約月份))
# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.set_dataframe(台指今日新增預估或確認除息_df.replace("nan",""), 'A3', copy_index=False, nan='')
ws_2.set_dataframe(富台今日新增預估或確認除息_df.replace("nan",""), 'F3', copy_index=False, nan='')

ws_2.update_value('K1:S1', "台指/富台 今天遠月新增 確定/預估除息日 之公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))
ws_2.update_value('K2:N2', "目前台指遠月合約月份 : %s " %(台指遠月合約月份))
ws_2.update_value('P2:S2', "目前富台遠月合約月份 : %s " %(富台遠月合約月份))
# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.set_dataframe(台指遠月今日新增預估或確認除息_df.replace("nan",""), 'K3', copy_index=False, nan='')
ws_2.set_dataframe(富台遠月今日新增預估或確認除息_df.replace("nan",""), 'P3', copy_index=False, nan='')





ws_2.update_value('U1:AA1', " %s 預估除息總表 & 與前日差額 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.update_value('U5', " 差額 " )
ws_2.update_value('U4:AA4', " %s 與 %s 預估除息總表差額 " %(today,yesterday))
ws_2.update_value('U12:AA12', " %s 預估除息總表 " %(today))
ws_2.update_value('U20:AA20', " %s 預估除息總表 " %(yesterday))


ws_2.set_dataframe(除息總表差別.replace("nan",""), 'U5', copy_index=True, nan='')
ws_2.set_dataframe(今日除息總表.replace("nan",""), 'U13', copy_index=False, nan='')
ws_2.set_dataframe(昨日除息總表.replace("nan",""), 'U21', copy_index=False, nan='')

ws_2.update_value('U5', " 差額 " )

ws_2.update_value('E2', "台指總影響點數")
ws_2.update_value('E3', "=sum(D4:D500)")

ws_2.update_value('J2', "富台總影響點數")
ws_2.update_value('J3', "=sum(I4:I500)") 

ws_2.update_value('O2', "台指總影響點數")
ws_2.update_value('O3', "=sum(N4:N500)")

ws_2.update_value('T2', "富台總影響點數")
ws_2.update_value('T3', "=sum(S4:S500)")


ws_2.update_value('AC1:AK1', "台指/富台 今天除息公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))

ws_2.update_value('AC2:AF2', "目前台指近月合約月份 : %s " %(台指合約月份))
ws_2.update_value('AH2:AK2', "目前富台近月合約月份 : %s " %(富台合約月份))

ws_2.set_dataframe(台指今日除息公司.replace("nan",""), 'AC3', copy_index=False, nan='')
ws_2.set_dataframe(富台今日除息公司.replace("nan",""), 'AH3', copy_index=False, nan='')



ws_2.update_value('AG2', "台指總影響點數")
ws_2.update_value('AG3', "=sum(AF4:AF500)")

ws_2.update_value('AL2', "富台總影響點數")
ws_2.update_value('AL3', "=sum(AK4:AK500)") 





#前30大權值股_int = [2330,2317,2454,2412,6505,2308,2881,2303,2882,1303,1301,2002,3711,2886,2891,1326,1216,5880,5871,2884,2892,3045,2603,2207,2382,2880,3008,2885,2912,1101,2395,3034,6415,5876,2609]
前30大權值股_str = ["2330","2317","2454","2412","6505","2308","2881","2303","2882","1303","1301","2002","3711","2886","2891","1326","1216","5880","5871","2884","2892","3045","2207","2603","3008","2382","2880","2885","2912","2395","1101","3034","2327","2883","2357","2609","4904","5876","2615","6415","1590","2890","3037","2379","2887","1605","4938","2801","2408","2301","2345","2409","2474","3529","8069"]

前30大公司預估_df = 今日預估公司[(今日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(today)) & 今日預估公司.公司.isin(前30大權值股_str)]


除息日_dic = {}
台指影響點數_dic = {}
富台影響點數_dic = {}
台富影響Ratio_同合約月_dic = {}
台富影響Ratio_台近富遠_dic = {}
台富影響Ratio_台遠富近_dic = {}

for i in 前30大公司預估_df["公司"].unique() :
    除息日_dic[i] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].除息日.values[0]
    台指影響點數_dic[(除息日_dic[i],i)] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].台指影響點數.values[0]
    富台影響點數_dic[(除息日_dic[i],i)] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].富台影響點數.values[0]
    台富影響Ratio_同合約月_dic[(除息日_dic[i],i)] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].台富影響Ratio_同合約月.values[0]
    台富影響Ratio_台近富遠_dic[(除息日_dic[i],i)] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].台富影響Ratio_台近富遠.values[0]
    台富影響Ratio_台遠富近_dic[(除息日_dic[i],i)] = 前30大公司預估_df[前30大公司預估_df["公司"] == i].台富影響Ratio_台遠富近.values[0]

ws_3 = sh.worksheet_by_title('預估除權息日期_依照市值排序')
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



台指今日減少預估或確認除息 = set(台指昨日預估公司列表)-set(台指今日預估公司列表)
富台今日減少預估或確認除息 = set(富台昨日預估公司列表)-set(富台今日預估公司列表)

台指今日減少預估或確認除息_df = 昨日預估公司[(昨日預估公司['公司'].isin(台指今日減少預估或確認除息)) & (昨日預估公司['台指合約月份']== 台指合約月份)][['除息日','公司','確認/預估','台指影響點數']]
富台今日減少預估或確認除息_df = 昨日預估公司[(昨日預估公司['公司'].isin(富台今日減少預估或確認除息)) & (昨日預估公司['富台合約月份']== 富台合約月份)][['除息日','公司','確認/預估','富台影響點數']]

台指遠月今日減少預估或確認除息 = set(台指遠月昨日預估公司列表)-set(台指遠月今日預估公司列表)
富台遠月今日減少預估或確認除息 = set(富台遠月昨日預估公司列表)-set(富台遠月今日預估公司列表)

台指遠月今日減少預估或確認除息_df = 昨日預估公司[(昨日預估公司['公司'].isin(台指遠月今日減少預估或確認除息)) & (昨日預估公司['台指合約月份']== 台指遠月合約月份)][['除息日','公司','確認/預估','台指影響點數']]
富台遠月今日減少預估或確認除息_df = 昨日預估公司[(昨日預估公司['公司'].isin(富台遠月今日減少預估或確認除息)) & (昨日預估公司['富台合約月份']== 富台遠月合約月份)][['除息日','公司','確認/預估','富台影響點數']]

print("今日新增預估公司 worksheet done")

print("今日減少預估公司 worksheet")

ws_2 = sh.worksheet_by_title('今日減少公司')

ws_2.clear()

ws_2.update_value('A1:I1', "台指/富台 今天近月減少 確定/預估除息日 之公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))
ws_2.update_value('A2:D2', "目前台指近月合約月份 : %s " %(台指合約月份))
ws_2.update_value('F2:I2', "目前富台近月合約月份 : %s " %(富台合約月份))
# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.set_dataframe(台指今日減少預估或確認除息_df.replace("nan",""), 'A3', copy_index=False, nan='')
ws_2.set_dataframe(富台今日減少預估或確認除息_df.replace("nan",""), 'F3', copy_index=False, nan='')

ws_2.update_value('K1:S1', "台指/富台 今天遠月減少 確定/預估除息日 之公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))
ws_2.update_value('K2:N2', "目前台指遠月合約月份 : %s " %(台指遠月合約月份))
ws_2.update_value('P2:S2', "目前富台遠月合約月份 : %s " %(富台遠月合約月份))
# ws.update_value('J1:K1', "%s 權證Indicator, DW Price, Term 資料 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.set_dataframe(台指遠月今日減少預估或確認除息_df.replace("nan",""), 'K3', copy_index=False, nan='')
ws_2.set_dataframe(富台遠月今日減少預估或確認除息_df.replace("nan",""), 'P3', copy_index=False, nan='')

ws_2.update_value('U1:AA1', " %s 預估除息總表 & 與前日差額 更新時間 : %s " %(today,str(dt.datetime.today())[0:19]))

ws_2.update_value('U5', " 差額 " )
ws_2.update_value('U4:AA4', " %s 與 %s 預估除息總表差額 " %(today,yesterday))
ws_2.update_value('U12:AA12', " %s 預估除息總表 " %(today))
ws_2.update_value('U20:AA20', " %s 預估除息總表 " %(yesterday))


ws_2.set_dataframe(除息總表差別.replace("nan",""), 'U5', copy_index=True, nan='')
ws_2.set_dataframe(今日除息總表.replace("nan",""), 'U13', copy_index=False, nan='')
ws_2.set_dataframe(昨日除息總表.replace("nan",""), 'U21', copy_index=False, nan='')

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

print("今日新增ratio影響公司 worksheet")

con1_t = 今日預估公司['台富影響Ratio_同合約月'] != 0
con2_t = 今日預估公司['台富影響Ratio_台近富遠'] != 0
con3_t = 今日預估公司['台富影響Ratio_台遠富近'] != 0

con4_t = 今日預估公司['台指合約月份'] == 台指合約月份
con5_t = 今日預估公司['台指合約月份'] == 台指遠月合約月份
con6_t = 今日預估公司['富台合約月份'] == 富台合約月份
con7_t = 今日預估公司['富台合約月份'] == 富台遠月合約月份

今日ratio影響公司 = 今日預估公司[(今日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(today)) & (con1_t | con2_t | con3_t)]
今日ratio影響公司 = 今日ratio影響公司[(con4_t|con5_t|con6_t|con7_t)]

con1_y = 昨日預估公司['台富影響Ratio_同合約月'] != 0
con2_y = 昨日預估公司['台富影響Ratio_台近富遠'] != 0
con3_y = 昨日預估公司['台富影響Ratio_台遠富近'] != 0

con4_y = 昨日預估公司['台指合約月份'] == 台指合約月份
con5_y = 昨日預估公司['台指合約月份'] == 台指遠月合約月份
con6_y = 昨日預估公司['富台合約月份'] == 富台合約月份
con7_y = 昨日預估公司['富台合約月份'] == 富台遠月合約月份

昨日ratio影響公司 = 昨日預估公司[(昨日預估公司['除息日'].apply(lambda x : pd.to_datetime(x)) >= pd.to_datetime(yesterday)) & (con1_y | con2_y | con3_y)]
昨日ratio影響公司 = 昨日ratio影響公司[(con4_y|con5_y|con6_y|con7_y)]


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

ws_6 = sh.worksheet_by_title('每日新增Ratio影響')

ws_6.clear()

ws_6.update_value('A1:J1', "台指/富台 今天新增 近月&遠月 影響ratio公司 更新時間 : %s " %(str(dt.datetime.today())[0:19]))
ws_6.set_dataframe(ratio影響新增公司_df.replace("nan",""), 'A2', copy_index=False, nan='')

print("今日新增ratio影響公司 worksheet done")





print("更新成功!")


def line_notify(token):
    import requests
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


import os
import datetime

now = datetime.datetime.now().time() 
target_time_am = datetime.time(hour=8, minute=45) 

if now < target_time_am :
    token_company = "pD9ODB7gtrmYiM4So3UmCoaPigaCE8UpzXUsG8Mdh7k"
    line_notify(token_company)
    
print("指數權重")

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import os
plt.style.use('seaborn')
import datetime as dt
import string

import pygsheets

path='//192.168.60.83/Wellington/Allen/Index'
twn_path='//192.168.60.83/Wellington/Allen/Index/BloombergTWNI/'
index_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index')
TWN_list=os.listdir(r'\\192.168.60.83\Wellington\Allen\Index\BloombergTWNI')

今日資料日期=((datetime.datetime.today())).date()
# 今日資料日期 = datetime.date(2023,3,30)
today_readmode=今日資料日期.strftime('%b %d %Y')

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

富台指數成分股=pd.read_excel(twn_path+'/'+TWN_use_data,header=0,index_col=0)[["股數","價格"]]
富台指數成分股.index=[i.split(" ")[0] for i in 富台指數成分股.index]
富台指數成分股.columns=['Shares',"Close"]

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

stock_lst = [2330,2317,2454,2412,6505,2308,2881,2303,2882,1303,1301,2002,3711,2886,2891,1326,1216,5880,
             5871,2884,2892,3045,2207,2603,3008,2382,2880,2885,2912,2395,1101,3034,2327,2883,2357,2609,
             4904,5876,2615,6415,1590,2890,3037,2379,2887,1605,4938,2801,2408,2301,2345,2409,2474,3529,8069]

str_lst = [str(x) for x in stock_lst]

前50大台指成分股 = 加權指數成分股[加權指數成分股.Code.isin(str_lst)].sort_values('weight_%',ascending = False).reset_index(drop = True).rename(columns = {"weight_%":"台指權重"})
前50大富台成分股 = 富台指數成分股[富台指數成分股.Code.isin(str_lst)].sort_values('weight_%',ascending = False).reset_index(drop = True).rename(columns = {"weight_%":"富台權重"})
成分股權重 = 前50大台指成分股.merge(前50大富台成分股)[['Code',"TSE_Industry","台指權重","富台權重"]]
成分股權重['相差'] = 成分股權重['台指權重'] - 成分股權重['富台權重']
for i in 成分股權重.columns[2:] :
    成分股權重[i] = 成分股權重[i].apply(lambda x : str(np.around(x*100,2))) + " %"





ws = sh.worksheet_by_title('每日台指富台前十大產業權重')

ws.clear()

ws.update_value('A1:B1', "台指前十大產業權重")
ws.update_value('D1:E1', "富台前十大產業權重")
ws.update_value('G1:H1', "富台 - 台指")
ws.update_value('J1:N1', "前50大成分股權重")
ws.update_value('A14:D14', "更新時間 : %s " %(str(dt.datetime.today())[0:19]))

ws.set_dataframe(加權前十.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'A2', copy_index=True, nan='')
ws.set_dataframe(富台前十.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'D2', copy_index=True, nan='')
ws.set_dataframe(富台台指差別.apply(lambda x : np.around(x,4)*100).replace("nan",""), 'G2', copy_index=True, nan='')
ws.set_dataframe(成分股權重.replace("nan",""), 'J2', copy_index=False, nan='')
