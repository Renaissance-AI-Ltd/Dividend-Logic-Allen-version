import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import os
import tejapi



class Save_date :
    def __init__(self, current_date,config):
        self.current_date = current_date
        self.config = config
        self.ip_address = self.config.IP_ADDRESS
        self.saved_file_name = self.config.SAVED_FILE_NAME
        self.add_hours = self.config.ADD_HOURS
        self.前30大權值股 = self.config.TOP_30_RATIO_STOCK
        self.前30大預估權值股 = []
        self.create_directory()
        
    def create_directory(self):
        directories = [
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/加權指數/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/電子指數/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/金融指數/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/非金電指數/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/富台指/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/摩台指/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/log/全除息個股序列總表/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/log/預估除息總表/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/log/預估除息公司列表/',
            f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/log/前三十大權值股近五筆開會資訊/'
        ]
        for directory in directories:
            path = os.path.join(directory, str((self.current_date+timedelta(hours=self.add_hours)).date()))
            if not os.path.isdir(path):
                os.makedirs(path)
    def save_total_table_csv(self,台指除息點數,電子除息點數,金融除息點數,非金電除息點數,台指除息家數,電子除息家數,金融除息家數,非金電除息家數,富台除息點數,摩根除息點數,富台除息家數,摩根除息家數,全部結算日):
        國內除息_list=[]
        print(全部結算日['台指結算日'].iloc[0:3])
        print("台指除息點數 :\n",台指除息點數)
        for data in zip([台指除息點數,電子除息點數,金融除息點數,非金電除息點數],[台指除息家數,電子除息家數,金融除息家數,非金電除息家數]):
            use_dict={}
            for i in 全部結算日['台指結算日'].iloc[0:3].index:
                month_data_calc=(data[0].loc[:i].loc[::-1].cumsum()[::-1])
                use_month=str(i.month)
                if len(use_month)==1:
                    use_month='0'+use_month
                use_dict[f'{use_month}月合約加總剩餘影響點數']=month_data_calc-data[0]
            加總影響點數=data[0]
            加總除權息家數=data[1]
            加總累計影響點數=data[0].cumsum()
            print("加總影響點數 :\n",加總影響點數)
            print("加總除權息家數 :\n",加總除權息家數)
            print("加總累計影響點數 :\n",加總累計影響點數)
            除息_dataframe=pd.DataFrame([加總影響點數,加總除權息家數,加總累計影響點數]).T
            除息_dataframe.columns=['加總影響點數','加總除權息家數','加總累計影響點數']
            國內除息_list.append(除息_dataframe.join(pd.DataFrame(use_dict)))


        self.save_dataframe_to_csv(國內除息_list[0],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/加權指數/')
        self.save_dataframe_to_csv(國內除息_list[1],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/電子指數/')
        self.save_dataframe_to_csv(國內除息_list[2],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/金融指數/')
        self.save_dataframe_to_csv(國內除息_list[3],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/非金電指數/')
        國外除息_list=[]
        for data in zip([富台除息點數,摩根除息點數],[富台除息家數,摩根除息家數]):
            use_dict={}
            for i in 全部結算日['海外結算日'].iloc[0:3].index:
                month_data_calc=(data[0].loc[:i].loc[::-1].cumsum()[::-1])
                use_month=str(i.month)
                if len(use_month)==1:
                    use_month='0'+use_month
                use_dict[f'{use_month}月合約加總剩餘影響點數']=month_data_calc-data[0]
            加總影響點數=data[0]
            加總除權息家數=data[1]
            加總累計影響點數=data[0].cumsum()
            
            除息_dataframe=pd.DataFrame([加總影響點數,加總除權息家數,加總累計影響點數]).T
            除息_dataframe.columns=['加總影響點數','加總除權息家數','加總累計影響點數']
            國外除息_list.append(除息_dataframe.join(pd.DataFrame(use_dict)))


        self.save_dataframe_to_csv(國外除息_list[0],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/富台指/')
        self.save_dataframe_to_csv(國外除息_list[1],f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/摩台指/')
        總表=pd.DataFrame(index=國外除息_list[0].columns)
        總表.index.name=str((self.current_date+timedelta(hours=self.add_hours)).date())
        總表['加權指數']=國內除息_list[0].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        總表['電子指數']=國內除息_list[1].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        總表['金融指數']=國內除息_list[2].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        總表['非金電指數']=國內除息_list[3].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        總表['摩台指']=國外除息_list[1].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        總表['富台指']=國外除息_list[0].loc[(self.current_date+timedelta(hours=self.add_hours)).date()]
        print(總表)

        總表.drop('加總累計影響點數',inplace=True)
        總表=總表.round(4)
        總表.loc['加總除權息家數']=總表.loc['加總除權息家數'].astype(int)
        總表.to_csv(f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'/預估除息總表_'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'.csv',encoding='utf_8_sig')

    def save_dataframe_to_csv(self,dataframe, filepath):
        date_today = (self.current_date + timedelta(hours=self.add_hours)).date()
        filepath = filepath + f'{date_today}' +f'/{date_today}.csv'
        dataframe.to_csv(filepath, encoding='utf_8_sig')
    def save_total_dividend_df_csv(self,Total_dividend_df,台指權重佔比,電子權重佔比,金融權重佔比,非金電權重佔比,富台權重佔比,摩根權重佔比,future_代碼):
        # Set the file paths
        total_dividend_path = f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/'
        # Define the file name for saving the total dividend DataFrame
        # Save Total_dividend_df to CSV without log path
        Total_dividend_df.to_csv(total_dividend_path + f'{self.current_date.date()}/全除息個股序列總表_{self.current_date.date()}.csv', encoding='utf_8_sig')
        # Convert index to strin
        富台權重佔比.index = 富台權重佔比.index.astype(str)
        # Filter Total_dividend_df for rows from today onwards and drop NaN values
        filtered_dividend_df = Total_dividend_df.loc[str(self.current_date):].dropna()
        # Calculate the sum of '台指影響點數' where '確認/預估' is '預估'
        total_taiex_points = Total_dividend_df[Total_dividend_df['確認/預估'] == '預估']['台指影響點數'].sum()
        # Create the '權重data' DataFrame by concatenating various dataframes
        權重data = pd.concat([台指權重佔比, 電子權重佔比, 金融權重佔比, 非金電權重佔比, 富台權重佔比, 摩根權重佔比, future_代碼],
                            axis=1, keys=['TX weight', 'TE weight', 'TF weight', 'XIF weight', 'TWN weight', 'MTW weight', 'future code']).fillna(0)
        # Create the 'future_data' DataFrame
        total_list = []
        item = ['101', '102', '125', '113', '114']
        for i in 權重data['future code']:
            use_list = []
            for t in item:
                if i != 0:
                    use_list.append(f"=DQII|FUSA!'{i}.{t}'")
                else:
                    use_list.append("")
            total_list.append(pd.Series(use_list))
        future_data = pd.concat(total_list, axis=1).T
        # Add 'future_data' columns to '權重data'
        權重data['F Bid'] = future_data[0].values
        權重data['F Ask'] = future_data[1].values
        權重data['F Trade'] = future_data[2].values
        權重data['F Bid Volume'] = future_data[3].values
        權重data['F Ask Volume'] = future_data[4].values
        print(權重data)
        print(future_data)
        # Print Total_dividend_df
        print(Total_dividend_df)
        # Create the '前十大用df' DataFrame by filtering Total_dividend_df and setting the index
        前十大用df = Total_dividend_df.loc[str(self.current_date.year) + '-' + str(self.current_date.month):].reset_index().set_index('公司')
    def save_top30_data_csv(self,predict_date_df,前30大預估權值股):
        total_lst = predict_date_df.公司
        # Retrieve data
        data = tejapi.get('TWN/AMT', paginate=True)
        table_info = tejapi.table_info('TWN/AMT')
        cname_mapping_dict = table_info['columns']
        data.columns=[cname_mapping_dict[i]['cname'] for i in data.columns]
        data["會議日期"]=pd.to_datetime(data["會議日期"].apply(lambda x:str(x)[0:10]))
        data["除息日"]=pd.to_datetime(data["除息日"].apply(lambda x:str(x)[0:10]))
        data["除權日(配股)"]=pd.to_datetime(data["除權日(配股)"].apply(lambda x:str(x)[0:10]))
        data['公司']=data['公司'].astype(str)
        # Create output dictionaries
        df_total_list = {}
        delist_dic = {}  # Assuming this dictionary is defined somewhere
        # Populate df_total_list for each company in total_lst
        for company in total_lst:
            df_total_list[company] = data[data['公司'] == company][['會議日期', '公司', '常會YN／董事會D', '董事會日期', '現金股利(元)', '除息日', '股息分配型態', '臨時會開會目的']].tail(5)
            df_total_list[company]['下市年分'] = df_total_list[company]['公司'].map(delist_dic)
            df_total_list[company]['相差日期'] = (df_total_list[company]['除息日'] - df_total_list[company]['會議日期']).dt.days
            df_total_list[company]['會議日期'] = df_total_list[company]['會議日期'].astype(str)
            df_total_list[company]['除息日'] = df_total_list[company]['除息日'].astype(str)
            df_total_list[company]['董事會日期'] = df_total_list[company]['董事會日期'].astype(str)
        # Define output paths
        today = (self.current_date+ timedelta(hours=self.add_hours)).date()
        output_dir = f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/'
        xlsx_path = f'{output_dir}{today}/預估除息公司列表_{today}.xlsx'
        # Write output to Excel
        with pd.ExcelWriter(xlsx_path) as writer:
            for ind, firm_name in enumerate(df_total_list.keys()):
                df_total_list[firm_name].to_excel(writer, startrow=ind*7+ind+1)
        print("\n")
        print("輸出台指/富台 前30大權值股近五筆開會資料")


        int_list = []
        前30大權值股_sort = []

        for i in self.前30大權值股 :
            int_list.append(int(i))
            
        int_list.sort()
        for i in int_list :
            前30大權值股_sort.append(str(i))
        前30大權值股_str = self.config.TOP_30_RATIO_STOCK
        前30大預估權值股.sort(key=lambda x: 前30大權值股_str.index(x))
        df_total_list = {}
        for i in 前30大權值股_str : 
            df_total_list[i] = data.reset_index()[data.reset_index()['公司'] == i][['會議日期','公司','常會YN／董事會D','董事會日期','現金股利(元)',"除息日",'股息分配型態','臨時會開會目的']].tail(5)
            df_total_list[i]['相差日期'] = (df_total_list[i]['除息日']-df_total_list[i]['會議日期']).values
            df_total_list[i]['會議日期'] = df_total_list[i]['會議日期'].apply(lambda x : str(x))
            df_total_list[i]['除息日'] = df_total_list[i]['除息日'].apply(lambda x : str(x))
            df_total_list[i]['董事會日期'] = df_total_list[i]['董事會日期'].apply(lambda x : str(x))  
        #path = f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'/前三十大權值股近五筆開會資訊_'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'.xlsx'
        xlsx_path = f'{output_dir}{today}/前三十大權值股近五筆開會資訊_{today}.xlsx'
        with pd.ExcelWriter(xlsx_path) as writer:   
            for ind ,firm_name in enumerate(df_total_list.keys()) :  
                df_total_list[firm_name].to_excel(writer,startrow = ind*7+ind+1)
        df_total_list = {}
        for j in 前30大預估權值股 : 
            df_total_list[j] = data.reset_index()[data.reset_index()['公司'] == j][['會議日期','公司','常會YN／董事會D','董事會日期','現金股利(元)',"除息日",'股息分配型態','臨時會開會目的']].tail(5)
            df_total_list[j]['相差日期'] = (df_total_list[j]['除息日']-df_total_list[j]['會議日期']).values
            df_total_list[j]['會議日期'] = df_total_list[j]['會議日期'].apply(lambda x : str(x))
            df_total_list[j]['除息日'] = df_total_list[j]['除息日'].apply(lambda x : str(x))
            df_total_list[j]['董事會日期'] = df_total_list[j]['董事會日期'].apply(lambda x : str(x))        
        #path = f'//{self.ip_address}/Wellington/{self.saved_file_name}/除息預估/除息總整理/'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'/前30大預估權值股_'+str((self.current_date+timedelta(hours=self.add_hours)).date())+'.xlsx'
        xlsx_path = f'{output_dir}{today}/前30大預估權值股_{today}.xlsx'
        with pd.ExcelWriter(xlsx_path) as writer:        
            for ind ,firm_name in enumerate(df_total_list.keys()) :  
                df_total_list[firm_name].to_excel(writer,startrow = ind*7+ind+1)



            
