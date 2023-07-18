from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import shutil
from datetime import datetime
import time
def download_latest_file():
    driver = webdriver.Chrome()
    # 登入的目標網站URL
    login_url = 'https://data.ftse.com/vp.jsp?path=~/RenaiAla5266/FTSE_Taiwan_RIC_Capped_TWD/Constituent'
    username = 'RenaiAla5266'
    password = 'GBBc4810'
    # 設定下載資料夾路徑
    download_folder = r'C:\Users\eric\Downloads'  # 下載資料夾的路徑

    # 設定目的地資料夾路徑
    destination_folder = '//192.168.60.81/Wellington/Faker/富台權重' # 指定的目的地資料夾的路徑

    # 移動並重新命名檔案
    source_file_name = 'ftcrtwdclst.csv'
    # 要下載CSV檔案的路徑

    # 使用Chrome瀏覽器驅動程式，需先下載對應版本的驅動程式並指定其路徑

    # 進入登入頁面
    driver.get(login_url)

    # 找到帳號和密碼的輸入框，並輸入相應的值
    driver.find_element(By.NAME,'j_username').send_keys(username)
    driver.find_element(By.NAME,'j_password').send_keys(password)

    # 找到登入按鈕，並點擊
    driver.find_element(By.NAME,'Submit').click()

    # 登入後等待頁面加載完成
    driver.get(login_url)
    # 進入特定路徑的資料夾
    #driver.get(csv_url)
    download_link = driver.find_element(By.NAME,'ftcrtwdclst.csv')

    # 取得下載連結的URL
    csv_download_url = download_link.get_attribute('href')

    # 下載CSV檔案
    driver.get(csv_download_url)
    time.sleep(20)
    driver.quit()
    current_date = datetime.today().date()

    new_file_name = f'TWII-{str(current_date)}.csv'
    current_date = datetime.today().date()
    str(current_date)
    source_path = os.path.join(download_folder, source_file_name)
    destination_path = os.path.join(destination_folder, new_file_name)

    shutil.move(source_path, destination_path)

if __name__ == '__main__':
    download_latest_file()
