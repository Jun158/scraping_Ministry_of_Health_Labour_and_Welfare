# Versions Python 3.10.0  Selenium 4.13.0
# 234行目のmain()を呼ぶときのパラメーター: main(ターゲットページ, 期間)
# ターゲットページのキーワード
    # 労働者派遣事業 ー＞ worker_dispatching
    # 職業紹介事業   ー＞ employment_placement
# 期間のキーワード
    # 直近2か月分のデータを取得する ー＞ "recent"
    # 全期間のデータを取得する      ー＞ "entire"
# 例）**労働者派遣事業**のデータを**全期間**取得したい場合　ー＞　main(worker_dispatching, entire)
# 例）**職業紹介事業**のデータを**直近二か月分**取得したい場合　ー＞　main(employment_placement, recent)

from selenium import webdriver
from time import sleep
import pandas as pd
import requests
from selenium.webdriver.support.ui import Select
from jeraconv import jeraconv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.by import By

def setup_driver(): 
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    return webdriver.Chrome(options=options)


def fill_out_date_range(driver):
    select = Select(driver.find_element("id", "ID_ucGengou1")); select.select_by_index(3)
    w2j = jeraconv.W2J()
    year = w2j.convert(return_type='dict')["year"]
    two_month_ago = datetime.now() - relativedelta(months=2)    
    month = two_month_ago.strftime("%m")
    day = two_month_ago.strftime("%d")
    driver.find_element('id', "ID_txtYear1").send_keys(year)
    driver.find_element('id', "ID_txtMonth1").send_keys(month)
    driver.find_element('id', "ID_txtDay1").send_keys(day)


def navigate_to_initial_page(driver, target_page, date_range):
    driver.get('https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb/GICB101010.do?screenId=GICB101010&action=initDisp');sleep(1)
    if target_page=="worker_dispatching": driver.find_element('xpath', '//*[@id="text_area"]/div[1]/dl/dt[1]/a/img').click()
    if target_page=="employment_placement": driver.find_element('xpath', '//*[@id="text_area"]/div[1]/dl/dt[2]/a/img').click();sleep(1)
    driver.find_element('id', 'ID_cbZenkoku1').click();sleep(1)
    if date_range=="recent":
        driver.find_element('xpath', '/html/body/form/div[2]/div[3]/div/div[2]/input').click()
        fill_out_date_range(driver)
        driver.find_element('xpath', '//*[@id="id_btnOk"]').click()
    elif date_range=="entire":
        pass
    else: # for debugging
        driver.find_element('id', 'ID_txtKyokatodokedeNo1').send_keys("13")
        select = Select(driver.find_element("id", 'ID_ucKyokatodokedeNo2')); select.select_by_index(1)
        driver.find_element('id', 'ID_txtKyokatodokedeNo3').send_keys("315073")
    driver.find_element('xpath', '/html/body/form/div[2]/div[3]/div/div[3]/input').click()
    sleep(1)


def extract_table_data(driver, jdg, save_flag):
    temp_webtable_df = None 
    while (True):
        try:
            table_html = driver.find_element('id', 'search').get_attribute('outerHTML')
            temp_webtable_df = pd.read_html(table_html)[0]
            break
        except Exception as e:
            logging.error("extract_table_data, an error message: \n%s\n", e, exc_info=True)
            if (200 <= check_http_status(driver) < 300):
                jdg = False
                save_flag = True
                break
            else:
                jdg = False
                save_flag = True  
                logging.critical("extract_table_data, if-else, an error message:\n %s\n", e) 
                break
    return temp_webtable_df, jdg, save_flag


def navigate_to_next_page(driver, page_id, jdg, save_flag):
    trblflag=0
    while (True):
        try: 
            driver.find_element('id', page_id).click()
            break
        except Exception as e:
            logging.error("navigate_to_next_page, an error message: \n%s\n", e, exc_info=True)
            if (200 <= check_http_status(driver) < 300):
                jdg = False
                save_flag = True
                break
            else:
                trblflag, jdg, save_flag = handle_navigation_issue(trblflag, driver, jdg, save_flag)
                break
    return jdg, save_flag


def handle_navigation_issue(trblflag, driver, jdg, save_flag):
    if trblflag==0:
        driver.back()
        trblflag+=1
        return trblflag, jdg, save_flag    
    if trblflag==1:
        driver.refresh()
        trblflag+=1
        return trblflag, jdg, save_flag
    if trblflag==2:
        jdg = False
        save_flag = True
        return trblflag, jdg, save_flag        


def append_dataframe(original_df, new_df):
    return pd.concat([original_df, new_df], ignore_index=True)


def check_http_status(driver):
    try:
        current_url = driver.current_url
        response = requests.get(current_url)
        logging.debug("http status: %d", response.status_code)
        return int(response.status_code)
    except:
        return 0
        
def log_heading(heading):
    logging.info("\n----- {} -----\n".format(heading))


def save_dataframe(webtable_df, target_page, date_range):
    if target_page=="worker_dispatching" and date_range=="recent": csv_file_name = "【サンプル】労働者派遣事業_直近２ヶ月分" 
    elif target_page=="worker_dispatching" and date_range=="entire": csv_file_name = "【サンプル】労働者派遣事業_全期間"
    elif target_page=="employment_placement" and date_range=="recent": csv_file_name = "【サンプル】職業紹介事業_直近２ヶ月分"
    elif target_page=="employment_placement" and date_range=="entire": csv_file_name = "【サンプル】職業紹介事業_全期間"
    else:  csv_file_name = "【サンプル】test"
    webtable_df.to_csv(csv_file_name + ".csv")


def modify_dataframe(driver, webtable_df, target_page):
    columns_with_slash = []
    columns_with_slash = get_columns_w_slash(webtable_df, target_page)
    webtable_df = modify_values(driver, target_page, columns_with_slash, webtable_df)
    return webtable_df

def get_columns_w_slash(webtable_df, target_page):
    if target_page=="worker_dispatching":
        for index, item in enumerate(webtable_df.columns):
            columns_with_slash = [(index, col) for index, col in enumerate(webtable_df) if '／' in col]
    elif target_page=="employment_placement":
        for index, item in enumerate(webtable_df.columns):
            columns_with_slash = [(index, col) for index, col in enumerate(webtable_df) if '／' in col[0]]  
    return columns_with_slash


def modify_values(driver, target_page, columns_with_slash, webtable_df):
    xpathes = pd.DataFrame({
        'left_clmn': ['//*[@id="ID_lbKyokatodokedeNo"]', '//*[@id="ID_lbJigyonushiName"]', '//*[@id="ID_lbJigyoshoAddress"]'],
        'right_clmn': ['//*[@id="ID_lbKyokatodokedeDate"]', '//*[@id="ID_lbJigyoshoName"]', '//*[@id="ID_lbTel"]']
    })
    for i in range(0, len(columns_with_slash)):
        column_name = columns_with_slash[i][1]
        column_index = columns_with_slash[i][0]
        if target_page=="worker_dispatching": new_column_names = columns_with_slash[i][1].split('／')
        if target_page=="employment_placement": new_column_names = columns_with_slash[i][1][0].split('／')
        webtable_df = webtable_df.drop(column_name, axis=1)
        for index, clmn in enumerate(xpathes.columns):
            elements = driver.find_elements('xpath', xpathes[clmn][i])
            def process_element(element):
                return {
                    'Text': element.text
                }
            with ThreadPoolExecutor(max_workers=1) as executor:
                data_list = list(executor.map(process_element, elements))
            clmn_value = pd.DataFrame(data_list)
            webtable_df.insert(column_index+index+i, new_column_names[index], clmn_value)
    return webtable_df


def main(target_page, date_range):
    count=0; jdg=True; save_flag=False
    logging.basicConfig(filename="program_scrape.log", format="%(asctime)s:%(levelname)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('selenium').setLevel(logging.CRITICAL + 1)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL + 1)
    logging.getLogger('requests').setLevel(logging.CRITICAL + 1)

    log_heading("Start of Program")
    logging.info("%s - %s", target_page, date_range)
    logging.info("%d page", count+1)
    
    driver = setup_driver()
    navigate_to_initial_page(driver, target_page, date_range)

    webtable_df, jdg, save_flag = extract_table_data(driver, jdg, save_flag)
    webtable_df = modify_dataframe(driver, webtable_df, target_page)

    """
    ループの回数調整中
    """
    try:
        while (jdg):
            number_of_pages = len(driver.find_element('xpath', '/html/body/form/div[2]/div[3]/div/table/tbody/tr[6]/td/table[2]/tbody/tr/td/table').find_elements(By.TAG_NAME, 'a'))
            print("/nnumber_of_pages: ", number_of_pages)
            for i in range(2, number_of_pages+2):
                count+=1
                logging.info("%d page", count+1)
                print("count : i", count, i)
                if i<10: i=('0' + str(i))
                page_id = ("ID_pager" + str(i))
                jdg, save_flag = navigate_to_next_page(driver, page_id, jdg, save_flag)
                temp_webtable_df, jdg, save_flag = extract_table_data(driver, jdg, save_flag)
                temp_webtable_df = modify_dataframe(driver, temp_webtable_df, target_page)
                webtable_df = append_dataframe(webtable_df, temp_webtable_df)
        
                if i==10: save_dataframe(webtable_df, target_page, date_range); logging.info("Periodic file save (per 10 pages).")
                if not jdg and save_flag: save_dataframe(webtable_df, target_page, date_range); logging.info("Done all process. Exit"); break
    except Exception as e:
        logging.critical(e, exc_info=True)
        logging.critical("Unexpected error occurs at page %d. Save %d records to a csv file. Here is an error message: \n%s", count+1, len(webtable_df), e, exc_info=True)
        save_dataframe(webtable_df, target_page, date_range)
    log_heading('End of Program')
    # driver.quit()


main("worker_dispatching", "recent")