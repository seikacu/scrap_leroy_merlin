import csv
import os

import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver

import platform
import time
import zipfile

from python_rucaptcha.image_captcha import ImageCaptcha
from python_rucaptcha.re_captcha import ReCaptcha
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as ex_cond

from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import secure
from db_sql import connect_db, check_exist_table, create_table_ads, check_url_in_bd, insert_url_table, \
    get_links_from_table, add_path_page

GLOB_ID = 0

# proxies = {'http': f'http://wkrHNdRr:AprXytnj@138.124.61.254:64654'}
proxies = {'http': f'http://NmkpYP:XPdvcY@5.8.61.240:8000'}

cookies = {
    '_slid_server': '',
    '_regionID': '34',
    'cookie_accepted': 'true',
    '_ym_uid': '1699188703410345003',
    '_ym_d': '1699188703',
    'GACookieStorage': 'undefined',
    'st_uid': '5256336dc03feb75152e3107acaef216',
    '_ym_isad': '2',
    'tmr_lvid': '9369dd6d258d8cf4ee92a4636250a3e6',
    'tmr_lvidTS': '1699188703990',
    'iap.uid': '3bc48a46ee3a4d178e200d40947099c2',
    'aplaut_distinct_id': 'WUuMuIGhc8uc',
    '___dmpkit___': '8fd78e10-2cc8-491d-b2a7-385ab5286930',
    'uxs_uid': '132ac0c0-7bda-11ee-9853-6bc32341f338',
    'adrdel': '1',
    'adrcid': 'AHsXIs7VFjJ0EdYZjg1tagQ',
    'rai': '82e0843e670cbdcb42949c6af8bd25ff',
    'sawOPH': 'true',
    'X-API-Experiments-sub': 'B',
    'user-geolocation': '0%2C0',
    'qrator_jsid': '1699194995.314.FGQ9FOcsUPIchVDG-om1q7pdcnj90p8gekton9gflsmjmhog8',
    'pageExperiments': 'srp_category_facet:B+apro_srp_firstPartyProducts:A',
    '_spx': 'eyJpZCI6IjUxYTAzMWYxLWJjNTAtNGY4YS1hNGM0LTcyZWEzMWI5ZmUxNyIsInNvdXJjZSI6IiIsImZpeGVkIjp7InN0YWNrIjpbMTUzMDkyMTE4MywxNTMwOTIxMTgyLDE1MzA5MjExODIsMTUzMDkyMTE4MiwxNTMwOTIxMTgyXX19',
    'tmr_detect': '0%7C1699201105564',
}

headers = {
    'authority': 'leroymerlin.ru',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    # 'cookie': '_slid_server=; _regionID=34; cookie_accepted=true; _ym_uid=1699188703410345003; _ym_d=1699188703; GACookieStorage=undefined; st_uid=5256336dc03feb75152e3107acaef216; _ym_isad=2; tmr_lvid=9369dd6d258d8cf4ee92a4636250a3e6; tmr_lvidTS=1699188703990; iap.uid=3bc48a46ee3a4d178e200d40947099c2; aplaut_distinct_id=WUuMuIGhc8uc; ___dmpkit___=8fd78e10-2cc8-491d-b2a7-385ab5286930; uxs_uid=132ac0c0-7bda-11ee-9853-6bc32341f338; adrdel=1; adrcid=AHsXIs7VFjJ0EdYZjg1tagQ; rai=82e0843e670cbdcb42949c6af8bd25ff; sawOPH=true; X-API-Experiments-sub=B; user-geolocation=0%2C0; qrator_jsid=1699194995.314.FGQ9FOcsUPIchVDG-om1q7pdcnj90p8gekton9gflsmjmhog8; pageExperiments=srp_category_facet:B+apro_srp_firstPartyProducts:A; _spx=eyJpZCI6IjUxYTAzMWYxLWJjNTAtNGY4YS1hNGM0LTcyZWEzMWI5ZmUxNyIsInNvdXJjZSI6IiIsImZpeGVkIjp7InN0YWNrIjpbMTUzMDkyMTE4MywxNTMwOTIxMTgyLDE1MzA5MjExODIsMTUzMDkyMTE4MiwxNTMwOTIxMTgyXX19; tmr_detect=0%7C1699201105564',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
}


# response = requests.get('https://leroymerlin.ru/', params=params, cookies=cookies, headers=headers, proxies=proxies)


def get_soup(url, mode):
    folder = ''
    file_name = ''
    if mode == 1:
        folder = 'data/main/'
        file_name = str(url.split('/')[-1:][0])
    elif mode == 2:
        folder = 'data/sub/'
        file_name = str(url.split('/')[-2:][0])
    elif mode == 3:
        folder = 'data/num/'
        file_name = 'num'

    r = requests.get(url=url, headers=headers, cookies=cookies, proxies=proxies, allow_redirects=False)

    if not file_name.endswith(".html"):
        file_name += ".html"

    # if not os.path.exists("data"):
    #     os.mkdir("data")
    with open(f"{folder}{file_name}", "w", encoding="utf-8") as file:
        file.write(r.text)
    with open(f"{folder}{file_name}", encoding="utf-8") as file:
        src = file.read()
    soup = BeautifulSoup(src, "lxml")
    return soup


def start():
    main_page = 'https://leroymerlin.ru'
    pages = 1
    for i in range(1, pages + 2):
        print(i)
        url = f'https://leroymerlin.ru/search/?q=UNIS&06575=UNIS&page={i}'
        soup = get_soup(url, 1)
        cards = soup.find_all('div', {'class': 'p155f0re_plp largeCard'})
        j = 0
        for card in cards:
            href = card.find_next('a').attrs.get('href')
            link = main_page + href
            time.sleep(3)
            get_soup(link, 2)
            print(f'i: {i}; j: {j}')
            j += 1


def set_driver_options(options):
    # безголовый режим браузера
    # options.add_argument('--headless=new')
    options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument(f"--user-data-dir={get_path_profile()}")


def get_path_profile():
    if platform.system() == "Windows":
        return r"C:\WebDriver\chromedriver\user"
    elif platform.system() == "Linux":
        return "/home/seikacu/webdriver/user"
    elif platform.system() == "Darwin":
        return "webdriver/chromedriver-macos/user"
    else:
        raise Exception("selen_get_path_profile_Unsupported platform!")


def get_selenium_driver(use_proxy, num_proxy):
    options = webdriver.ChromeOptions()
    # proxy = 'NmkpYP:XPdvcY@5.8.61.240:8000'
    # options.add_argument(f'--proxy-server=socks5://{proxy}')
    # options.ignore_local_proxy_environment_variables()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.debugger_address = 'localhost:9222'

    # set_driver_options(options)

    if use_proxy:
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        plugin_file = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(plugin_file, 'w') as zp:
            zp.writestr('manifest.json', secure.get_proxy_pref(num_proxy, 0))
            zp.writestr('background.js', secure.get_proxy_pref(num_proxy, 1))

        options.add_extension(plugin_file)

    user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/116.0.5845.967 YaBrowser/23.9.1.967 Yowser/2.5 Safari/537.36")
    options.add_argument(f'--user-agent={user_agent}')

    caps = DesiredCapabilities().CHROME
    caps['pageLoadStrategy'] = 'normal'

    service = Service(ChromeDriverManager().install(), desired_capabilities=caps)
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def get_nums_pages(url):
    soup = get_soup(url, 3)
    pass


def get_data(driver: webdriver.Chrome, connection):
    try:
        pages = 1
        with open('data/links.csv', newline='') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                launch = row[0]
                url = row[1]
                driver.get(url)
                time.sleep(1)
                nums = driver.find_elements(By.XPATH, "//a[contains(@class,'o1ojzgcq_plp')]")
                count = 1
                if nums:
                    len_nums = nums.__len__()
                    item = nums.__getitem__(len_nums - 1)
                    count = int(item.text)
                for i in range(1, pages + count):
                    print(i)
                    # url = f'https://leroymerlin.ru/search/?q=UNIS&06575=UNIS&page={i}' o1ojzgcq_plp
                    if i == 1:
                        if url.endswith('/'):
                            url = f'{url}?page={i}'
                        else:
                            url = f'{url}&page={i}'
                    elif i > 1:
                        url_split = url.split('=')
                        url_split_len = url_split.__len__()
                        last = url_split[url_split_len - 1]
                        len_cut = last.__len__()
                        url = f'{url[:-len_cut]}{i}'
                    driver.get(url)
                    time.sleep(1)
                    hrefs = driver.find_elements(By.XPATH, "//a[contains(@class,'ihytpj4_plp')]")
                    j = 0
                    for href in hrefs:
                        link = href.get_attribute('href')
                        if check_url_in_bd(connection, link):
                            continue
                        insert_url_table(connection, link, launch)
                        print(f'i: {i}; j: {j}; link: {link}')
                        j += 1
    except NoSuchElementException as ex:
        print(ex)
        pass


def get_links():
    connection = None
    driver = None
    try:
        connection = connect_db()
        connection.autocommit = True
        if not check_exist_table(connection):
            create_table_ads(connection)
        driver = get_selenium_driver(False, GLOB_ID)
        get_data(driver, connection)
    except IndexError as ierr:
        print("YAAAAAAAA")
        # log.write_log("IndexError", ierr)
        # log.write_log("IndexError. data = ", data)
        # log.write_log(f"IndexError. sel = {sel}, id_bd = {id_bd}, url = {url}, row = {row}. ", "INDEX ERROR END")
    except Exception as _ex:
        print("tk_clicked_get_phone_ Error while working with PostgreSQL", _ex)
        # log.write_log("tk_clicked_get_phone_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if driver:
            # driver.close()
            # driver.quit()
            print("[INFO] Selen driver closed")
        if connection:
            connection.close()
            print("[INFO] Сбор ссылок завершен")


def save_links_data(connection, driver: webdriver.Chrome, id_db, url):
    driver.get(url)
    time.sleep(2)
    while True:
        try:
            err = driver.find_element(By.XPATH, "//span[contains(@class,'e1t81b4d_static-pages')]")
            if err:
                err_text = err.text
                if 'Что-то пошло не так' in err_text:
                    time.sleep(60)
                    driver.refresh()
                    time.sleep(5)
            else:
                break
        except NoSuchElementException:
            # print(ex)
            break

    data_page = driver.page_source
    folder = 'data/sub/'
    file_name = str(url.split('/')[-2:][0])
    if not file_name.endswith(".html"):
        file_name += ".html"
    with open(f"{folder}{file_name}", "w", encoding="utf-8") as file:
        file.write(data_page)
    add_path_page(connection, id_db, f'{folder}{file_name}')


def get_links_source():
    connection = None
    driver = None
    try:
        connection = connect_db()
        connection.autocommit = True

        driver = get_selenium_driver(False, GLOB_ID)

        links = get_links_from_table(connection)
        count = 0
        for link in links:
            id_db = link[0]
            url = link[1]

            save_links_data(connection, driver, id_db, url)
            count += 1
            if count == 50:
                count = 0
                time.sleep(30)
                driver.refresh()
                time.sleep(15)


    except IndexError as ierr:
        print("YAAAAAAAA")
        # log.write_log("IndexError", ierr)
        # log.write_log("IndexError. data = ", data)
        # log.write_log(f"IndexError. sel = {sel}, id_bd = {id_bd}, url = {url}, row = {row}. ", "INDEX ERROR END")
    except Exception as _ex:
        print("tk_clicked_get_phone_ Error while working with PostgreSQL", _ex)
        # log.write_log("tk_clicked_get_phone_ Error while working with PostgreSQL", _ex)
        pass
    finally:
        if driver:
            # driver.close()
            # driver.quit()
            print("[INFO] Selen driver closed")
        if connection:
            connection.close()
            print("[INFO] Сохранение url-ов на диск заверщено")


def main():
    # get_links()
    get_links_source()


if __name__ == '__main__':
    main()
