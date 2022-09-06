import requests
import os
import json
from urllib.parse import unquote, quote
import fgisdb
import time
from fake_useragent import UserAgent
import datetime
import argparse
import app_logger
import json
import temp_database
from work_db import WorkDb

logger = app_logger.get_logger(__name__)
database = WorkDb(database='fgis',
                  user='postgres',
                  password='postgres',
                  port='5432',
                  host='10.48.153.106')
# Для запроса requests
dict_url = {
        'basic_url': 'https://fgis.gost.ru/fundmetrology/cm/xcdb/vri/select?fq=verification_year:',
        'filter_mititle': '&fq=mi.mititle:*',
        'filter_minumber': '&fq=mi.number:*',
        'other_part_url': '*&q=*&fl=vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,verification_date,valid_date,applicability,result_docnum,sticker_num&sort=verification_date+desc,org_title+asc',
        'rows_on_page': '&rows=',
        'start_row': '&start=',
        'basic_ref': 'https://fgis.gost.ru/fundmetrology/cm/results?',
        'filter_ref_mititle': 'filter_mi_mititle=',
        'filter_ref_minumber': '&filter_mi_number=',
        'filter_ref_page': '&page=',
        'filter_ref_rows': '&rows=',
        'filter_ref_activeyear': '&activeYear='
    }

cookies = {
        'session-cookie': '170339c22405c67b3851763e04983c4726a2fe0be9daf3435a9fef3519fdb6c2c1fdfe08a65ab958b465d9832be4dc6f',
    }

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': f'https://fgis.gost.ru/fundmetrology/cm/results?filter_mi_mititle={quote("трансформатор")}',
        # 'Cookie': 'session-cookie=170339c22405c67b3851763e04983c4726a2fe0be9daf3435a9fef3519fdb6c2c1fdfe08a65ab958b465d9832be4dc6f',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

def write_on_database(dict_for_write):
    """
    Метод для записи данных в БД
    :param dict_wor_write: отформатированный словарь
    :return:
    """
    logger.info(f"Попытка записи данных в БД")
    title = dict_for_write['title']
    modification = dict_for_write['modification']
    type_title = dict_for_write['mitype']
    type_number = dict_for_write['mitnumber']
    name_org = dict_for_write['org_title']

    if not database.check_value(title, mode="title"):
        database.set_id(title, mode="title")
    title = database.get_id(title, mode="title")

    if not database.check_value(modification, mode="modification"):
        database.set_id(modification, mode="modification")
    modification = database.get_id(modification, mode="modification")

    if not database.check_value(type_title, "type_title"):
        database.set_id([type_title, type_number], mode="type_title")
    type_title = database.get_id(type_title, mode="type_title")
    type_number = database.get_id(type_number, mode="type_number")

    if not database.check_value(name_org, mode="name_org"):
        database.set_id(name_org, mode="name_org")
    name_org = database.get_id(name_org, mode="name_org")

    dict_for_write['title'] = title
    dict_for_write['modification'] = modification
    dict_for_write['mitype'] = type_title
    dict_for_write['mitnumber'] = type_number
    dict_for_write['org_title'] = name_org

    database.write_metrology(dict_for_write)

def get_data_from_fgis(dict_filter):
    """
    Метод для получения данных из БД ФГИС по номеру СИ и году поверки

    :param year: год поверки
    :param minumber: номер СИ
    :return:
    """

    url, ref_url = format_url(dict_filter)
    headers['Referer'] = ref_url
    response = requests.get(url, cookies=cookies, headers=headers)
    if response.status_code == 200:
        print(f"Ok, номер СИ -- {dict_filter['filter_minumber']}")
        parse_response(response.json(), url, ref_url)
        print(f"Обработан СИ с номером - {dict_filter['filter_minumber']}")
    else:
        logger.warning(f"Не удалось выполнить запрос к fgis.ru, status = {response.status_code}, url: <{url}>")

def parse_response(resp_json, url, ref_url):
    """
    Метод парсинга ответа от fgis.ru

    :param resp: объект response
    :param url: url запроса, по которому получен response
    :param ref_url: referer запроса, по которому получен response
    :return: bool True or False
    """

    # database = get_database()
    # Общее число найденных записей
    num_found_result = int(resp_json.get('response').get('numFound'))
    if  num_found_result > 20:
        pages = num_found_result // 20
        count = 1
        url = url[:-1]
        rows_on_page = 20
        start_rows = 20
        for item in resp_json.get('response').get('docs'):
            dict_for_write = format_dict_for_write(item)
            try:
                # Запись в БД
                # database.write_on_db(dict_for_write)
                write_on_database(dict_for_write)
            except:
                logger.warning("Не удалось записать данныев БД")
        while count <= pages:
            headers['Referer'] = ref_url + '&page=' + str(count+1)
            while True:
                temp_resp = requests.get(url+str(start_rows), headers=headers, cookies=cookies)
                if temp_resp.status_code == 200:
                    print("Ок, запрос успешен!")
                    response = temp_resp.json()
                    break
                else:
                    time.sleep(3)
            for item in response.get('response').get('docs'):
                dict_for_write = format_dict_for_write(item)
                try:
                    # Запись в БД
                    # database.write_on_db(dict_for_write)
                    write_on_database(dict_for_write)
                except:
                    logger.warning("Не удалось записать данныев БД")
            count += 1
            start_rows += rows_on_page
    elif num_found_result > 0 and num_found_result < 20:
        dict_for_write = {}
        for item in resp_json.get('response').get('docs'):
            dict_for_write = format_dict_for_write(item)
            try:
                # Запись в БД
                # database.write_on_db(dict_for_write)
                write_on_database(dict_for_write)
            except:
                logger.warning("Не удалось записать данные в БД")
    elif num_found_result == 0:
        logger.info(f"Общее число найденных записей = {num_found_result}")

def format_dict_for_write(source_dict):
    res_dict = {}
    href = "https://fgis.gost.ru/fundmetrology/cm/results/"
    res_dict['mitnumber'] = source_dict['mi.mitnumber'] if 'mi.mitnumber' in source_dict else "None"
    res_dict['modification'] = source_dict['mi.modification'] if 'mi.modification' in source_dict else "None"
    res_dict['si_number'] = source_dict['mi.number'] if 'mi.number' in source_dict else "None"
    res_dict['valid_date'] = source_dict['valid_date'] if 'valid_date' in source_dict else "None"
    res_dict['docnum'] = source_dict['result_docnum'] if 'result_docnum' in source_dict else "None"
    res_dict['mitype'] = source_dict['mi.mitype'].encode().decode('utf-8', 'ignore') if 'mi.mitype' in source_dict else "None"
    res_dict['title'] = source_dict['mi.mititle'] if 'mi.mititle' in source_dict else "None"
    res_dict['org_title'] = source_dict['org_title'] if 'org_title' in source_dict else "None"
    res_dict['applicability'] = source_dict['applicability'] if 'applicability' in source_dict else "None"
    res_dict['vri_id'] = source_dict['vri_id'] if 'vri_id' in source_dict else "None"
    res_dict['verif_date'] = source_dict['verification_date'] if 'verification_date' in source_dict else "None"
    res_dict['href'] = href + source_dict['vri_id'] if 'vri_id' in source_dict else "None"

    return res_dict

def create_parse_arg():
    """
    Парсер для параметров командной строки,
    разбор переданных параметров запуска
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--years', type=int, default=datetime.datetime.date().year, help='Год поверки СИ для выборки')
    parser.add_argument('-t', '--title', type=str, default=None,
                        help='Наименование типа СИ для выборки')
    parser.add_argument('-n', '--number', type=str, default=None, help='Заводской номер СИ для выборки')
    parser.add_argument('-r', '--rows', type=int, default=20,
                        help='Количество строк на страницу')

    return parser

def create_source_json():
    namefile_db = 'dbfgis.db'
    dbconnect = fgisdb.create_db(namefile_db)
    fgisdb.create_table(dbconnect)
    cursor = dbconnect.cursor()

    cookies = {
        'session-cookie': '170339c22405c67b3851763e04983c4726a2fe0be9daf3435a9fef3519fdb6c2c1fdfe08a65ab958b465d9832be4dc6f',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': f'https://fgis.gost.ru/fundmetrology/cm/results?filter_mi_mititle={quote("трансформатор")}',
        # 'Cookie': 'session-cookie=170339c22405c67b3851763e04983c4726a2fe0be9daf3435a9fef3519fdb6c2c1fdfe08a65ab958b465d9832be4dc6f',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    ref = f'https://fgis.gost.ru/fundmetrology/cm/results?filter_mi_mititle={quote("трансформатор")}&rows=100'
    numpages = 1
    # response = requests.get(f'https://fgis.gost.ru/fundmetrology/cm/xcdb/vri/select?fq=verification_year:2022&fq=mi.mititle:*{quote("трансформатор")}*&q=*&fl=vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,verification_date,valid_date,applicability,result_docnum,sticker_num&sort=verification_date+desc,org_title+asc&rows=20&start=0', cookies=cookies, headers=headers)
    start_rows = 0
    # url = f'https://fgis.gost.ru/fundmetrology/cm/xcdb/vri/select?fq=verification_year:2022&fq=mi.mititle:*{quote("трансформатор")}*&q=*&fl=vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,verification_date,valid_date,applicability,result_docnum,sticker_num&sort=verification_date+desc,org_title+asc&rows=20&start={start_rows}'

    # response = requests.get(url, cookies=cookies, headers=headers).json()
    # num_found = response.get('response').get('numFound')
    # res_dict = {}
    href = "https://fgis.gost.ru/fundmetrology/cm/results/"
    count = 1
    # for item in response.get('response').get('docs'):
    #     res_dict[item['mi.number']] = item
    #     print(item)
    # len(response.get('response').get('docs'))
    while True:
        dict_for_write = {}
        url = f'https://fgis.gost.ru/fundmetrology/cm/xcdb/vri/select?fq=verification_year:2022&fq=mi.mititle:*{quote("трансформатор")}*&q=*&fl=vri_id,org_title,mi.mitnumber,mi.mititle,mi.mitype,mi.modification,mi.number,verification_date,valid_date,applicability,result_docnum,sticker_num&sort=verification_date+desc,org_title+asc&rows=100&start={start_rows}'
        ref = f'https://fgis.gost.ru/fundmetrology/cm/results?filter_mi_mititle={quote("трансформатор")}'#+'&page='+str(numpages)+'&rows=20'
        # time.sleep(2)
        headers['Referer'] = ref if numpages == 1 else ref + '&page='+str(numpages)+'&rows=100'
        try:
            response = requests.get(url, cookies=cookies, headers=headers)
            cc = 0
            while cc < 50:
                if response.status_code == 429:
                    with open("log_file.log", 'a') as log:
                        log.write("Возникла ошибка запроса, статус запроса - 429\n")

                    headers['User-Agent'] = UserAgent().firefox
                    print("I go to sleep")
                    time.sleep(3601)
                    response = requests.get(url, cookies=cookies, headers=headers)
                    cc += 1
                else:
                    break
            response = response.json()
            for item in response.get('response').get('docs'):
                # res_dict[item['mi.number']] = item
                dict_for_write['mi_mitnumber'] = item['mi.mitnumber'] if 'mi.mitnumber' in item else "None"
                dict_for_write['mi_modification'] = item['mi.modification'] if 'mi.modification' in item else "None"
                dict_for_write['mi_number'] = item['mi.number'] if 'mi.number' in item else "None"
                dict_for_write['valid_date'] = item['valid_date'] if 'valid_date' in item else "None"
                dict_for_write['result_docnum'] = item['result_docnum'] if 'result_docnum' in item else "None"
                dict_for_write['mi_mitype'] = item['mi.mitype'] if 'mi.mitype' in item else "None"
                dict_for_write['mi_mititle'] = item['mi.mititle'] if 'mi.mititle' in item else "None"
                dict_for_write['org_title'] = item['org_title'] if 'org_title' in item else "None"
                dict_for_write['applicability'] = item['applicability'] if 'applicability' in item else "None"
                dict_for_write['vri_id'] = item['vri_id'] if 'vri_id' in item else "None"
                dict_for_write['verification_date'] = item['verification_date'] if 'verification_date' in item else "None"
                dict_for_write['href'] = href+item['vri_id'] if 'vri_id' in item else "None"
                fgisdb.write_on_db(dict_for_write, cursor)
                dbconnect.commit()
            if len(response.get('response').get('docs')) < 100:
                break
        except TimeoutError:
            print("Ошибка по таймауту")
        # except:
        #     time.sleep(15)
        print(f"Номер запроса - {count}")
        start_rows += 100
        numpages += 1
        count += 1
        time.sleep(1)

def result():
    url_res = "https://fgis.gost.ru/fundmetrology/cm/results/"
    temp_j = {}
    with open("fundmetrology.json", encoding="utf-8") as jfile:
        full_json = json.load(jfile)

    with open("res_json.json", "w", encoding="utf-8") as res_file:
        for key, value in full_json.items():
            if value == []:
                continue
            # for trans in value:
                # print(key, trans)
            mi_number = key
            org_title = value.get('org_title')
            hyper = url_res + value.get('vri_id')
            temp_j[mi_number] = {'Наименование СИ': value.get('mi.mititle'),
                                     'Тип СИ': value.get('mi.mitype'),
                                     'Модификация типа СИ': value.get('mi.modification'),
                                     'Поверитель': org_title,
                                    'Дата поверки': value.get('verification_date'),
                                     'Дата следующей поверки': value.get('valid_date'),
                                     'Свидетельство о поверке': value.get('result_docnum'),
                                    'Ссылка на карточку': hyper}
        json.dump(temp_j, res_file, indent=4, ensure_ascii=False)

def format_url(dict_filter, start=0, pages=0):
    """
    Форматирование URL на основе переданных условий

    :param dict_filter: словарь с условиями для форматирования URL
    :return: строка URL
    """
    filter_mititle = ""
    year = None
    number = None
    rows = None
    ref_mititle = None
    ref_number = None


    if 'filter_mititle' in dict_filter:
        if dict_filter['filter_mititle'] != None:
            if len(dict_filter['filter_mititle'].split(sep=" ")) > 1:
                for item in dict_filter['filter_mititle'].split(sep=" "):
                    filter_mititle += dict_url['filter_mititle']+quote(item)+'*'
            else:
                filter_mititle = dict_url['filter_mititle']+quote(dict_filter['filter_mititle'])+'*'
            ref_mititle = dict_url['filter_ref_mititle']+quote(dict_filter['filter_mititle'])

    year = dict_filter['verification_year']
    if 'filter_minumber' in dict_filter and dict_filter['filter_minumber'] != None:
        number = dict_url['filter_minumber']+dict_filter['filter_minumber']
        ref_number = dict_url['filter_ref_minumber']+dict_filter['filter_minumber']
    rows = dict_url['rows_on_page']+dict_filter['rows']
    start_row = dict_url['start_row']+str(start)
    url = dict_url['basic_url']+year+filter_mititle+number+dict_url['other_part_url']+rows+start_row
    logger.info(f"Строка URL для запроса: {url}")
    ref_url = dict_url['basic_ref']+ref_mititle+ref_number
    logger.info(f"Строка ref_url для запроса: {ref_url}")

    return url, ref_url

if __name__ == "__main__":
    # Парсинг параметров командной строки
    arg_parser = create_parse_arg()
    namespace_arg = arg_parser.parse_args(sys.argv[1:])

    mi_title = namespace_arg.title
    verif_year = namespace_arg.years
    mi_number = namespace_arg.number
    rows_on_page = namespace_arg.rows
    dict_filter = {
        'filter_mititle': mi_title,
        'verification_year': verif_year,
        'filter_minumber': mi_number,
        'rows': rows_on_page
    }

    create_source_json()
    # result()