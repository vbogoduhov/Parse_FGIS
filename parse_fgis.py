import time
import datetime
import argparse
import app_logger
from work_db import WorkDb
import fgis_api

logger = app_logger.get_logger(__name__, 'parse_log.log')
database = WorkDb(database='fgis',
                  user='postgres',
                  password='postgres',
                  port='5432',
                  host='10.48.153.106')

def write_on_database(dict_for_write):
    """
    Метод для записи данных в БД
    :param dict_wor_write: отформатированный словарь
    :return:
    """
    if not database.check_tbmetrology_value(dict_for_write):
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
        if not database.check_value(type_number, mode='type_number'):
            database.set_id([type_title, type_number], mode="type_number")
        type_id = database.get_id([type_title, type_number], mode="type")
        # type_number = database.get_id(type_number, mode="type_number")

        if not database.check_value(name_org, mode="name_org"):
            database.set_id(name_org, mode="name_org")
        name_org = database.get_id(name_org, mode="name_org")

        dict_for_write['title'] = title
        dict_for_write['modification'] = modification
        dict_for_write['mitype'] = type_id
        dict_for_write['mitnumber'] = type_id
        dict_for_write['org_title'] = name_org

        database.write_metrology(dict_for_write)

def get_data_from_fgis(dict_filter):
    """
    Метод для получения данных из БД ФГИС по номеру СИ и году поверки

    :param year: год поверки
    :param minumber: номер СИ
    :return:
    """

    result_items = fgis_api.request_fgis(dict_filter)
    if len(result_items) > 0:
        print(f"Ok, номер СИ -- {dict_filter['filter_minumber']}")
        for item in result_items:
            dict_for_write = format_dict_for_write(item)
            try:
                # Запись в БД
                write_on_database(dict_for_write)
            except Exception as err:
                logger.warning(f"{err.__str__()}")
                logger.warning(f"Не удалось записать данные в БД <{str(dict_for_write)}>")
        print(f"Обработан СИ с номером - {dict_filter['filter_minumber']}")
    else:
        logger.warning(f"Не удалось выполнить запрос к fgis.ru.")

# def parse_response(resp_json, url, ref_url):
#     """
#     Метод парсинга ответа от fgis.ru
#
#     :param resp: объект response
#     :param url: url запроса, по которому получен response
#     :param ref_url: referer запроса, по которому получен response
#     :return: bool True or False
#     """
#
#     # database = get_database()
#     # Общее число найденных записей
#     num_found_result = int(resp_json.get('response').get('numFound'))
#     if  num_found_result > 20:
#         pages = num_found_result // 20
#         count = 1
#         url = url[:-1]
#         rows_on_page = 20
#         start_rows = 20
#         for item in resp_json.get('response').get('docs'):
#             dict_for_write = format_dict_for_write(item)
#             try:
#                 # Запись в БД
#                 # database.write_on_db(dict_for_write)
#                 write_on_database(dict_for_write)
#             except:
#                 logger.warning("Не удалось записать данные в БД")
#         while count <= pages:
#             headers['Referer'] = ref_url + '&page=' + str(count+1)
#             while True:
#                 temp_resp = requests.get(url+str(start_rows), headers=headers, cookies=cookies)
#                 if temp_resp.status_code == 200:
#                     print("Ок, запрос успешен!")
#                     response = temp_resp.json()
#                     print(f"Прерываем цикл, страница №{count}")
#                     break
#                 else:
#                     time.sleep(3)
#             for item in response.get('response').get('docs'):
#                 dict_for_write = format_dict_for_write(item)
#                 try:
#                     # Запись в БД
#                     # database.write_on_db(dict_for_write)
#                     write_on_database(dict_for_write)
#                 except:
#                     logger.warning("Не удалось записать данные в БД")
#             count += 1
#             start_rows += rows_on_page
#     elif num_found_result > 0 and num_found_result < 20:
#         dict_for_write = {}
#         for item in resp_json.get('response').get('docs'):
#             dict_for_write = format_dict_for_write(item)
#             try:
#                 # Запись в БД
#                 # database.write_on_db(dict_for_write)
#                 write_on_database(dict_for_write)
#             except:
#                 logger.warning("Не удалось записать данные в БД")
#     elif num_found_result == 0:
#         logger.info(f"Общее число найденных записей = {num_found_result}")

def format_dict_for_write(source_dict):
    res_dict = {}
    href = "https://fgis.gost.ru/fundmetrology/cm/results/"
    res_dict['mitnumber'] = source_dict['mit_number'] if 'mit_number' in source_dict else "None"
    res_dict['modification'] = source_dict['mi_modification'].encode().decode('utf-8', 'ignore') if 'mi_modification' in source_dict else "None"
    res_dict['si_number'] = source_dict['mi_number'].encode().decode('utf-8', 'ignore') if 'mi_number' in source_dict else "None"
    res_dict['valid_date'] = source_dict['valid_date'] if 'valid_date' in source_dict else "None"
    res_dict['docnum'] = source_dict['result_docnum'] if 'result_docnum' in source_dict else "None"
    res_dict['mitype'] = source_dict['mit_notation'].encode().decode('utf-8', 'ignore') if 'mit_notation' in source_dict else "None"
    res_dict['title'] = source_dict['mit_title'] if 'mit_title' in source_dict else "None"
    res_dict['org_title'] = source_dict['org_title'] if 'org_title' in source_dict else "None"
    res_dict['applicability'] = source_dict['applicability'] if 'applicability' in source_dict else "None"
    res_dict['vri_id'] = source_dict['vri_id'] if 'vri_id' in source_dict else "None"
    res_dict['verif_date'] = source_dict['verification_date'] if 'verification_date' in source_dict else "None"
    res_dict['href'] = href + source_dict['vri_id'] if 'vri_id' in source_dict else "None"

    return res_dict

# def create_parse_arg():
#     """
#     Парсер для параметров командной строки,
#     разбор переданных параметров запуска
#     """
#
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-y', '--years', type=int, default=datetime.datetime.date().year, help='Год поверки СИ для выборки')
#     parser.add_argument('-t', '--title', type=str, default=None,
#                         help='Наименование типа СИ для выборки')
#     parser.add_argument('-n', '--number', type=str, default=None, help='Заводской номер СИ для выборки')
#     parser.add_argument('-r', '--rows', type=int, default=20,
#                         help='Количество строк на страницу')
#
#     return parser

# def format_url(dict_filter, start=0, pages=0):
#     """
#     Форматирование URL на основе переданных условий
#
#     :param dict_filter: словарь с условиями для форматирования URL
#     :return: строка URL
#     """
#     filter_mititle = ""
#     mitype = ""
#     ref_mitype = ""
#     year = None
#     number = None
#     rows = None
#     ref_mititle = None
#     ref_number = None
#
#
#     if 'filter_mititle' in dict_filter:
#         if dict_filter['filter_mititle'] != None:
#             if len(dict_filter['filter_mititle'].split(sep=" ")) > 1:
#                 for item in dict_filter['filter_mititle'].split(sep=" "):
#                     filter_mititle += dict_url['filter_mititle']+quote(item)+'*'
#             else:
#                 filter_mititle = dict_url['filter_mititle']+quote(dict_filter['filter_mititle'])+'*'
#             ref_mititle = dict_url['filter_ref_mititle']+quote(dict_filter['filter_mititle'])
#
#     year = dict_filter['verification_year']
#     if 'filte_mitype' in dict_filter and dict_filter['filter_mitype'] != None:
#         mitype = dict_url['filter_mitype'] + quote(dict_filter['filter_mitype'])+'*'
#         ref_mitype = dict_url['filter_ref_mitype'] + quote(dict_filter['filter_mitype'])
#     if 'filter_minumber' in dict_filter and dict_filter['filter_minumber'] != None:
#         number = dict_url['filter_minumber']+quote(dict_filter['filter_minumber'])
#         ref_number = dict_url['filter_ref_minumber']+quote(dict_filter['filter_minumber'])
#
#     rows = dict_url['rows_on_page']+dict_filter['rows']
#     start_row = dict_url['start_row']+str(start)
#     url = dict_url['basic_url']+year+filter_mititle+mitype+number+dict_url['other_part_url']+rows+start_row
#     logger.info(f"Строка URL для запроса: {url}")
#     ref_url = dict_url['basic_ref']+ref_mititle+ref_mitype+ref_number
#     logger.info(f"Строка ref_url для запроса: {ref_url}")
#
#     return url, ref_url

def main():
    pass


if __name__ == "__main__":
    # # Парсинг параметров командной строки
    # arg_parser = create_parse_arg()
    # namespace_arg = arg_parser.parse_args(sys.argv[1:])
    #
    # mi_title = namespace_arg.title
    # verif_year = namespace_arg.years
    # mi_number = namespace_arg.number
    # rows_on_page = namespace_arg.rows
    # dict_filter = {
    #     'filter_mititle': mi_title,
    #     'verification_year': verif_year,
    #     'filter_minumber': mi_number,
    #     'rows': rows_on_page
    # }
    main()