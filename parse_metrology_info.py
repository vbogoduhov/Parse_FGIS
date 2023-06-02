#!/usr/bin/python3
"""
Основной скрипт для запуска.
Логика взаимодействия остальных модулей описана здесь.

У скрипта несколько режимов работы:
    1. Получить информацию по СИ из БД ФГИС по конкретному году
    поверки, если год известен. Записать полученную информацию
    в локальную БД.
    2. Работа с локальной БД, чтобы получить ссылки на карточку СИ в ФГИС
    с информацией о поверке. Работа по конкретному году.
    3. Если год очередной поверки неизвестен с даты последней поверки,
    то перебираем по каждому СИ годы поверки с года последней поверки до
    текущего года.

    Во всех случаях при запуске скрипта желательно указать
    вид типа СИ (ТТ, ТН, ПУ) для работы. В противном случае проверяться будут
    все виды СИ - ТТ, ТН и ПУ.
    Если при запуске скрипта не указан конкретный год и указан режим работы с БД ФГИС,
    то по СИ будет предпринята попытка получить информацию из ФГИС по всем годам,
    начиная с года последней поверки и до текущего года.

    Предполагается, что структура файла Excel не меняется,
    всегда остаётся постоянной, потому что номера столбцов прописаны в виде констант.
"""

# Блок импорта
import sys
import os
import argparse

import requests

import app_logger
import fgis_eapi
import localdb
import xlsx
from datetime import datetime, date
import re
from parse_type_si import TypeParseSi
from progress.bar import IncrementalBar
import configparser

# Конец блока импорта

# Логгер
logger = app_logger.get_logger(__name__, 'metrology_info_log.log')

# Константы
# ================================== #
COLUMNS_SI = {
    'ПУ': {
        'type': 8,
        'serial': 9,
        'verif_date': 12,
        'valid_date': 13,
        'href': 14
    },
    'ТТ': {
        'type': 15,
        'serial': 17,
        'verif_date': 20,
        'valid_date': 21,
        'href': 22
    },
    'ТН': {
        'type': 23,
        'serial': 25,
        'verif_date': 28,
        'valid_date': 29,
        'href': 30
    }
}
COLUMN_ID = 36

EXC_STR = ['',
           '-',
           '--',
           '---',
           'не пригоден',
           'н/д',
           'нет данных',
           'отсутствует',
           None]


# ================================== #

def parse_args():
    """
    Парсер для параметров командной строки,
    разбор переданных параметров запуска

    :return: объект parser
    """
    logger.info("Парсинг параметров командной строки")
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--serial', type=str, default='', help='Конкретный номер СИ для поиска в БД FGIS')
    parser.add_argument('-n', '--namefile', type=str,
                        default="Перечень ТУ АСКУЭ ООСС АСКУЭ Приложения к СТО  2023г.  итог 2 кв..xlsx",
                        help='Имя файла Excel для обработки')
    parser.add_argument('-y', '--years', type=int, default=0,
                        help='Год поверки СИ для выборки')
    parser.add_argument('-k', '--keyword', type=str, default='ТТ ТН',
                        help="СИ по которому нужно получить данные из ФГИС или локальной БД: "
                             "ПУ - приборы учёта, ТТ - трансформаторы тока, ТН - трансформаторы напряжения."
                             "Можно вводить несколько, в таком случае значения должны быть разделена пробелом,"
                             "например: <ТТ ТН> или <ТН ПУ ТТ>")
    parser.add_argument('-m', '--mode', type=str, default='unknow', help="Режим запуска скрипта: "
                                                                       "fgis - для сбора данных из БД ФГИС,"
                                                                       "local - для локальной работы и заполнения файла Excel,"
                                                                       "unknow - когда неизвестен год последней поверки, и нужно проверить СИ"
                                                                       "по годам, начиная с известного года последней поверки,"
                                                                       "change_serial - для добавления 0 вначале номера для ЕвроАльфа")
    parser.add_argument('--START', type=int, default=13, help='Начальная строка')
    parser.add_argument('-cfg', '--setfile', type=str, default='settings.ini',
                        help='Файл с настройками подключения к локальной БД')
    logger.info("Парсинг параметров командной строки закончен")

    return parser


def read_settings_file(namefile):
    """
    Функция для чтения файла настроек поключения к БД АЦ
    и парсинга

    :param namefile: имя файла для чтения
    :return: словарь с данными подключения к локальной БД
    """
    if os.path.isfile(namefile):
        parser = configparser.ConfigParser()
        parser.read(namefile)
        dict_settings = {'host': parser.get('DB', 'host'),
                         'database': parser.get('DB', 'database'),
                         'user': parser.get('DB', 'user'),
                         'password': parser.get('DB', 'password'),
                         'port': parser.get('DB', 'port')}

        return dict_settings
    else:
        logger.warning(f"Не удалось считать параметры подключения к БД из файла <{namefile}>."
                       f"Возможной файла не существует или повреждена структура файла.")
        print(f"Проверь существование файла <{namefile}>. И структуру данных в файле")


def check_mode(mode: str):
    """
    Проверка выбранного режима работы скрипта,
    и запуск соответствующей функции для дальнейшей работы.

    :param mode: режим работы скрипта.
    :return: функция для дальнейшей работы.
    """
    pass


def format_dict_for_write(source_dict: dict, row_number: int):
    """
    Функция для форматирования словаря с данными и подготовкой их к записи
    в локальную БД

    :param source_dict: словарь исходных данных
    :return: словарь с данными для записи
    """
    res_dict = {}
    href = "https://fgis.gost.ru/fundmetrology/cm/results/"
    res_dict['mitnumber'] = source_dict['mit_number'] if 'mit_number' in source_dict else "None"
    res_dict['modification'] = source_dict['mi_modification'].encode().decode('utf-8',
                                                                              'ignore') if 'mi_modification' in source_dict else "None"
    res_dict['si_number'] = source_dict['mi_number'].encode().decode('utf-8',
                                                                     'ignore') if 'mi_number' in source_dict else "None"
    res_dict['valid_date'] = source_dict['valid_date'] if 'valid_date' in source_dict else "None"
    res_dict['docnum'] = source_dict['result_docnum'] if 'result_docnum' in source_dict else "None"
    res_dict['mitype'] = source_dict['mit_notation'].encode().decode('utf-8',
                                                                     'ignore') if 'mit_notation' in source_dict else "None"
    res_dict['title'] = source_dict['mit_title'] if 'mit_title' in source_dict else "None"
    res_dict['org_title'] = source_dict['org_title'] if 'org_title' in source_dict else "None"
    res_dict['applicability'] = source_dict['applicability'] if 'applicability' in source_dict else "None"
    res_dict['vri_id'] = source_dict['vri_id'] if 'vri_id' in source_dict else "None"
    res_dict['verif_date'] = source_dict['verification_date'] if 'verification_date' in source_dict else "None"
    res_dict['href'] = href + source_dict['vri_id'] if 'vri_id' in source_dict else "None"
    res_dict['change_date'] = datetime.now().date().strftime("%d.%m.%Y")
    res_dict['change_flag'] = 1
    res_dict['rows_number'] = row_number

    return res_dict


def format_dict_requests(title="", verif_year=datetime.today().year, mitype=None, number="", rows=str(100)):
    """
    Формируем словарь с данными для запроса в БД ФГИС

    :param title: наименование типа СИ, часть или полное наименование
    :param verif_year: год последней поверки, для выборки из ФГИС
    :param number: заводской номер СИ для выборки из ФГИС
    :param rows: количество строк в запросе
    :return: dict_requests - сформированный словарь для запроса
    """
    if title == "ПУ":
        filter_mititle = "Счетчик*электрической*%20"
    elif title == "ТТ":
        filter_mititle = "Трансформаторы*тока*%20"
    elif title == "ТН":
        filter_mititle = "Трансформаторы*напряжения*%20"

    dict_request = {
        'filter_mititle': filter_mititle,
        'verification_year': str(verif_year),
        'filter_minumber': str(number),
        'filter_mitype': mitype,
        'rows': rows
    }

    return dict_request


def parse_file(sheet, keyword: str, year: int, mode: str, serial: str):
    """
    Парсинг файла Excel и обработка перечня СИ в соответствии с
    заданным режимом работы

    :param sheet: объект worksheet
    :param keyword: вид типа СИ, по которому вести обработку файла
    :param year: год поверки, по которому искать информацию
    :param mode: режим работы скрипта
    :param serial: серийный номер СИ, для поиска конкретного СИ
    :return:
    """
    pass


def parse_type(mitype):
    lst = re.split(r'[- .,]', mitype)
    return lst


def get_verif_year_from_str(str_verif_year):
    """
    Функция для получения года поверки из строки

    :param str_verif_year: строка - дата последние поверки
    :return: verif_year: int - год последней поверки
    """

    try:
        logger.info(f"Попытка получить из строки {str_verif_year} год")
        if type(str_verif_year) == datetime:
            verif_year = str_verif_year.date().year
        elif type(str_verif_year) == str:
            tmp_datetime = datetime.strptime(str_verif_year, "%d.%m.%Y")
            verif_year = tmp_datetime.date().year
        # print(f"Год поверки для текущего СИ - {verif_year}")
        logger.info(f"Год поверки для текущего СИ - {verif_year}")
        return verif_year
    except:
        logger.warning(f"Не удалось получить год поверки из строки - {str_verif_year}")


def get_si_info(worksheet, si: str, current_row: int):
    """
    Функция для получения типа СИ, номера СИ, даты последней и следующей поверки

    :param worksheet: объект worksheet
    :param si: вид СИ: ПУ, ТТ или ТН
    :param current_row: текущая строка
    :return: словарь с результатами
    """
    logger.info(f"Получаем номер текущего СИ {si} в строке {current_row}")
    tmp_serial = str(worksheet.cell(row=current_row, column=COLUMNS_SI[si]['serial']).value)
    # Убираем лишние пробелы
    current_serial = tmp_serial.strip().rstrip()
    logger.info(f"Заводской номер текущего СИ - {current_serial}")
    # Получаем тип СИ
    tmp_mitype = worksheet.cell(row=current_row, column=COLUMNS_SI[si]['type']).value
    mitype = parse_type(tmp_mitype)[0]

    return current_serial, mitype


def check_dict_for_write(dict_for_write, database):
    """
    Функция для проверки наличия в локальной БД предполагаемых к записи значений,
    другими словами, проверка дубликатов

    :param dict_for_write: словарь с исходными данными
    :return: словарь, готовый к записи
    """
    if not database.check_tbmetrology_value(dict_for_write):
        logger.info(f"Попытка записи данных в БД")
        title = dict_for_write['title']
        modification = dict_for_write['modification']
        type_title = dict_for_write['mitype']
        type_number = dict_for_write['mitnumber']
        name_org = dict_for_write['org_title']

        if not database.check_value((title,), mode="title"):
            title = database.set_id((title,), mode="title")
        else:
            title = database.get_id_title(title)

        if not database.check_value((modification,), mode="modification"):
            modification = database.set_id((modification,), mode="modification")
        else:
            modification = database.get_id_mod(modification)

        if not database.check_value((type_title, type_number), "type"):
            type_id = database.set_id((type_title, type_number), mode="type")
        else:
            type_id = database.get_id_type(type_title, type_number)

        if not database.check_value((name_org,), mode="name_org"):
            name_org = database.set_id((name_org,), mode="name_org")
        else:
            name_org = database.get_id_org(name_org)

        dict_for_write['title'] = title
        dict_for_write['modification'] = modification
        dict_for_write['mitype'] = type_id
        dict_for_write['mitnumber'] = type_id
        dict_for_write['org_title'] = name_org

        return dict_for_write


def check_verif_date(card: localdb.CardFgis, last_verif_date: str):
    """
    Функция для сверки даты последней поверки текущего СИ
    и даты послдней поверки, полученной из ФГИС

    :param card: объект CardFgis
    :param last_verif_date: дата последней поверки текущего СИ, полученная из файла
    :return: True or False
    """
    card_last_verif_date = datetime.strptime(card.verification_date, "%d.%m.%Y")
    if type(last_verif_date) == str:
        tmp_verif_date = datetime.strptime(last_verif_date, "%d.%m.%Y")
    else:
        tmp_verif_date = last_verif_date

    if card_last_verif_date == tmp_verif_date:
        return True
    else:
        return False


def pass_to_si_list(keywords: str):
    """
    Функция для прохода по перечню СИ.

    :param keywords: строка с перечнем видов СИ, разделённыъх пробелом
    :return:
    """
    """
    Функцию pass_to_rows() вызывать отсюда, с передачей номеров 
    начальной и конечной строки.
    """
    pass


def pass_to_rows(start_row: int, end_row: int):
    """
    Функция для прохода по строкам файла.

    :param start_row: начальная строка.
    :param end_row: конечная строка.
    :return:
    """
    """
    Отсюда вызывать метод чтения информации по СИ на текущей строке.
    Метод чтения информации реализовать в модуле xlsx, с передачей в качестве аргументов
    номера строки. Модуль xlsx должен считать в строке номер СИ, тип, даты последней и следующей поверки, 
    проверить валидность даты последней поверки, при необходимости скорректировать её, и вернуть всё это дело
    в виде одного объекта (или списка, словаря, кортежа) в готовом для использования виде, чтобы никаких
    преобразований больше не требовалось.
    """
    pass


def check_response(response: list[dict]):
    """
    Проверка ответа по запросу во ФГИС
    """
    # Проверяем результаты запроса, получено ли вообще что-нибудь
    # Если количество элементов списка в ответе больше 0, то есть
    # что-то получили
    if len(response) > 0:
        if len(response) > 1:
            return 0
            # prepare_and_write(response, 0)
        elif len(response) == 1:
            return 1
            # prepare_and_write(response, current_row)
    else:
        return -1


def prepare_and_write(response: list, database: localdb.WorkDb, row_number: int = 0):
    """
    Функция для подготовки к записи в локальную БД данных,
    полученных из ФГИС.

    :param response: список со словарями
    :param row_number: номер строки файла Excel, которая в данный момент обрабатывается
    :param database: объект localdb.WordDb - локальной БД
    :return:
    """
    for item in response:
        logger.info(f"Item = {item}")
        if item is not None:
            dict_for_write = check_dict_for_write(format_dict_for_write(item, row_number), database)
            logger.info(f"dict_for_write = {dict_for_write}")
            # Попытка записи в локальную БД
            if dict_for_write is not None:
                try:
                    database.write_metrology(dict_for_write)
                except Exception as err:
                    logger.warning(f"{err.__str__()}")
                    logger.warning(
                        f"Не удалось записать данные в БД <{str(dict_for_write)}>")



def work_on_fgis(**kwargs):
    """
    Функция для обработки файла и получения данных из ФГИС
    """
    # Присвоение значений переменным
    # # Имя Excel файла
    # name_excel_file = kwargs['name_excel_file']
    # # Год последней поверки
    # verif_year = kwargs['last_verif_year']
    # # Имя файла, содержащего параметры подключения к локальной БД
    # name_settings_file = kwargs['name_settings_file']
    # # Начальная строка файла Excel
    # start_row = kwargs['start_row']
    # Режим работы
    work_mode = kwargs['work_mode']
    # Серийный номер, если нужно найти информацию по какому-то конкретному номеру
    serial = kwargs['serial']
    # Информация по СИ из файла
    inform_si = kwargs['inform_si']
    # Объект локально БД - localdb.WorkDb
    database = kwargs['database']
    current_si = kwargs['current_si']
    # # Перечень видов СИ для обработки
    # keywords_si = kwargs['keywords_si']
    #
    # # Открываем книгу Excel
    # workbook = xlsx.XlsxFile(name_excel_file)
    # # Получаем объект worksheet - лист книги Excel
    # worksheet = workbook.active_sheet
    # # Получаем значение номера последней активной строки
    # row_end = worksheet.max_row

    def request_and_write(current_serial, database, last_verif_year, mitype, current_si):
        # Формируем словарь с данными для запроса во ФГИС
        dict_request = format_dict_requests(title=current_si,
                                            number=current_serial,
                                            verif_year=last_verif_year,
                                            mitype=mitype,
                                            rows=str(100))
        # Делаем запрос, получаем ответ
        response = fgis_eapi.request_fgis(dict_request)
        match check_response(response):
            # Если количество элементов в ответе = 1
            case 1:
                logger.info(f"Ок, получили данные для номера СИ - {current_serial}")
                prepare_and_write(response=response,
                                  database=database,
                                  row_number=current_row)
                logger.info(f"Обработан СИ с номером - {current_serial}")
            # Если количество элементов в ответе больше 1
            case 0:
                logger.info(f"Ок, получили данные для номера СИ - {current_serial}")
                prepare_and_write(response=response,
                                  database=database,
                                  row_number=0)
                logger.info(f"Обработан СИ с номером - {current_serial}")
            # Если ничего не получено на запрос, ничего и не обрабатываем
            case -1:
                logger.info(f"Для номера СИ - {current_serial}, по запросу к ФГИС ничего не получено")
                continue

    # Проверка режима работы
    match work_mode:
        case 'fgis':
            # Если режим работы - fgis
            # Текущий серийный номер СИ
            current_serial = inform_si['serial']
            # Текущий тип СИ
            mitype = inform_si['type']
            # Год последней поверки
            last_verif_year = kwargs['last_verif_year']
            # Год следующей поверки
            valid_year = kwargs['valid_year']
            # Проверяем параметр serial
            if serial != '' and serial != current_serial:
                logger.info(
                    f"Задан конкретный номер СИ для поиска - {serial}. Текущий номер СИ - {current_serial}"
                    f" не совпадает с введённым, пропускаем его.")
            else:
                logger.info(f"Готовим запрос в БД ФГИС по СИ - №{current_serial}")
                request_and_write(current_serial, database, last_verif_year, mitype, current_si)

        case 'unknow':
            # Если режим работы - unknow
            # Текущий серийный номер СИ
            current_serial = inform_si['serial']
            # Текущий тип СИ
            mitype = inform_si['type']
            # Год последней поверки
            last_verif_year = kwargs['last_verif_year']
            # Год следующей поверки
            valid_year = kwargs['valid_year']
            # Проверяем, если серийный номер, тип и год последней поверки не None
            # то начинаем запросы к ФГИС
            if current_serial != None and mitype != None and last_verif_year != None:
                # Цикл по годам текущего СИ
                for year in range(last_verif_year, datetime.now().year + 1):
                    request_and_write(current_serial, database, last_verif_year, mitype, current_si)



def work_on_local(**kwargs):
    """
    Функция для обработки файла локально, без подключения к ФГИС
    """
    pass


def work_on_change_serial(**kwargs):
    """
    Функция работает при выборе режима работы скрипта 'change_serial'
    """
    pass


def file_processing(name_excel_file: str, verif_year: int, keyword_si: str, work_mode: str,
                serial_number: str, start_row: int, name_setting_file: str):
    """
    Функция по обработке файла с заданными параметрами

    :param name_excel_file: имя файла Excel для обработки (полный путь)
    :param verif_year: значение года последней поверки, от которого отталкиваться при обработки файла
    :param keyword_si: строка с перечнем видов СИ для обработки
    :param work_mode: режим работы скрипта - local, fgis, unknow, change_serial, unknow_local
    :param serial_number: строка серийного номера СИ, если нужно получить информацию по какому-то
                        конкретному СИ
    :param start_row: начальная строка файла, с которой начинать обработку
    :param name_setting_file: имя файла, в котором находятся параметры подключения к локальной БД

    :return: None
    """
    # Читаем файл с настройками подключения к локальной БД
    localdb_parameters = read_settings_file(name_setting_file)
    # Устанавливаем соединение с БД
    database = localdb.WorkDb(database=localdb_parameters['database'],
                              user=localdb_parameters['user'],
                              password=localdb_parameters['password'],
                              port=localdb_parameters['port'],
                              host=localdb_parameters['host'])

    # Открываем файл Excel
    workbook = xlsx.XlsxFile(name_excel_file)
    # Получаем активный лист и значение максимальной строки (конечной)
    worksheet = workbook.active_sheet
    row_end = worksheet.max_row

    # Инициализация прогрессбара
    bar = IncrementalBar('Выполнение: ', max=(row_end - START_ROW) * len(keyword_si.split(sep=" ")) + (
            1 * len(keyword_si.split(sep=" "))))

    # Основной цикл по видам СИ
    for current_si in keyword_si.split(sep=' '):
        # Нужно получить номера столбцов для соответствующего вида СИ
        # Столбец типа СИ
        type_col = COLUMNS_SI[current_si]['type']
        # Столбец серийного номера СИ
        serial_col = COLUMNS_SI[current_si]['serial']
        # Столбец даты последней поверки
        verif_date_col = COLUMNS_SI[current_si]['verif_date']
        # Столбец даты следующей поверки
        valid_date_col = COLUMNS_SI[current_si]['valid_date']
        # Столбец для ссылки на корточку СИ
        href_col = COLUMNS_SI[current_si]['href']
        logger.info(f"Начинаем обход строк файла для вида СИ: {current_si}")

        # Цикл для обхода файла построчно
        for current_row in range(start_row, row_end + 1):
            bar.next()
            inform_si = workbook.get_inform_si(current_si, current_row)
            logger.info(f"Дата последней поверки для {current_si} и строки №{current_row} - {inform_si['verif_date']}")

            # Пытаемся получить год поверки из полученной строки
            # даты последней поверки
            if inform_si['verif_date'] is None:
                last_verif_year = None
                valid_year = None
            else:
                last_verif_year = inform_si['verif_date'].year
                # print(type(inform_si['valid_date']))
                if type(inform_si['valid_date']) is date:
                    valid_year = inform_si['valid_date'].year
                else:
                    # print(f"type: {type(inform_si['valid_date'])}, type_verif: {type(inform_si['verif_date'])}")
                    # print(type(inform_si['valid_date']) == date)
                    valid_year = None
                # Проверяем режим работы
                match work_mode:
                    # Если режим работы - unknow
                    case 'unknow':
                        # Запуск функции для обработки данных по СИ при режиме unknow
                        if verif_year == 0 and valid_year != None:
                            work_on_fgis(work_mode=work_mode, serial=serial_number,
                                         inform_si=inform_si, database=database,
                                         valid_year=valid_year, last_verif_year=last_verif_year, current_si=current_si)

                            # # Текущий серийный номер СИ
                            # current_serial = inform_si['serial']
                            # # Текущий тип СИ
                            # mitype = inform_si['type']
                            # # Проверяем, если серийный номер, тип и год последней поверки не None
                            # # то начинаем запросы к ФГИС
                            # if current_serial != None and mitype != None and last_verif_year != None:
                            #     # Цикл по годам текущего СИ
                            #     for year in range(last_verif_year, datetime.now().year + 1):
                            #         # Формируем словарь с данными для запроса во ФГИС
                            #         dict_request = format_dict_requests(title=current_si,
                            #                                             number=current_serial,
                            #                                             verif_year=last_verif_year,
                            #                                             mitype=mitype,
                            #                                             rows=str(100))
                            #         # Делаем запрос, получаем ответ
                            #         response = fgis_eapi.request_fgis(dict_request)
                            #         match check_response(response):
                            #             # Если количество элементов в ответе = 1
                            #             case 1:
                            #                 logger.info(f"Ок, получили данные для номера СИ - {current_serial}")
                            #                 prepare_and_write(response=response,
                            #                                   database=database,
                            #                                   row_number=current_row)
                            #                 logger.info(f"Обработан СИ с номером - {current_serial}")
                            #             # Если количество элементов в ответе больше 1
                            #             case 0:
                            #                 logger.info(f"Ок, получили данные для номера СИ - {current_serial}")
                            #                 prepare_and_write(response=response,
                            #                                   database=database,
                            #                                   row_number=0)
                            #                 logger.info(f"Обработан СИ с номером - {current_serial}")
                            #             # Если ничего не получено на запрос, ничего и не обрабатываем
                            #             case -1:
                            #                 logger.info(f"Для номера СИ - {current_serial}, по запросу к ФГИС ничего не получено")
                            #                 continue
                        else:
                            logger.warning(f"Условия для запуска обработки СИ по всем годам не соответствуют требуемым."
                                           f"{verif_year = }, {valid_year = }")
                    case 'fgis':
                        # Запуск функции для обработки данных по СИ при режиме fgis
                        work_on_fgis(work_mode=work_mode, serial=serial_number, inform_si=inform_si,
                                     database=database, valid_year=valid_year, last_verif_year=last_verif_year,
                                     current_si=current_si)

                    case 'local':
                        # Запуск функции для обработки данных по СИ при режиме local
                        pass
                    case 'unknow_local':
                        # Запуск функции для обработки данных по СИ при режиме unknow_local
                        pass


def main():
    """
    Основная функция скрипта.
    Обрабатывается заданный файл Excel и сохраняет результаты работы.

    :return: None
    """
    # Начальная строка файла по умолчанию - если не задана другая
    START_ROW = 13
    # Запись в лог-файл информации о начале работы
    logger.info(f"Запуск скрипта {__name__}, дата и время: {datetime.now()}")
    # Парсинг параметров запуска скрипта
    argv_parser = parse_args()
    namespace_argv = argv_parser.parse_args(sys.argv[1:])
    namefile_xlsx = namespace_argv.namefile
    verif_year = namespace_argv.years
    keyword_si = namespace_argv.keyword
    mode = namespace_argv.mode
    serial = namespace_argv.serial
    start = namespace_argv.START
    namefile_setting = namespace_argv.setfile


    # =========================================================================================#

    # # Читаем файл с параметрами подключения к локальной БД
    # local_db_parameters = read_settings_file(namefile_setting)
    # # Устанавливаем соединение с БД
    # database = localdb.WorkDb(database=local_db_parameters['database'],
    #                           user=local_db_parameters['user'],
    #                           password=local_db_parameters['password'],
    #                           port=local_db_parameters['port'],
    #                           host=local_db_parameters['host'])

    # ==========================================================================================#

    # Проверяем введённые параметры на корректность,
    # недопустим режим unknow и год 0
    if verif_year != 0 and (mode in ['unknow', 'unknow_local']):
        print(f"Заданы некорректные параметры запуска: при использовании режима {mode} не нужно задавать год."
              f"Прекращаем работу...")
        logger.warning(
            f"Заданы некорректные параметры запуска: mode = {mode}, verif_year = {verif_year}.\nПрекращаем работу.")
        sys.exit()
    if verif_year == 0 and (mode in ['fgis', 'local']):
        joke_answer = input(f"Вы не задали год для обработки, но при этом выбрали режим {mode}."
                            f"В таком случае будет использован текущий год - {datetime.now().year}? (Y/n): ")
        if joke_answer.lower() == 'n':
            logger.warning(f"Пользователь отказался от дальнейшего выполнения. Прекращаем работу и по домам!")
            print("Ути, какие мы ранимые...ну тогда всё.")
            sys.exit()
        else:
            verif_year = datetime.now().year
    if start != 0:
        START_ROW = start
    else:
        pass
    if verif_year == 0 and mode == 'unknow_local':
        verif_year = date.today().year

    # Параметры проверены, запись в лог-файл о выбранных параметрах работы скрипта
    logger.info(f"Парсим файл Excel со следующими исходными данными: имя файла - {namefile_xlsx}, "
                f"год поверки - {verif_year}, СИ для парсинга - {keyword_si}")

    # Вызывем функцию по дальнейшей обработке файла
    file_processing(name_excel_file=namefile_xlsx,
                    verif_year=verif_year,
                    keyword_si=keyword_si,
                    work_mode=mode,
                    serial_number=serial,
                    start_row=START_ROW,
                    name_setting_file=namefile_setting)

    # workbook = xlsx.XlsxFile(namefile_xlsx)
    # worksheet = workbook.active_sheet
    # row_end = worksheet.max_row

    def check_si_on_localdb(si_inform: dict):
        """
        Проверяем наличие в локальной БД информации по текущему СИ

        :param si_inform: словарь с данными по СИ:
                            'serial' - серийный номер СИ,
                            'type' - тип СИ,
                            'verif_date' - дата поверки
        :return: True or False
        """
        # Получаем из словаря серийный номер
        # тип СИ, дату поверки и пытаемся получить из БД данные
        # по этим данным
        serial = si_inform['serial']
        type = si_inform['type']
        verif_date = si_inform['verif_date']

    def check_true(dct):
        lst = list(dct.values())
        count = 0
        for i in lst:
            if i:
                count += 1

        return True if count > 0 else False

    def fgis_request(request_parameters: dict):
        """
        Функция для отправки параметров для запроса во ФГИС и
        обработки результатов запроса
        :param request_parameters: словарь с параметрами
        :return:
        """
        dict_request = format_dict_requests(title=request_parameters['current_si'],
                                            number=request_parameters['current_serial'],
                                            verif_year=request_parameters['last_verif_year'],
                                            mitype=request_parameters['mitype'],
                                            rows=str(100))
        response = fgis_eapi.request_fgis(dict_request)
        return response

    def parse_list_card(lst, type_si, verif_date):
        """
        Функция для обработки списка объектов CardFgis

        :param lst: список объектов CardFgis
        :param type_si: строка типа СИ
        :param verif_date: дата последней поверки, взята из файла
        :return: словарь с объектами CardFgis, по которым дата последней поверки
                совпадает с требуемой
        """

        res_dict = {}
        date = None

        if type(verif_date) is datetime:
            date = datetime.strftime(verif_date, '%d.%m.%Y')
        elif type(verif_date) is str:
            date = datetime.strptime(verif_date, '%d.%m.%Y')
        if len(lst) == 1:
            res_dict[0] = lst[0]
        else:
            for ind, card in enumerate(lst):
                current_type = card.mi_mitype
                current_mod = card.mi_modification
                parse_type = TypeParseSi(current_type, type_si)
                res_parse = parse_type.parse()
                parse_mod = TypeParseSi(current_mod, type_si)
                res_parse_mod = parse_mod.parse()
                if check_true(res_parse) and date == card.verification_date:
                    res_dict[ind] = card

        return res_dict

    def local_request(serial, si, year, current_type, verif_date):
        """
        Обращаемся к локальной БД для получения данных по номеру, типу СИ
        и году поверки

        :param serial: номер СИ
        :param si: тип СИ
        :param year: год поверки СИ
        :param current_type: наименование типа СИ
        :return: словарь с данными по текущему СИ
        """
        dict_filter = {'serial_si': serial,
                       'name_si': si,
                       'verif_year': year,
                       'type_si': current_type,
                       'verif_date': verif_date}

        # lst_card = database.get_card_for_si(dict_filter)
        lst_card = database.get_card_si(dict_filter['serial_si'], dict_filter['type_si'])

        if not type(lst_card) == list and not lst_card == None:
            return lst_card
        else:
            if not lst_card == None:
                logger.info(f"Получено для текущего СИ {len(lst_card)} значений из БД.")
            else:
                logger.info(f"Ничего не получено для текущего СИ {serial}")
        if type(lst_card) == list and len(lst_card) > 0:
            # Здесь нужно реализовать проверку карточек на все условия:
            #  - сформировать список карточек в которых дата поверки совпадает, если таких больше одной
            #  - проверить эти карточки по идентификаторам

            d = parse_list_card(lst_card, dict_filter['type_si'], dict_filter['verif_date'])
            if len(d) == 1:
                res_lst_card = d[list(d.keys())[0]]
                return res_lst_card
            else:
                return list(d.values())

    def check_result_local_request(res_request, coord: tuple, str_verif_date: str):
        """
        Функция для проверки результатов запроса к локальной БД
        и записи ссылки в ячейку, если она найдена

        :param res_request: результаты запроса
        :return:
        """
        if type(res_request) == list:
            # worksheet.cell(row=coord[0],
            #                column=coord[1]).value = f"Проверить в ручном режиме. Найдено {len(res_request)} значения(й)"
            # xlsx.set_fill(worksheet, coord, 'yellow')
            # worksheet.cell(row=coord[0], column=coord[1]).fill = FillYellow
            lst_same_date = []
            for lst in res_request:
                # Отправляем на проверку полученную из файла строку даты последней поверки
                # и объект CardFgis по текущему счётчику.
                # В результате сравниваются две этих даты - если они равны, то True
                # если не равны, то False
                if check_verif_date(lst, str_verif_date):
                    # Если даты из файла и из объекта CardFgis равны, то добавляем этот счётчик
                    # в список ПУ с одинаковыми датами последней поверки
                    lst_same_date.append(lst)
            # Если в результате у нас в списке находится только один счётчик с датой последней поверки
            # совпадающей с тем, что в файле, то устанавливаем ссылку
            if len(lst_same_date) == 1:
                if set_href(lst_same_date[0], coord, str_verif_date, worksheet):
                    logger.info(f"Ссылка для СИ {lst_same_date[0].mi_number} найдена.")
                    workbook.set_id_record((coord[0], COLUMN_ID), lst_same_date[0].id_record)
            # Если в списке счётчиков больше 1, то отправляем список на обработку
            # и получения единственно верного объекта CardFgis, если такое возможно
            elif len(lst_same_date) > 1:
                res_card = lst_same_date_parse(lst_same_date)
                if set_href(res_card, coord, str_verif_date, worksheet):
                    workbook.set_id_record((coord[0], COLUMN_ID), res_card.id_record)
                    logger.info(f"Ссылка для {res_card.mi_number} найдена.")
        else:
            # Если тип res_request == CardFgis, то считаем его единственно верным вариантом
            # и пытаемся записать ссылку на карточку в ячейку
            if set_href(res_request, coord, str_verif_date, worksheet):
                logger.info(f"Ссылка для СИ {res_request.mi_number} найдена.")

    def lst_same_date_parse(lst_same_date: list):
        """
        Функция для обработки карточек с одинаковыми датами поверки

        :param lst_same_date: список с объектами CardFgis
        :return: объект CardFgis
        """
        tmp_lst_card = {}
        # for ind, card in enumerate(lst_same_date):
        #     for i, c in enumerate(lst_same_date[1:]):
        #         if card.result_docnum[:card.result_docnum.rfind('/')] == c.result_docnum[:c.result_docnum.rfind('/')]:
        #             tmp_lst_card[ind].append(i)
        # tmp_lst_card[lst_same_date[0].vri_id] = lst_same_date[0]
        for ind in range(len(lst_same_date)):
            try:
                if lst_same_date[ind].check_equals(lst_same_date[ind + 1]):
                    tmp_lst_card[lst_same_date[ind].vri_id] = lst_same_date[ind]
                    tmp_lst_card[lst_same_date[ind + 1].vri_id] = lst_same_date[ind + 1]
            except IndexError:
                break
        keys = {int(item[0][2:]): item[0] for item in tmp_lst_card.items()}
        max_id = 0
        for k in list(keys.keys()):
            if k > max_id:
                max_id = k
        return tmp_lst_card[keys[max_id]]

    def set_href(card, coord, str_verif_date, worksheet):
        """
        Функция для записи гиперссылки по СИ в ячейку

        :param card: объект CardFgis с информацией по СИ
        :param coord: кортеж с коордиинатами ячейки для записи
        :param str_verif_date: строка даты последней поверки
        :param worksheet:
        :return: True or False
        """
        if not card == None:
            verif_date_current_card = datetime.strptime(card.verification_date, "%d.%m.%Y")
            date_current_card = verif_date_current_card.date()
            if type(str_verif_date) == datetime:
                date_verif_current_row = str_verif_date.date()
            elif type(str_verif_date) == str:
                date_verif_current_row = datetime.strptime(str_verif_date, "%d.%m.%Y")
            if date_verif_current_row == date_current_card:
                href = card.href

                if not workbook.check_merged(coord):
                    workbook.set_href(coord, href)
                    workbook.set_fill(coord, 'green')
                    return True
            else:
                href = card.href
                workbook.set_href(coord, href)
                workbook.set_fill(coord, 'red')
                workbook.set_date((coord[0], coord[1] - 2), card.verification_date)
                return False
        else:
            workbook.set_fill(coord, 'blue')
            return False

    # def prepare_and_write(response: list, row_number: int = 0):
    #     """
    #     Функция для подготовки к записи в локальную БД данных,
    #     полученных из ФГИС.
    #
    #     :param response: список со словарями
    #     :param row_number: номер строки файла Excel, которая в данный момент обрабатывается
    #     :return:
    #     """
    #     for item in response:
    #         logger.info(f"Item = {item}")
    #         if item is not None:
    #             dict_for_write = check_dict_for_write(format_dict_for_write(item, row_number), database)
    #             logger.info(f"dict_for_write = {dict_for_write}")
    #             # Попытка записи в локальную БД
    #             if dict_for_write is not None:
    #                 try:
    #                     database.write_metrology(dict_for_write)
    #                 except Exception as err:
    #                     logger.warning(f"{err.__str__()}")
    #                     logger.warning(
    #                         f"Не удалось записать данные в БД <{str(dict_for_write)}>")
    #     logger.info(f"Обработан СИ с номером - {current_serial}")

    # =============================================================================#
    # Функции по режимам работы (добавить), пока не вводить в строй
    def work_on_fgis(last_verif_year: int, si_for_fgis: str, namefile: str, start_row: int):
        """
        Функция работает при выборе режима работы скрипта 'fgis'
        С параметрами тоже определиться

        :return: возможно true or false (подумать надо ли)
        """
        print("Отработала функция work_on_fgis")

    def work_on_local(last_verif_year: int, si_for_local: str, namefile: str, start_row: int):
        """
        Функция работает при выборе режима работы скрипта 'local'
        С параметрами определиться

        :return: возможно true or false (подумать надо ли)
        """
        print("Отработала функция work_on_local")

    def work_in_unknowledge():
        """
        Функция работает при выборе режима работы скрипта 'unknow'
        Возможно не нужна, можно будет реализовать через параметры для <work_on_fgis>

        :return:
        """
        print("Отработала функция work_on_unknowledge")

    def work_on_change_serial():
        """
        Функция работает при выборе режима работы скрипта 'change_serial'
        С параметрами определиться

        :return:
        """
        print("Отработала функция work_on_change_serial")

    def work_on_unknow_local(last_verif_year: int, si_for_local: str, namefile: str, start_row: int,
                database: localdb.WorkDb):
        """
        Функция для получения данных по СИ из локальной БД, предполагая, что год
        последней поверки - текущий год

        :param last_verif_year:
        :param si_for_local:
        :param namefile:
        :param start_row:
        :param database:
        :return:
        """
        pass

    # =============================================================================#
    match mode:
        case 'fgis':
            print(f"Запуск work_on_fgis для режима работы {mode}")
            # work_on_fgis()
        case 'local':
            print(f"Запуск work_on_local для режима работы {mode}")
            # work_on_local()
        case 'unknow':
            print(f"Запуск work_on_unknowledge для режима работы {mode}")
            # work_in_unknowledge()
        case 'change_serial':
            print(f"Запуск work_on_change_serial для режима работы {mode}")
            # work_on_change_serial()
    # ============================================================================#
    bar = IncrementalBar('Выполнение: ', max=(row_end - START_ROW) * len(keyword_si.split(sep=" ")) + (
                1 * len(keyword_si.split(sep=" "))))

    # Основной цикл прохода по введённым СИ
    for current_si in keyword_si.split(sep=" "):
        # Нужно получить номера столбцов для соответствующего вида СИ
        # Столбец типа СИ
        type_col = COLUMNS_SI[current_si]['type']
        # Столбец серийного номера СИ
        serial_col = COLUMNS_SI[current_si]['serial']
        # Столбец даты последней поверки
        verif_date_col = COLUMNS_SI[current_si]['verif_date']
        # Столбец даты следующей поверки
        valid_date_col = COLUMNS_SI[current_si]['valid_date']
        # Столбец для ссылки на корточку СИ
        href_col = COLUMNS_SI[current_si]['href']
        logger.info(f"Начинаем обход строк файла для вида СИ: {current_si}")

        for current_row in range(START_ROW, row_end + 1):
            bar.next()
            inform_si = workbook.get_inform_si(current_si, current_row)
            logger.info(f"Дата последней поверки для {current_si} и строки №{current_row} - {inform_si['verif_date']}")

            # Пытаемся получить год поверки из полученной строки
            # даты последней поверки, если значение ячейки не соответствует
            # исключениям в словаре

            if inform_si['verif_date'] is None:
                last_verif_year = None
                valid_year = None
            else:
                last_verif_year = inform_si['verif_date'].year
                # print(type(inform_si['valid_date']))
                if type(inform_si['valid_date']) is date:
                    valid_year = inform_si['valid_date'].year
                else:
                    # print(f"type: {type(inform_si['valid_date'])}, type_verif: {type(inform_si['verif_date'])}")
                    # print(type(inform_si['valid_date']) == date)
                    valid_year = None

            # Проверяем год для обработки из параметров скрипта
            # для дальнейших действий
            if verif_year == 0 and valid_year != None:
                # Действия для проверки СИ по всем годам,
                # начиная с года последней поверки и до текущего года
                current_serial = inform_si['serial']
                mitype = inform_si['type']
                if current_serial != None and mitype != None and last_verif_year != None:
                    # change_date = database.get_change_date()
                    # Цикл по годам текущего СИ
                    for year in range(last_verif_year, datetime.now().year + 1):
                        fgis_request({'current_si': current_si,
                                      'current_serial': current_serial,
                                      'last_verif_year': last_verif_year,
                                      'mitype': mitype})
            elif verif_year == last_verif_year:
                # Действия для проверки по конкретному году
                current_serial = inform_si['serial']
                mitype = inform_si['type']
                flag_href_style = inform_si['flag_href_style']
                href_value = inform_si['href_value']
                id_record = inform_si['id']

                # Провряем режим работы и выбираем стезю...блядь
                match mode:
                    case 'fgis':
                        if serial != '' and serial != current_serial:
                            logger.info(
                                f"Задан конкретный номер СИ для поиска - {serial}. Текущий номер СИ - {current_serial}"
                                f" не совпадает с введённым, пропускаем его.")
                        else:
                            logger.info(f"Готовим запрос в БД ФГИС по СИ - {current_si}, №{current_serial}")
                            response = fgis_request({'current_si': current_si,
                                                     'current_serial': current_serial,
                                                     'last_verif_year': last_verif_year,
                                                     'mitype': mitype})
                            # Проверяем результаты запроса, получено ли вообще что-нибудь
                            # Если количество элементов списка в ответе больше 0, то есть
                            # что-то получили
                            if len(response) > 0:
                                logger.info(f"Ок, получили данные для номера СИ - {current_serial}")
                                if len(response) > 1:
                                    prepare_and_write(response, 0)
                                elif len(response) == 1:
                                    prepare_and_write(response, current_row)

                    case 'local':
                        str_verif_date = workbook.get_value((current_row, verif_date_col))

                        if not flag_href_style:
                            # Ссылки нет
                            # не забыть про идентификатор записи в БД и номер строки файла
                            # ...
                            # ...
                            # ...
                            res_local_request = local_request(current_serial, current_si, last_verif_year, mitype,
                                                              str_verif_date)
                            check_result_local_request(res_local_request, (current_row, href_col), str_verif_date)
                        else:
                            # Проверить валидность гиперссылки для начала
                            # Если ссылка валидна, то выделить одним цветом, если нет, то другим
                            # Учесть номер идентификатор записи и номер строки в БД
                            # ...
                            # ...
                            # ...
                            if database.check_valid_href(href_value):
                                cur_id_record = database.get_id_for_href(href_value)
                                if not id_record is None:
                                    if cur_id_record == id_record:
                                        workbook.set_fill((current_row, href_col), 'orange')
                                else:
                                    workbook.set_fill((current_row, href_col), 'orange')
                                    # workbook.set_id_record((current_row, COLUMN_ID), cur_id_record)
                            else:
                                workbook.set_fill((current_row, href_col), 'red_brown')
                    case 'change_serial':
                        if str(current_serial)[0] == '0':
                            continue
                        elif str(current_serial)[0] == '1':
                            current_serial = '0' + str(current_serial)
                            workbook._write_value((current_row, COLUMNS_SI[current_si]['serial']), current_serial)
            elif verif_year != last_verif_year and verif_year == valid_year:
                logger.info(f"Пропускаем строку {current_row}, так как не совпадает год.")

    workbook.save()
    bar.finish()


if __name__ == "__main__":
    """
    Запуск функции main()
    """
    main()
