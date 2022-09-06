"""
Модуль для парсинга файла Excel с перечнём средств измерений
и метрологией по ним.
"""
import os
import sys
from datetime import datetime, date
import openpyxl
from openpyxl.styles import PatternFill
import argparse
import app_logger
import parse_fgis
from work_db import WorkDb

logger = app_logger.get_logger(__name__)
# Константы
#==================#

# Номера столбцов для соответствующего типа СИ
COLUMNS_SI = {
    'ПУ': {
        'type': 8,
        'number': 9,
        'verif_date': 12,
        'valid_date': 13,
        'href': 14
    },
    'ТТ': {
        'type': 15,
        'number': 17,
        'verif_date': 20,
        'valid_date': 21,
        'href': 22
    },
    'ТН': {
        'type': 23,
        'number': 25,
        'verif_date': 28,
        'valid_date': 29,
        'href': 30
    }
}

EXC_STR = ['',
           '-',
           '--',
           '---',
           'не пригоден',
           'н/д',
           'нет данных',
           'отсутствует',
           None]

# Начальная строка
START_ROW = 13

# ===================#

def create_parse_arg():
    """
    Парсер для параметров командной строки,
    разбор переданных параметров запуска
    """
    logger.info("Парсинг параметров командной строки")
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--namefile', type=str, default="Перечень ТУ АСКУЭ.xlsx",
                        help='Имя файла Excel')
    parser.add_argument('-y', '--years', type=int, default=datetime.today().year,
                        help='Год поверки СИ для выборки')
    parser.add_argument('-k', '--keyword', type=str, default='ПУ ТТ ТН', help="СИ по которому нужно получить данные из ФГИС: "
                                                                              "ПУ - приборы учёта, ТТ - трансформаторы тока, ТН - трансформаторы напряжения.")
    logger.info("Парсинг параметров командной строки закончен")

    return parser

def get_worksheet(namefile: str):
    """
    Открываем файл Excel, возвращаем объект книги и листа

    :param namefile: имя файла
    :return: workbook, worksheet - объект книги и объект листа
    """
    logger.info(f"Пытаемся открыть файл {namefile}")
    if os.path.isfile(namefile):
        workbook = openpyxl.load_workbook(namefile, data_only=True)
        worksheet = workbook['Прил.1.1 (Сч,ТТ,ТН)']
        logger.info(f"Файл {namefile} успешно открыт")
        return workbook, worksheet
    else:
        logger.warning(f"Невозможно открыть файл {namefile}, так как он не существует")

def get_verif_year_from_str(str_verif_year):
    """
    Метод для получения года поверки из строки

    :param str_verif_year: строка - дата последние поверки
    :return: verif_year: int - год последней поверки
    """

    try:
        logger.info(f"Попытка получить из строки {str_verif_year} год")
        verif_year = str_verif_year.date().year
        print(f"Год поверки для текущего СИ - {verif_year}")
        logger.info(f"Год поверки для текущего СИ - {verif_year}")
        return verif_year
    except:
        logger.warning(f"Не удалось получить год поверки из строки - {str_verif_year}")

def format_dict_requests(title="", verif_year=datetime.today().year, number="", rows=str(20)):
    """
    Формируем словарь для формирования запроса в БД ФГИС

    :param title: наименование типа СИ, часть или полное наименование
    :param verif_year: год последней поверки, для выборки из ФГИС
    :param number: заводской номер СИ для выборки из ФГИС
    :param rows: количество строк в запросе
    :return: dict_requests - сформированный словарь для запроса
    """
    if title == "ПУ":
        filter_mititle = "электрической энергии"
    elif title == "ТТ":
        filter_mititle = "трансформаторы тока"
    elif title == "ТН":
        filter_mititle = "трансформаторы напряжения"

    dict_request = {
        'filter_mititle': filter_mititle,
        'verification_year': str(verif_year),
        'filter_minumber': str(number),
        'rows': rows
    }

    return dict_request

def get_parse_si(si_for_parse: str, verif_year, mode='fgis', namefile=''):
    """
    Парсинг из файла средства измерений, переданного в строке si_for_parse

    :param si_for_parse: СИ для парсинга
    :param verif_year: год поверки дл выборки
    :param mode: режим работы: fgis - для сбора данных из БД ФГИС по СИ и запись в локальную БД
                для дальнейшей выборки оттуда гиперссылок;
                               local - для работы с локальной БД, и заполнения информации по СИ в файле
    :param namefile: имя Excel файла для работы, по умолчанию из файла берётся конкретный лист, с конкретным названием
    :return:
    """
    logger.info("Старт get_parse_si()")
    Fill = PatternFill(start_color='cccc9966', end_color='cccc9933', fill_type='solid')
    # Открываем файл и получаем объект листа
    workbook, worksheet = get_worksheet(namefile)
    if workbook != None and worksheet != None:
        end_row = worksheet.max_row

        # В цикле проходим по всем СИ
        for si in si_for_parse.split(sep=" "):
            # Получаем номера столбцов для соответствующего СИ
            # из словаря констант
            type_col = COLUMNS_SI[si]['type']
            number_col = COLUMNS_SI[si]['number']
            verif_date_col = COLUMNS_SI[si]['verif_date']
            valid_date_col = COLUMNS_SI[si]['valid_date']
            href_col = COLUMNS_SI[si]['href']
            logger.info(f"Старт обхода строк файла {namefile} для СИ: {', '.join(si_for_parse.split(sep=' '))}")
            for r in range(START_ROW, end_row + 1):
                print(f"Строка №{r} файла Excel.")
                str_verif_date = worksheet.cell(row=r, column=verif_date_col).value
                logger.info(f"Дата последней поверки текущей строки - {str_verif_date}")

                if str_verif_date not in EXC_STR:
                    year = get_verif_year_from_str(str_verif_date)
                else:
                    year = None

                if year == verif_year:
                    logger.info("Получаем номер текущего СИ")
                    current_serial = worksheet.cell(row=r, column=number_col).value
                    logger.info(f"Заводской номер текущего СИ - {current_serial}")
                    match mode:
                        case 'fgis':
                            request_fgis(current_serial, si, verif_year, year)
                        case 'local':
                            current_si_type = worksheet.cell(row=r, column=type_col).value
                            local_request = request_local(current_serial, si, year, current_si_type)
                            href = get_href(local_request)
                            worksheet.cell(row=r, column=href_col).value = href
                            worksheet.cell(row=r, column=href_col).fill = Fill
                    # worksheet.cell(row=r, column=number_col).fill = Fill
                else:
                    pass


def request_fgis(current_serial, si, verif_year, year):
    """
    Метод для запроса в БД ФГИС

    :param current_serial: текущий номер СИ
    :param si: наименование типа СИ
    :param verif_year: год поверки для проверки
    :param year: год поверки текущего СИ
    :return:
    """
    logger.info(
        f"Попытка запроса данных из БД ФГИС по текущему номеру СИ - {current_serial} и году поверки - {verif_year}")
    dict_request = format_dict_requests(title=si,
                                        number=current_serial,
                                        verif_year=year,
                                        rows=str(20))
    print(dict_request)
    parse_fgis.get_data_from_fgis(dict_request)

def request_local(serial, si, year, current_type):
    """
    Обращаемся к локальной БД для получения данных по номеру, типу СИ
    и году поверки
    :param serial: номер СИ
    :param si: тип СИ
    :param year: год поверки СИ
    :param current_type: наименование типа СИ
    :return: словарь с данными по текущему СИ
    """
    pass

def get_href(local_request):
    """
    Метод для разбора словаря с данными по СИ
    и получения ссылки из него
    :param local_request: словарь с данными
    :return: строка, содержащая ссылку на карточку СИ
    """
    pass

def main():
    logger.info("Запуск скрипта!")
    arg_parser = create_parse_arg()
    namespace_arg = arg_parser.parse_args(sys.argv[1:])
    namefile = namespace_arg.namefile
    verif_year = namespace_arg.years
    keyword_si = namespace_arg.keyword
    logger.info(f"Парсим файл Excel со следующими исходными данными: имя файла - {namefile}, "
                f"год поверки - {verif_year}, СИ для парсинга - {keyword_si}")
    get_parse_si(keyword_si, verif_year=verif_year, namefile=namefile, mode="fgis")


if __name__ == "__main__":
    main()