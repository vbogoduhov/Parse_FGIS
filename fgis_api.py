import time
import requests
from http.client import HTTPConnection
import app_logger

HTTPConnection._http_vsn_str = "HTTP/1.0"

cookies = {
    'session-cookie': '171461b5615dcb9d80a41a5504983c47ae7e8d3664c36487bc4aa2a12ae7627675b4918f13e577d80a8a29a0e98958c2',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    # 'Cookie': 'session-cookie=171461b5615dcb9d80a41a5504983c47ae7e8d3664c36487bc4aa2a12ae7627675b4918f13e577d80a8a29a0e98958c2',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

logger = app_logger.get_logger('fgis_api', 'fgis_log.log')

def request_fgis(dict_params: dict):
    """
    Запрос в БД ФГИС
    :return:
    """
    url_for_request = format_url(dict_params)
    logger.info(f"Попытка запроса <{url_for_request}>")
    result_items = []
    count_req = 0
    count_err = 0
    while True:
        if count_req > 15:
            logger.warning(f"Получение данных по запросу <{url_for_request}> прервано из-за превышения допустимого количества попыток")
            break
        if count_err > 15:
            logger.warning(f"Подключение прервано из-за таймаунта соединения.")
            break
        try:
            response = requests.get(url_for_request, cookies=cookies, headers=headers)
        except requests.exceptions.ConnectTimeout:
            count_err += 1
            continue
        if response.status_code == 200:
            logger.info(f"Запрос <{url_for_request}> успешно выполнен")
            response_json = response.json()
            # Получаем количество элементов в ответе
            count_items = response_json.get('result').get('count')
            if count_items <= int(dict_params['rows']) and count_items > 0:
                result_items = parse_response(response_json)
                logger.info(f"Результаты запроса <{url_for_request}> обработаны. Завершаем цикл обработки.")
            elif count_items == 0:
                logger.info(f"По запросу <{url_for_request}> данных не получено")
            else:
                logger.warning(f"По запросу <{url_for_request}> получено результатов, более {dict_params['rows']}. Прекращаем обработку")
            break
        else:
            logger.warning(f"По запросу <{url_for_request}> не получено ответа от сервера. Код ответа: {response.status_code}, {response.text}")
            time.sleep(2)
            count_req += 1

    return result_items

def parse_response(result_response):
    """
    Обработка результата запроса
    :param result_response: результат запроса к БД ФГИС
    :return: True - если все результаты обработаны, или False -
            если требуется получить ещё страницу(ы) по запросу
    """
    items = result_response.get('result').get('items')

    return items

def format_url(d_params: dict, start: int=0):
    """
    Форматирование строки url с использованием
    переданных параметров для запроса
    :param d_params: параметры для запроса
    :return: форматированная строка url
    """
    title = d_params['filter_mititle']
    verif_year = d_params['verification_year']
    mi_number = d_params['filter_minumber']
    mi_type = d_params['filter_mitype']+'*%20'
    rows = d_params['rows']
    url = f"http://fgis.gost.ru/fundmetrology/eapi/vri?year={verif_year}&search={title}{mi_type}{mi_number}&start={start}&rows={rows}"

    return url

def main():
    pass


if __name__ == '__main__':
    main()