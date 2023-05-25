import requests
import time
from http.client import HTTPConnection
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

params = {
    'year': '2022',
    'start': '0',
    'rows': '20',
    'search': '04977'
}
start = 0
count_page = 100
f_out = 'out_fgis.txt'
with open(f_out, 'a', encoding='utf-8') as out_file:
    while True:
        url = f"http://fgis.gost.ru/fundmetrology/eapi/vri?year=2023&search=Трансформаторы*тока*%203171040338&start=0&rows=100"


        response = requests.get(url, cookies=cookies, headers=headers)
        # print(response.status_code, response.text)
        if response.status_code == 200:
            j = response.json()
            for i in j['result']['items']:
                out_file.writelines(str(i)+'\n')
                print(i.keys(), i.values(), sep='\n')
            out_file.write('\n\n')
            count = j.get('result').get('count')
            print(len(j.get("result").get('items')), j.get('result').get('count'))
            if count < count_page:
                break
            else:
                if (count - start) > 100:
                    start += 100
                else:
                    break
        else:
            print('Что-то пошло не так...')
            print(response.status_code, response.text)
            time.sleep(2)