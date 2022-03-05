import json
import os
import tushare as ts
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
import jmespath
import threading


global header, company_url, session, page, logger
header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/97.0.4692.99 Safari/537.36",
            "Connection": "close"
}
company_url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
session = requests.Session()
page = 0
the_poor_of_threads = []


ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
pro = ts.pro_api()
df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date,market')
df_company_list.set_index('ts_code', inplace=True)
print(df_company_list)



def get_Annual_Report(company_list):
    global page, company_url, logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(lineno)d - %(threadName)s - %(message)s')
    logger = logging.getLogger(__name__)
    for stock in range(len(company_list)+1):
        if company_list.symbol[stock][0:3] == '68':
            data = {'secCode': company_list.symbol[stock], 'market': 'K', 'pn': page}
        elif company_list.symbol[stock][0] == '8':
            data = {'secCode': company_list.symbol[stock], 'market': 'bjs', 'pn': page}
            company_url = 'http://doc.rongdasoft.com/stockInfo/stockContentThree'
        else:
            data = {'secCode': company_list.symbol[stock], 'market': 'A', 'pn': page}
        jmespath_expr = jmespath.compile("files[?ends_with(title, '2021年年度报告')].downpath")
        res = session.post(company_url, headers=header, data=data)
        pages = jmespath.search('page_num', json.loads(res.text))
        # print(f"正在查询{company_list.symbol[stock]}")
        for page in range(pages):
            res = session.post(company_url, headers=header, data=data)
            downpath = jmespath_expr.search(json.loads(res.text))
            if len(downpath) > 0:
                title = jmespath.search("files[?ends_with(title, '2021年年度报告')].title", json.loads(res.text))
            if downpath:
                try:
                    if company_list.industry[stock]:
                        path = f'./{company_list.industry[stock]}/'
                        if not os.path.exists(path):
                            os.makedirs(path)
                    else:
                        path = ''
                    files_downpath = session.get(downpath[0], headers=header)
                    with open(f'{path}{title}.pdf', 'wb') as f:
                        f.write(files_downpath.content)
                        f.flush()
                    f.close()
                    print(f"已下载{title}")
                    break
                except Exception as e:
                    print(f"{title}下载失败\n{e}")
            else:
                print(f"{company_list.symbol[stock]}未披露21年年报")




for i, n in enumerate(range(0, len(df_company_list), len(df_company_list) // 10 + 1)):
    stars_threading = threading.Thread(target=get_Annual_Report, args=(df_company_list[n:n + len(df_company_list) // 10 + 1],))
    stars_threading.start()
    the_poor_of_threads.append(stars_threading)
for threadings in the_poor_of_threads:
    threadings.join()
print('Done')


# # for page in range(pages[0]):
# data = {'secCode': '000509', 'market': 'A', 'pn': '2'}
# res = session.post(company_url, headers=header, data=data)
# json_data = json.loads(res.text)
# downpath = jsonpath.jsonpath(json_data, '$..title')
# print(downpath)

