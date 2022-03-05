import json
import tushare as ts
import logging
import pandas as pd
import requests
import jmespath


ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
pro = ts.pro_api()
header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/97.0.4692.99 Safari/537.36",
            "Connection": "close"
}
session = requests.Session()


df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date,market')
df_company_list.set_index('ts_code', inplace=True)
df_company_list['page_nums'] = ''
for i, symbol in enumerate(df_company_list.symbol):
    if symbol[0:3] == '68':
        data = {'secCode': symbol, 'market': 'K', 'pn': ''}
        company_url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
    elif symbol[0] == '8':
        data = {'secCode': symbol, 'market': 'bjs', 'pn': ''}
        company_url = 'http://doc.rongdasoft.com/stockInfo/stockContentThree'
    else:
        data = {'secCode': symbol, 'market': 'A', 'pn': ''}
        company_url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
    res = session.post(company_url, headers=header, data=data)
    pages = jmespath.search('page_num', json.loads(res.text))
    df_company_list.iloc[[i], [5]] = pages
df_company_list.to_excel('./stock_list.xlsx')