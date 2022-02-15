import tushare as ts
import pandas as pd
import requests
from bs4 import BeautifulSoup


ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
pro = ts.pro_api()
df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date')
df_company_list.set_index('ts_code', inplace=True)



company_symbol = df_company_list.symbol[1]
header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/97.0.4692.99 Safari/537.36",
            "Connection": "close"
}
company_url = f'http://doc.rongdasoft.com/stockInfo/stockBaseInfo?secCode={company_symbol}&type=8&module=A'
res = requests.get(company_url,headers=header)
soup = BeautifulSoup(res.content,'html.parser')
print(soup)