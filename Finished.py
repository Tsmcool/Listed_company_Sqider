import json
import os
import tushare as ts
import logging
import pandas as pd
import requests
import requests_cache
import jmespath
import threading
import numpy as np
from multiprocessing import  Pool
from functools import partial


# 定义全局变量
global all_company_list, header, session
header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/97.0.4692.99 Safari/537.36",
            "Connection": "close"
}
session = requests.Session()
requests_cache.install_cache('stock_cache')


def parallelize(data, func, num_of_processes=8):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)


def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)


# 获取pages
def get_stock_pages(row):
    if row.market == 'bjs':
        url = 'http://doc.rongdasoft.com/stockInfo/stockContentThree'
    else:
        url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
    data = {'secCode': row.symbol, 'market': row.market, 'pn': ''}
    res = session.post(url=url, headers=header, data=data)
    pages = jmespath.search('page_num', json.loads(res.text))
    return pages


def get_annual_report(row):
    # 配置log文件
    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    handler = logging.FileHandler("log.txt")
    logger.addHandler(console)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(thread)d - %(levelname)s- %(message)s')
    console.setFormatter(formatter)
    handler.setFormatter(formatter)
    if row.market == 'bjs':
        url = 'http://doc.rongdasoft.com/stockInfo/stockContentThree'
    else:
        url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
    page_nums = row.page_nums
    jmespath_expr = jmespath.compile("files[?ends_with(title, '2021年年度报告')].downpath")
    path = f'../{row.industry}/'
    for pages in np.arange(1, page_nums+1):
        data = {'secCode': row.ts_code[0:6], 'market': row.market, 'pn': pages}
        res = session.post(url=url, headers=header, data=data)
        downpath = jmespath_expr.search(json.loads(res.text))
        if len(downpath) > 0:
            title = jmespath.search("files[?ends_with(title, '2021年年度报告')].title", json.loads(res.text))[0]
            if not os.path.exists(path):
                os.makedirs(path)
            elif os.path.exists(f'{path}{title}.pdf'):
                status = '已下载'
                break
            try:
                files_downpath = session.get(downpath[0], headers=header)
                with open(f'{path}{title}.pdf', 'wb') as f:
                    f.write(files_downpath.content)
                    f.flush()
                f.close()
                logger.info(f"已下载{title}")
                status = '已下载'
                break
            except Exception as e:
                logger.warning(f"{title}下载失败{e}")
                status = '下载失败'
                break
    if pages == page_nums and len(downpath) == 0:
        logger.info(f"{row.ts_code[0:6]}未披露21年年报")
        status = '未披露'
    logger.removeHandler(console)
    logger.removeHandler(handler)
    return status


# tushare抓取股票清单
def get_stock_list():
    if os.path.exists('../stock_list.xlsx'):
        _ = input("是否更新股票列表?(y/n)")
        if _ == 'n':
            df_company_list = pd.read_excel('../stock_list.xlsx')
        else:
            ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
            pro = ts.pro_api()
            df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date,market')
            df_company_list.loc[df_company_list['industry'].isnull(), ['industry']] = '北交所'
            df_company_list['page_nums'] = ''
            df_company_list.loc[df_company_list['symbol'].str.contains(r'^68\d{4}', regex=True), ['market']] = 'K'
            df_company_list.loc[df_company_list['symbol'].str.contains(r'^8\d{5}', regex=True), ['market']] = 'bjs'
            df_company_list.loc[df_company_list['symbol'].str.contains(r'[^8][^8]\d{4}', regex=True), ['market']] = 'A'
            df_company_list['page_nums'] = parallelize_on_rows(df_company_list, get_stock_pages)
            # df_company_list['page_nums'] = df_company_list.apply(get_stock_pages, axis=1)
            df_company_list.to_excel('../stock_list.xlsx')
    else:
        ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
        pro = ts.pro_api()
        df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date,market')
        df_company_list.loc[df_company_list['industry'].isnull(), ['industry']] = '北交所'
        df_company_list['page_nums'] = ''
        df_company_list.loc[df_company_list['symbol'].str.contains(r'^68\d{4}', regex=True), ['market']] = 'K'
        df_company_list.loc[df_company_list['symbol'].str.contains(r'^8\d{5}', regex=True), ['market']] = 'bjs'
        df_company_list.loc[df_company_list['symbol'].str.contains(r'[^8][^8]\d{4}', regex=True), ['market']] = 'A'
        df_company_list['page_nums'] = parallelize_on_rows(df_company_list, get_stock_pages)
        df_company_list.to_excel('../stock_list.xlsx')
    return df_company_list


if __name__ == '__main__':
    df_company_list = get_stock_list()
    df_company_list['status'] = ''
    df_company_list['status'] = parallelize_on_rows(df_company_list, get_annual_report)
    df_company_list.to_excel('../stock_list.xlsx')

