import json
import os
import tushare as ts
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
import jmespath
import threading

global all_company_list


def get_annual_report(company_list):
    global all_company_list
    # 设置参数
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/97.0.4692.99 Safari/537.36",
        "Connection": "close"
    }
    session = requests.Session()
    jmespath_expr = jmespath.compile("files[?ends_with(title, '2021年年度报告')].downpath")
    # 配置日志文件
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler("log.txt")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(thread)d - %(levelname)s- %(message)s')
    handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # 加载股票列表
    df_company_list = company_list
    # 抓取数据
    for i, ts_code in enumerate(df_company_list.index):
        if df_company_list['status'][i] == '已下载':
            continue
            logger.info(f"{ts_code}数据已存在")
        logger.info(f"开始搜索{ts_code}")
        if ts_code[0:3] == '68':
            market = 'K'
            company_url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
        elif ts_code[0] == '8':
            market = 'bjs'
            company_url = 'http://doc.rongdasoft.com/stockInfo/stockContentThree'
        else:
            market = 'A'
            company_url = 'http://doc.rongdasoft.com/stockInfo/stockContent'
        for page in range(1, df_company_list['page_nums'][i] + 1):
            data = {'secCode': ts_code[:6], 'market': market, 'pn': page}
            res = session.post(company_url, headers=header, data=data)
            downpath = jmespath_expr.search(json.loads(res.text))
            print(ts_code,downpath)
        if len(downpath) > 0:
            title = jmespath.search("files[?ends_with(title, '2021年年度报告')].title", json.loads(res.text))
            try:
                if df_company_list.industry[i]:
                    path = f'./{df_company_list.industry[i]}/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                else:
                    path = ''
                files_downpath = session.get(downpath[0], headers=header)
                with open(f'{path}{title}.pdf', 'wb') as f:
                    f.write(files_downpath.content)
                    f.flush()
                f.close()
                logger.info(f"已下载{title}")
                all_company_list.loc[[ts_code], ['status']] = '已下载'
                break
            except Exception as e:
                logger.warning(f"{title}下载失败{e}")
                all_company_list.loc[[ts_code], ['status']] = '下载失败'
        else:
            logger.info(f"{df_company_list.index[i]}未披露21年年报")
            all_company_list.loc[[ts_code], ['status']] = '未披露'
    return df_company_list


def multi_threading_get_annual_report(threading_nums=100):
    global all_company_list
    all_company_list = pd.read_excel('stock_list.xlsx', index_col='ts_code')
    if 'status' not in all_company_list.columns:
        all_company_list['status'] = ''
    the_poor_of_threads = []
    for i, n in enumerate(range(0, len(all_company_list), len(all_company_list) // threading_nums + 1)):
        stars_threading = threading.Thread(target=get_annual_report,
                                           args=(all_company_list[n:n + len(all_company_list) // threading_nums + 1],))
        stars_threading.start()
        the_poor_of_threads.append(stars_threading)
    for threadings in the_poor_of_threads:
        threadings.join()
    all_company_list.to_excel('./stock_list.xlsx')
    print('Done')

company_list = pd.read_excel('stock_list.xlsx', index_col='ts_code')
company_list['status'] = ''
get_annual_report(company_list)