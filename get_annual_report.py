import json
import os
import tushare as ts
import logging
import pandas as pd
import requests
import jmespath
import threading


# 定义全局变量
global all_company_list, header, session
header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/97.0.4692.99 Safari/537.36",
            "Connection": "close"
}
session = requests.Session()


def get_stock_list():
    ts.set_token('63b6038d68dd853d09b4652260647332328e6434c37a41d90facbcda')
    pro = ts.pro_api()
    df_company_list = pro.stock_basic(fields='ts_code,symbol,name,industry,list_date,market')
    df_company_list.set_index('ts_code', inplace=True)
    df_company_list['page_nums'] = ''
    for i, symbol in enumerate(df_company_list.symbol):
        if symbol[0:2] == '68':
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


def get_annual_report(company_list):
    global all_company_list
    # 设置参数
    jmespath_expr = jmespath.compile("files[?ends_with(title, '2021年年度报告')].downpath")
    # 配置日志文件
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler("log.txt")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(thread)d - %(levelname)s- %(message)s')
    handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)
    # 加载股票列表
    df_company_list = company_list
    downpath_list = []
    # 抓取数据
    for i, ts_code in enumerate(df_company_list.index):
        if df_company_list['status'][i] == '已下载':
            logger.info(f"{ts_code}数据已存在")
            continue
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
            downpath_list.append(downpath)
            if len(downpath) > 0:
                title = jmespath.search("files[?ends_with(title, '2021年年度报告')].title", json.loads(res.text))[0]
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
                    all_company_list.loc[[ts_code], ['status']] = '下载失败'
                    logger.warning(f"{title}下载失败{e}")
        if page == df_company_list['page_nums'][i] and len(downpath) == 0:
            logger.info(f"{df_company_list.index[i]}未披露21年年报")
            all_company_list.loc[[ts_code], ['status']] = '未披露'


def multi_threading_get_annual_report(threading_nums=100):
    global all_company_list
    if os.path.exists('stock_list.xlsx'):
        _ = input("是否更新股票列表?(y/n)")
        if _ == 'n':
            pass
        else:
            get_stock_list()
    else:
        get_stock_list()
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


multi_threading_get_annual_report(100)
