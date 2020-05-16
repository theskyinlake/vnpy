# encoding: UTF-8

#from vnpy.trader.vtObject import VtBarData
#from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME

##########################################################################
import time
import datetime
import threading
#import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
from examples.TushareDataService.config import pro
from examples.TushareDataService.conner import engine,db,insert_sql
from examples.TushareDataService.getdata import  get_trade_date


#################################################################################


#----------------------------------------------------------------------
def get_code():
    """获取所有上市合约代码"""
    codes = pro.stock_basic(list_status='L').ts_code.values
    print(codes,len(codes))
    return codes

#----------------------------------------------------------------------
'''
#----------------------------------------------------------------------
def get_data(dbname,stock_pool,start_dt,end_dt,exists):
    n=len(stock_pool)
    for code in  stock_pool:
        n=n-1
        df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt, end_date=end_dt)
        insert_sql(df,dbname,exists)
        #time.sleep(3)  # 数据接口限制,每分钟最多访问该接口80次
        print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+'  完成:',start_dt,end_dt,code,dbname,'还剩下：'+str(n))
#df = ts.pro_bar(ts_code='000001.SH', asset='I', start_date=start_dt, end_date=end_dt)
#insert_sql(df, 'stock_data_szzs',exists)
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def generateExchange(symbol):
    """生成VT合约代码"""
    if symbol[0:2] in ['60', '51']:
        exchange = 'SSE'
    elif symbol[0:2] in ['00', '15', '30']:
        exchange = 'SZSE'
    return exchange

#----------------------------------------------------------------------
def generateVtBar(row):
    """生成K线"""
    bar = VtBarData()
    ###result= pd.DataFrame()
    bar.symbol = row['code']
    bar.exchange = generateExchange(bar.symbol)
    bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
    bar.open = row['open']
    bar.high = row['high']
    bar.low = row['low']
    bar.close = row['close']
    bar.volume = row['vol']
    bar.datetime = row.name
    bar.date = bar.datetime.strftime("%Y%m%d")
    bar.time = bar.datetime.strftime("%H:%M:%S")
    
    return bar
'''
#----------------------------------------------------------------------
def downMinuteBarBySymbol(code,start_dt,end_dt):
    """下载某一合约的分钟线数据"""
    df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt, end_date=end_dt)

    '''
    start = time()

    cl = db[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    df = ts.bar(symbol, ktype='1min')
    df = df.sort_index()
    
    for ix, row in df.iterrows():
        bar = generateVtBar(row)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)            

    end = time()
    cost = (end - start) * 1000

    print('合约%s数据下载完成%s - %s，耗时%s毫秒' %(symbol, df.index[0], df.index[-1], cost))
    '''
    return  df
#----------------------------------------------------------------------
def downloadAllMinuteBar(start_dt,end_dt,exists):
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print('开始下载合约分钟线数据')
    print('-' * 50)

    # 取值时间节点
    #start_dt = '20200401'

    now_time = str(time.strftime("%Y%m%d", time.localtime()))
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # time_temp =datetime.datetime.now() - datetime.timedelta(days=1)
    # end_dt = time_temp.strftime('%Y%m%d')
    # stock_pool =['000001.SZ']
    if end_dt != '' and len(end_dt)==8 and end_dt<'now_time' :
        end_dt0=end_dt
    else:
        end_dt0=now_time

    stock_pool = get_code()
    dbname='stock_data_day'
    n = len(stock_pool)
    # 添加下载任务
    for code in stock_pool:
        n = n - 1
        df=downMinuteBarBySymbol(code,start_dt,end_dt0)
        insert_sql( code,df, dbname, exists)
        # time.sleep(3)  # 数据接口限制,每分钟最多访问该接口80次
        print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + '  完成:', start_dt, end_dt, code, dbname,'还剩下：' + str(n))
    #get_data(dbname,stock_pool,start_dt,end_dt0,exists)
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start Time:", start_time)
    print("All over Time:", end_time)

    print('-' * 50)
    print('合约分钟线数据下载完成')
    print('-' * 50)
#----------------------------------------------------------------------
