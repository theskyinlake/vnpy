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
from examples.TushareDataService.conner import engine#,check_sql ,db,insert_sql, del_sql
from examples.TushareDataService.getdata import get_trade_cal , get_last_trade_cal , get_last_trade_cal1,get_fina_indicator, get_basic, get_code_list_300, get_code_list_all #, get_moneyflow
print(get_trade_cal, get_last_trade_cal, get_last_trade_cal1)

#################################################################################


#----------------------------------------------------------------------
def get_code():
    """获取所有上市合约代码"""
    codes = pro.stock_basic(list_status='L').ts_code.values
    print(codes,len(codes))
    return codes

#----------------------------------------------------------------------
#if_exists字段定义:
#fail的意思如果表存在，啥也不做
#replace的意思，如果表存在，删了表，再建立一个新表，把数据插入
#append的意思，如果表存在，把数据插入，如果表不存在创建一个表！！
def insert_sql(data,tb_name,exists):
    #使用try...except..continue避免出现错误，运行崩溃
    try:
        data.to_sql(tb_name,engine,index=False,if_exists=exists)
        #print( code +'写入数据库成功')
    except:
        pass

#----------------------------------------------------------------------
def get_data(threadID,dbname,stock_pool,start_dt,end_dt,exists):
    n=len(stock_pool)
    for code in  stock_pool:
        n=n-1
        if threadID ==1001:
            df = ts.pro_bar(ts_code=code,  start_date=start_dt, end_date=end_dt,adjfactor='True')
        elif threadID ==1002:
            df = pro.adj_factor(ts_code=code, trade_date='') #复权因子
        elif threadID ==1003:
            df = pro.income(ts_code=code, start_date=start_dt, end_date=end_dt)  #利润表
        elif threadID ==1004:
            df= pro.balancesheet(ts_code=code, start_date=start_dt, end_date=end_dt) #资产负债表
        elif threadID ==1005:
            df = pro.cashflow(ts_code=code, start_date=start_dt, end_date=end_dt) # 现金流量表
        elif threadID ==1006:
            df= pro.query('fina_indicator', ts_code=code, start_date=start_dt, end_date=end_dt,fields='ts_code,end_date,q_netprofit_yoy') # 财务指标数据
        elif threadID ==1007:
            df = pro.forecast(ts_code=code, start_date=start_dt, end_date=end_dt) # 业绩预告
        elif threadID ==1008:
            df= pro.express(ts_code=code, start_date=start_dt, end_date=end_dt) #业绩快报
        elif threadID ==1009:
            df= pro.daily_basic(ts_code=code, start_date=start_dt,  fields='ts_code,trade_date,pe,pe_ttm') #每日指标
        else:
            pass
        insert_sql(df,dbname,exists)
        #time.sleep(3)  # 数据接口限制,每分钟最多访问该接口80次
        print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+'  完成:',start_dt,end_dt,code,threadID,dbname,'还剩下：'+str(n))
#df = ts.pro_bar(ts_code='000001.SH', asset='I', start_date=start_dt, end_date=end_dt)
#insert_sql(df, 'stock_data_szzs',exists)
#----------------------------------------------------------------------
# 创建进程类
class myThread(threading.Thread):
    # 构造函数
    def __init__(self, threadID, dbname,stock_pool,start_dt,end_dt,exists):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.dbname = dbname
        self.stock_pool = stock_pool
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.exists = exists

    # 重写run()
    def run(self):
        print("Thread Strat：" + str(self.threadID) +"  "+ self.dbname)
        get_data(self.threadID,self.dbname,self.stock_pool,self.start_dt,self.end_dt,self.exists)
        print("Thread Exit：" + str(self.threadID) +"  "+ self.dbname)
'''
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

#----------------------------------------------------------------------
def downMinuteBarBySymbol(symbol):
    """下载某一合约的分钟线数据"""


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
#----------------------------------------------------------------------
def downloadAllMinuteBar(start_dt,exists):
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print('开始下载合约分钟线数据')
    print('-' * 50)
    
    # 添加下载任务
    #for symbol in SYMBOLS:
    #   downMinuteBarBySymbol(str(symbol))
    # 取值时间节点
    #start_dt = '20200401'
    end_dt = str(time.strftime("%Y%m%d", time.localtime()))
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # time_temp =datetime.datetime.now() - datetime.timedelta(days=1)
    # end_dt = time_temp.strftime('%Y%m%d')
    # stock_pool =['000001.SZ']
    stock_pool = get_code()

    thread1 = myThread(1001, "stock_data_day", stock_pool, start_dt, end_dt,exists)
    # thread2 = myThread(1002, "stock_data_month", stock_pool, start_dt, end_dt)
    thread2 = myThread(1002, "stock_data_adj_factor", stock_pool, start_dt, end_dt,exists)  # 复权
    thread3 = myThread(1003, "stock_data_income", stock_pool, start_dt, end_dt,exists)  # 利润表
    thread4 = myThread(1004, "stock_data_balancesheet", stock_pool, start_dt, end_dt,exists)  # 资产负债表
    thread5 = myThread(1005, "stock_data_cashflow", stock_pool, start_dt, end_dt,exists)  # 现金流量表
    thread6 = myThread(1006, "stock_data_fina_indicator", stock_pool, start_dt, end_dt,exists)  # 财务指标数据
    thread7 = myThread(1007, "stock_data_forecast", stock_pool, start_dt, end_dt,exists)  # 业绩预告
    thread8 = myThread(1008, "stock_data_express", stock_pool, start_dt, end_dt,exists)  # 业绩快报
    thread9 = myThread(1009, "stock_data_daily_basic", stock_pool, start_dt, end_dt,exists)  # 每日指标
    # 开启线程
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()
    thread9.join()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start Time:", start_time)
    print("All over Time:", end_time)

    print('-' * 50)
    print('合约分钟线数据下载完成')
    print('-' * 50)
#----------------------------------------------------------------------
