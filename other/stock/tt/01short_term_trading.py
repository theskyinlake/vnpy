#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-
#投资逻辑：
#非ST;非科创；非退市；无退市警示，上市时间大于60日，量比>0.71，流通市值<150亿;换手率>5.9%且换手率<9%;WR1大于21；RSI6大于33；当日 周平均换手率从大到小排名
#时机 : 开盘买入
# 持股周期 :   2  天
# 持股上限 :   1 只
# 单日买入数 :   1 只
#止盈 : 收益率 ≥ 17.6 % 时坚定持有;直到最高收益回落 2 %
# 止损 : 收益率 ≤ - 8 %
# 仓位控制 : 满仓

import time
import datetime
import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
from hc.conner import db,engine,insert_sql,del_sql
from hc.config import pro, get_trade_cal, get_last_trade_cal,get_last_trade_cal1
from hc.getdata import get_fina_indicator,get_basic,get_moneyflow,get_code_list_300,get_code_list_all

if __name__ == '__main__':
    dfb = get_basic()
    dff = get_fina_indicator()
    dfb = dfb[dfb.ts_code.isin(get_code_list_all())]
    dfbf = pd.merge(dfb, dff, on=['ts_code'], how='left')
    dfbf = dfbf[dfbf['circ_mv'] < 1500000]
    dfbf = dfbf[dfbf['volume_ratio'] > 0.71]
    dfbf = dfbf[dfbf['turnover_rate'] > 5.9]
    dfbf = dfbf[dfbf['turnover_rate'] < 9]
    stock_pool = dfbf.ts_code.values.tolist()
    #######################################################
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=20)
    start_dt1 = time_temp.strftime('%Y%m%d')
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1200)
    start_dt2 = time_temp.strftime('%Y%m%d')
    if dt_now == get_trade_cal:
        if dt_h < '17':
            dt01 = get_last_trade_cal
        else:
            dt01 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt01 = get_trade_cal
    ##################
    n = len(stock_pool)
    del_sql('stock_data_tor')
    del_sql('stock_data_weektor')
    del_sql('Short_term_trading'+dt01)
    print(n)
    print(dt01)
    for code in stock_pool:
        n = n - 1
        #周平均换手率
        dft = pro.daily_basic(ts_code=code, start_date=start_dt1, end_date=dt01,fields='ts_code,trade_date,turnover_rate,turnover_rate_f')
        dft = dft.sort_values(by=['trade_date'], ascending=1)
        dft['MA5'] = ta.MA(dft['turnover_rate'].values, timeperiod=4)
        dft = dft.tail(1)
        dft = dft.sort_values(by=['trade_date'], ascending=0)
        # 指标rsi willr
        dfd = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt2, end_date=dt_now)
        dfd = dfd.sort_values(by=['trade_date'], ascending=1)
        dfd['RSI'] = ta.RSI(dfd['close'].values, timeperiod=6)  # RSI的天数一般是6、12、24
        dfd['WILLR'] = ta.WILLR(dfd['high'].values, dfd['low'].values, dfd['close'].values, timeperiod=10)
        dfd = dfd.tail(1)
        dfd['WILLRabs'] = dfd['WILLR'].abs()
        dfd = dfd.sort_values(by=['trade_date'], ascending=0)
        insert_sql(dft, 'stock_data_weektor', 'append')
        insert_sql(dfd, 'stock_data_tor', 'append')
    #########################################################
    sql1 = '''SELECT DISTINCT  *  FROM stock_data_tor ;'''
    dfs1 = pd.read_sql_query(sql1, engine)
    dfs1 = dfs1.fillna(0)
    ##################################################
    sql2 = '''SELECT DISTINCT  *  FROM stock_data_weektor ;'''
    dfs2 = pd.read_sql_query(sql2, engine)
    dfs2 = dfs2.fillna(0)
    ##################################################
    df11 = pd.merge(dfbf, dfs1, on=['ts_code'], how='right')
    df22 = pd.merge(dfbf, dfs2, on=['ts_code'], how='right')
    df11 = df11[df11['RSI'] > 33]
    df11 = df11[df11['WILLRabs'] > 21]
    stock_list = df11.ts_code.values.tolist()
    df = df22[df22.ts_code.isin(stock_list)]
    df = df.sort_values(by=['MA5'], ascending=0)
    df1 = df.head(5)
    insert_sql(df1, 'Short_term_trading'+dt01, 'append')
    print(df.head(5))
