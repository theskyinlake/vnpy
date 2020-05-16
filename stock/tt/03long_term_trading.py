#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-
#投资逻辑：
#券商持仓，总市值小于26亿元；90日360日线多头；不包含ST；非*ST；每股净资产大于1.2；连续放量；资产负债小于20%；非停牌股；市值由小到大 ；
#时机 : 开盘买入
# 持股周期 :   73  天
# 持股上限 :   1 只
# 单日买入数 :   1 只
#止盈 : 收益率 ≥ 23 % 时坚定持有;直到最高收益回落 5 %
# 止损 : 收益率 ≤ - 20 %
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
    dfb=dfb[dfb.ts_code.isin(get_code_list_all())]
    dfbf=pd.merge(dfb,dff, on=['ts_code'],how='left')  
    dfbf=dfbf[dfbf['total_mv']<270000]
    dfbf=dfbf[dfbf['bps']>1.2]
    dfbf=dfbf[dfbf['debt_to_assets']<20]
    stock_pool=dfbf.ts_code.values.tolist()
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp =datetime.datetime.now() - datetime.timedelta(days=600)
    start_dt = time_temp.strftime('%Y%m%d')
    if dt_now == get_trade_cal:
        if dt_h < '17':
            dt01 = get_last_trade_cal
        else:
            dt01 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt01 = get_trade_cal
    n=len(stock_pool)
    del_sql('stock_data_ma90_ma360')
    del_sql('long_term_trading'+dt01)
    #print(stock_pool)
    print(n)
    for code in  stock_pool:
        n=n-1
        dfd = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt, end_date=dt01,ma=[3,5,10])
        #print(dfd)       
        dfd=dfd.sort_values(by=['trade_date'], ascending=1)   
        #dfd['RSI'] = ta.RSI(dfd['close'].values, timeperiod=6)  # RSI的天数一般是6、12、24
        #dfd['WILLR'] = ta.WILLR(dfd['high'].values, dfd['low'].values, dfd['close'].values, timeperiod=10)
        dfd['MA90'] = ta.MA(dfd['close'].values, timeperiod=90) 
        dfd['MA360'] = ta.MA(dfd['close'].values, timeperiod=360) 
        dfd=dfd.tail(1)    
        #dfd['WILLRabs']= dfd['WILLR'].abs()
        dfd=dfd.sort_values(by=['trade_date'], ascending=0) 
        dfd.loc[dfd['ma_v_3']>dfd['ma_v_5'], 'col_A'] = 1       
        dfd.loc[dfd['MA90']>dfd['MA360'], 'col_B'] = 1       
        #print(dfd) 
        #print(n)
        insert_sql(dfd,'stock_data_ma90_ma360','append') 
    ##################################################
    sql='''SELECT DISTINCT  *  FROM stock_data_ma90_ma360;'''
    dfs = pd.read_sql_query(sql, engine)  
    dfs=dfs.fillna(0)
    dfs=dfs[dfs['col_A']>0]
    dfs=dfs[dfs['col_B']>0]
    ##################################################
    dfma=pd.merge(dfbf,dfs, on=['ts_code'],how='right')   
    dfma=dfma.sort_values(by=['total_mv'], ascending=1) 
    dfma= dfma.reset_index(drop=True)
    dfma = dfma.head(5)
    insert_sql(dfma,'long_term_trading'+dt01,'append')
    print(dfma)

