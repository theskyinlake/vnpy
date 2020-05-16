#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import time
import datetime
import pymysql
import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
import mysql.connector
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 100)  # 显示的最大行数（避免只显示部分行数据）
pd.set_option('display.max_columns', 1000)  # 显示的最大列数（避免列显示不全）
pd.set_option("display.max_colwidth", 1000)  # 每一列最大的宽度（避免属性值或列名显示不全）
pd.set_option('display.width', 1000)  # 每一行的宽度（避免换行）
# 设置token及数据库连接
ts.set_token('809eb4ebed2618990f0da9579f701706196b84216a9d7302fa08ac26')
pro = ts.pro_api()

# 初始化数据库连接，使用pymysql模块
db = pymysql.connect("127.0.0.1", "root", "shmily789", "stock")
# engine = create_engine('mysql+pymysql://root:shmily789@127.0.0.1:3306/stock?charset=utf8')
engine = create_engine('mysql+mysqlconnector://root:shmily789@127.0.0.1:3306/stock?charset=utf8')


######################利润#########################
def get_fina_indicator():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    the_year = int(str(dt_now)[0:4])
    the_month = str(dt_now)[4:6]
    if the_month < '04':
        dt1 = str(the_year - 1) + '1231'
        dt2 = str(the_year - 1) + '0930'
    elif the_month >= '04' and the_month < '07':
        dt1 = str(the_year) + '0331'
        dt2 = str(the_year - 1) + '1231'
    elif the_month >= '07' and the_month < '10':
        dt1 = str(the_year) + '0630'
        dt2 = str(the_year) + '0331'
    elif the_month >= '10':
        dt1 = str(the_year) + '0930'
        dt2 = str(the_year) + '0630'
    else:
        print('this wrong')
    dff1 = pro.fina_indicator_vip(period=dt1,
                                  fields='ts_code,end_date,interestdebt,int_to_talcap,eqt_to_interestdebt,ocf_to_interestdebt,cash_to_liqdebt_withinterest,bps,bps_yoy,eps,dt_eps,basic_eps_yoy,dt_eps_yoy,inv_turn,grossprofit_margin,ocfps,cfps,cfps_yoy,op_yoy,or_yoy,tr_yoy,ebt_yoy, netprofit_yoy,q_netprofit_yoy,q_netprofit_qoq,q_gr_qoq,q_sales_qoq,q_gr_yoy,q_profit_qoq,q_profit_yoy,q_sales_yoy,current_ratio,quick_ratio,roe,roa,debt_to_assets,eqt_to_debt,cash_to_liqdebt')
    dff2 = pro.fina_indicator_vip(period=dt2,
                                  fields='ts_code,end_date,interestdebt,int_to_talcap,eqt_to_interestdebt,ocf_to_interestdebt,cash_to_liqdebt_withinterest,bps,bps_yoy,eps,dt_eps,basic_eps_yoy,dt_eps_yoy,inv_turn,grossprofit_margin,ocfps,cfps,cfps_yoy,op_yoy,or_yoy,tr_yoy,ebt_yoy, netprofit_yoy,q_netprofit_yoy,q_netprofit_qoq,q_gr_qoq,q_sales_qoq,q_gr_yoy,q_profit_qoq,q_profit_yoy,q_sales_yoy,current_ratio,quick_ratio,roe,roa,debt_to_assets,eqt_to_debt,cash_to_liqdebt')
    list_f = dff1.ts_code.values.tolist()
    dff2 = dff2[~dff2.ts_code.isin(list_f)]
    dff = pd.concat([dff1, dff2], ignore_index=True)
    dff = dff.drop_duplicates(subset=None, keep='first', inplace=False)
    return dff


##########################估值############################
def get_basic():
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    if dt_now == get_trade_cal():
        if dt_h < '16':
            dt01 = get_last_trade_cal()
        else:
            dt01 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt01 = get_trade_cal()
    dfdb = pro.daily_basic(ts_code='', trade_date=dt01,
                           fields='ts_code,trade_date,close,turnover_rate,volume_ratio,turnover_rate_f,pb,pe_ttm,ps_ttm,dv_ttm,circ_mv,total_mv')
    dfsb = pro.stock_basic(exchange='', list_status='L')
    dfb = pd.merge(dfdb, dfsb, on=['ts_code'])
    dfb = dfb[
        ['ts_code', 'symbol', 'name', 'area', 'industry', 'market', 'pe_ttm', 'ps_ttm', 'dv_ttm', 'pb', 'turnover_rate',
         'turnover_rate_f', 'volume_ratio', 'circ_mv', 'total_mv']]
    return dfb


##################资金流向##############
def get_moneyflow():
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    if dt_now == get_trade_cal():
        if dt_h < '17':
            dt02 = get_last_trade_cal()
        else:
            dt02 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt02 = get_trade_cal()
    dfm = pro.moneyflow(trade_date=dt02)
    dfm['smv'] = dfm['buy_sm_vol'] - dfm['sell_sm_vol']
    dfm['mdv'] = dfm['buy_md_vol'] - dfm['sell_md_vol']
    dfm['lgv'] = dfm['buy_lg_vol'] - dfm['sell_lg_vol']
    dfm['elgv'] = dfm['buy_elg_vol'] - dfm['sell_elg_vol']
    dfm['sgv'] = dfm['elgv'] + dfm['lgv']
    dfm = dfm[['ts_code', 'smv', 'mdv', 'lgv', 'elgv', 'sgv', 'net_mf_vol']]
    return dfm


##########################################

# 获取当前交易日的日期
def get_trade_cal():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=10)
    start_dt = time_temp.strftime('%Y%m%d')
    df_trade = pro.trade_cal(exchange='', start_date=start_dt, end_date=dt_now)
    df_trade = df_trade.sort_values(by=['cal_date'], ascending=0)
    df_trade = df_trade[df_trade['is_open'] == 1]
    df_trade = df_trade.reset_index(drop=True)
    trade_date = df_trade.loc[0, 'cal_date']
    return trade_date


# 获取上一个交易日的日期
def get_last_trade_cal():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=10)
    start_dt = time_temp.strftime('%Y%m%d')
    df_trade = pro.trade_cal(exchange='', start_date=start_dt, end_date=dt_now)
    df_trade = df_trade.sort_values(by=['cal_date'], ascending=0)
    df_trade = df_trade[df_trade['is_open'] == 1]
    df_trade = df_trade.reset_index(drop=True)
    trade_date = df_trade.loc[1, 'cal_date']
    return trade_date


# 获取上上一个交易日的日期
def get_last_trade_cal1():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=10)
    start_dt = time_temp.strftime('%Y%m%d')
    df_trade = pro.trade_cal(exchange='', start_date=start_dt, end_date=dt_now)
    df_trade = df_trade.sort_values(by=['cal_date'], ascending=0)
    df_trade = df_trade[df_trade['is_open'] == 1]
    df_trade = df_trade.reset_index(drop=True)
    trade_date = df_trade.loc[2, 'cal_date']
    return trade_date


def insert_sql(data, tb_name, exists):
    # 使用try...except..continue避免出现错误，运行崩溃
    try:
        data.to_sql(tb_name, engine, index=False, if_exists=exists)
        # print( code +'写入数据库成功')
    except:
        pass


def del_sql(tb_name):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 更新语句
    # cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
    sql = "drop table IF EXISTS " + tb_name
    try:
        # 执行SQL语句
        cursor.execute(sql)
        print(tb_name + '删掉完成')
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        # db.rollback()
        print(tb_name + 'sql wrong')
        pass
        # 关闭数据库连接
    # db.close()


def get_code_list():
    # 获取上市超过60天的股票
    codes_basic = pro.stock_basic(exchange='', list_status='L',
                                  fields='ts_code,symbol,name,area,market,industry,list_date')
    # codes_basic.to_sql('stock_basic',engine,index=False,if_exists='replace')
    time_temp = datetime.datetime.now() - datetime.timedelta(days=60)
    dt_100 = time_temp.strftime('%Y%m%d')
    codes_basic_100 = codes_basic.loc[codes_basic['list_date'] < dt_100].ts_code.values.tolist()

    # 获取ST股票
    st = codes_basic[codes_basic['name'].str.contains('ST')].ts_code.values.tolist()
    n = len(st)
    for code in st:
        if codes_basic_100.count(code) > 0:
            codes_basic_100.remove(code)
            # print('st:',n,len(codes_basic_100),code)
            n = n - 1
        else:
            pass

    # 获取停牌的股票
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    codes_suspend = pro.suspend(ts_code='', suspend_date=dt_now, resume_date='', fields='').ts_code.values.tolist()
    n = len(codes_suspend)
    for code in codes_suspend:
        if codes_basic_100.count(code) > 0:
            codes_basic_100.remove(code)
            # print('tp:',n,len(codes_basic_100),code)
            n = n - 1
        else:
            pass

    return codes_basic_100


####################################################
if __name__ == '__main__':
    list_o = ['002734.SZ', '300443.SZ', '000069.SZ', '601186.SH']
    #list_ths = ['002283.SZ', '600642.SH', '000573.SZ', '002545.SZ', '601900.SH', '002457.SZ', '000582.SZ', '600351.SH','000910.SZ', '603017.SH', '002641.SZ']
    list_cc = list_o #+ list_ths
    dfb = get_basic()
    dff = get_fina_indicator()
    dfm = get_moneyflow()

    df44 = pd.merge(dff, dfm, on=['ts_code'], how='left')
    df55 = pd.merge(dfb, df44, on=['ts_code'])
    # M_current_ratio=df55['current_ratio'].median()
    # M_quick_ratio=df55['quick_ratio'].median()
    # print( M_current_ratio,M_quick_ratio)
    df66 = df55[df55.ts_code.isin(list_cc)]

    dfb = dfb[dfb.ts_code.isin(get_code_list())]
    dfb = dfb[dfb.pe_ttm < 30]
    # dfb=dfb[dfb.pe_ttm>0]
    dfb = dfb[dfb.pb < 3]
