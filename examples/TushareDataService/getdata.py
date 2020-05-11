#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import time
import datetime
import numpy as np
import pandas as pd
from .config import pro
###############################################################################
# 获取交易日的日期
def get_trade_date():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=20)
    start_dt = time_temp.strftime('%Y%m%d')
    df_trade = pro.trade_cal(exchange='', start_date=start_dt, end_date=dt_now)
    df_trade = df_trade.sort_values(by=['cal_date'], ascending=0)
    df_trade = df_trade[df_trade['is_open'] == 1]
    df_trade = df_trade.reset_index(drop=True)
    trade_date0 = df_trade.loc[0, 'cal_date'] # 获取当前交易日的日期
    trade_date1 = df_trade.loc[1, 'cal_date'] # 获取上一个交易日的日期
    trade_date2 = df_trade.loc[2, 'cal_date'] # 获取上上一个交易日的日期
    return trade_date0,trade_date1,trade_date2
#####################################################################
get_trade_cal , get_last_trade_cal , get_last_trade_cal1 = get_trade_date()

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
    dff1 = pro.fina_indicator_vip(period=dt1,fields='ts_code,end_date,ocf_to_or,interestdebt,salescash_to_or,int_to_talcap,eqt_to_interestdebt,ocf_to_interestdebt,cash_to_liqdebt_withinterest,bps,bps_yoy,eps,dt_eps,basic_eps_yoy,dt_eps_yoy,inv_turn,grossprofit_margin,ocfps,cfps,cfps_yoy,op_yoy,or_yoy,tr_yoy,ebt_yoy, netprofit_yoy,q_netprofit_yoy,q_netprofit_qoq,q_gr_qoq,q_sales_qoq,q_gr_yoy,q_profit_qoq,q_profit_yoy,q_sales_yoy,current_ratio,quick_ratio,roe,roa,debt_to_assets,eqt_to_debt,cash_to_liqdebt')
    dff2 = pro.fina_indicator_vip(period=dt2,fields='ts_code,end_date,ocf_to_or,salescash_to_or,interestdebt,int_to_talcap,eqt_to_interestdebt,ocf_to_interestdebt,cash_to_liqdebt_withinterest,bps,bps_yoy,eps,dt_eps,basic_eps_yoy,dt_eps_yoy,inv_turn,grossprofit_margin,ocfps,cfps,cfps_yoy,op_yoy,or_yoy,tr_yoy,ebt_yoy, netprofit_yoy,q_netprofit_yoy,q_netprofit_qoq,q_gr_qoq,q_sales_qoq,q_gr_yoy,q_profit_qoq,q_profit_yoy,q_sales_yoy,current_ratio,quick_ratio,roe,roa,debt_to_assets,eqt_to_debt,cash_to_liqdebt')
    list_f = dff1.ts_code.values.tolist()
    dff2 = dff2[~dff2.ts_code.isin(list_f)]
    dff = pd.concat([dff1, dff2], ignore_index=True)
    dff = dff.drop_duplicates(subset=None, keep='first', inplace=False)
    return dff


##########################估值############################
def get_basic():
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    if dt_now == get_trade_cal:
        if dt_h < '16':
            dt01 = get_last_trade_cal
        else:
            dt01 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt01 = get_trade_cal
    dfdb = pro.daily_basic(ts_code='', trade_date=dt01, fields='ts_code,trade_date,close,turnover_rate,volume_ratio,pb,pe_ttm,ps_ttm,dv_ttm,total_mv,circ_mv')
    dfsb = pro.stock_basic(exchange='', list_status='L')
    dfb = pd.merge(dfdb, dfsb, on=['ts_code'])
    dfb = dfb[['ts_code', 'symbol', 'name', 'area', 'industry', 'market','trade_date','close', 'pe_ttm', 'ps_ttm', 'dv_ttm','pb', 'turnover_rate', 'volume_ratio','total_mv','circ_mv']]
    return dfb


##################资金流向##############
def get_moneyflow():
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    if dt_now == get_trade_cal:
        if dt_h < '17':
            dt02 = get_last_trade_cal
        else:
            dt02 = str(time.strftime("%Y%m%d", time.localtime()))
    else:
        dt02 = get_trade_cal
    dfm = pro.moneyflow(trade_date=dt02)
    dfm['smv'] = dfm['buy_sm_vol'] - dfm['sell_sm_vol']
    dfm['mdv'] = dfm['buy_md_vol'] - dfm['sell_md_vol']
    dfm['lgv'] = dfm['buy_lg_vol'] - dfm['sell_lg_vol']
    dfm['elgv'] = dfm['buy_elg_vol'] - dfm['sell_elg_vol']
    dfm['sgv'] = dfm['elgv'] + dfm['lgv']
    dfm = dfm[['ts_code', 'smv', 'mdv', 'lgv', 'elgv', 'sgv', 'net_mf_vol']]
    return dfm
############


def get_code_list_all():
    # 获取沪深所有股票
    codes_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,market,industry,list_date')
    # 获取上市超过100天的股票
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

def get_code_list_300():
    # 获取沪深300
    codes_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,market,industry,list_date')
    codes_300 = pro.index_weight(index_code='399300.SZ').head(300)#.con_code.values.tolist()
    codes_300['ts_code']= codes_300['con_code']
    codes_basic=pd.merge(codes_300,codes_basic, on=['ts_code'],how='left')
    # 获取上市超过100天的股票
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