#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import time
import datetime
import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
from hc.config import pro
from hc.DMI import DMI
#from stock.hc.conner import engine,check_sql #,db,insert_sql, del_sql
from hc.getdata import get_trade_cal , get_last_trade_cal , get_last_trade_cal1,get_fina_indicator, get_basic, get_code_list_300, get_code_list_all #, get_moneyflow
print(get_trade_cal, get_last_trade_cal, get_last_trade_cal1)

if __name__ == '__main__':
    list_o = []
    list_ths = ['000069.SZ','600233.SH','601168.SH']
    list_cc = list_o #+ list_ths
    dfb = get_basic()
    dff = get_fina_indicator()
    df55 = pd.merge(dfb, dff, on=['ts_code'])
    df66 = df55[df55.ts_code.isin(list_cc)]
    ####################################################
    M_current_ratio = df55['current_ratio'].median()
    M_quick_ratio = df55['quick_ratio'].median()
    M_eqt_to_debt = df55['eqt_to_debt'].median()
    M_cash_to_liqdebt = df55['cash_to_liqdebt'].median()
    print('流动比率:', M_current_ratio, '速动比率', M_quick_ratio, '现金比率', M_cash_to_liqdebt, '归属于母公司的股东权益/负债合计', M_eqt_to_debt)
    ##############################################################
    #dfb = dfb[dfb.ts_code.isin(get_code_list_300())]
    dfb = dfb[dfb.ts_code.isin(get_code_list_all())]

    dfb = dfb[dfb.pe_ttm < 30]
    dfb = dfb[dfb.pe_ttm > 0]
    dfb = dfb[dfb.pb < 3]
    dfb = dfb[dfb.pb > 0]
    # interestdebt 带息债务
    # ocf_to_or 经营活动产生的现金流量净额/营业收入 即公司的营业收入中多少收回了现金，一般情况下考虑增值税，这个比值在1.16，即经营活动现金流入/营业收入=1.16。
    # salescash_to_or 销售商品提供劳务收到的现金/营业收入  意思是1元的营业收入收到了多少现金。因为增值税的原因，一般情况下，收现比约等于1.16。
    # 虽然净现比和收现比的理论值分别为1.32 和1.16，但是很多公司达不到的，初善君个人认为净现比不低于0.5，收现比不能低于0.8。
    # eqt_to_interestdebt 归属于母公司的股东权益/带息债务
    # ocf_to_interestdebt 经营活动产生的现金流量净额/带息债务
    # cash_to_liqdebt_withinterest	 货币资金／带息流动负债
    # bps_yoy #每股净资产相对年初增长率(%)
    # inv_turn  #存货周转率
    # ocf_to_or,salescash_to_or
    # grossprofit_margin #销售毛利率
    # cfps_yoy 每股经营活动产生的现金流量净额同比增长率(%)
    dff = dff[dff['bps'] > 0]  # 每股净资产
    dff = dff[dff['roe'] > 0]
    dff = dff[dff['roa'] > 0]
    dff = dff[dff['dt_eps'] > 0]  # 稀释每股收益
    dff = dff[dff['dt_eps_yoy'] > 0]  # 稀释每股收益同比增长率(%)
    #dff = dff[dff['cfps']>0] #每股现金流量净额
    #dff = dff[dff['ocfps']>0]  #每股经营活动产生的现金流量净额
    #dff = dff[dff['ocf_to_or'] > 0.5]  # 净现比
    #dff = dff[dff['salescash_to_or'] > 0.8]  # 收现比

    dff = dff[dff['op_yoy'] > 0]  # 营业利润同比增长率(%)
    dff = dff[dff['ebt_yoy'] > 0]  # 利润总额同比增长率(%)
    dff = dff[dff['or_yoy'] > 0]  # 营业收入同比增长率(%)
    dff = dff[dff['tr_yoy'] > 0]  # 营业总收入同比增长率(%)
    dff = dff[dff['netprofit_yoy'] > 0]  # 归属母公司股东的净利润同比增长率(%)

    dff=dff[dff['q_netprofit_yoy']>0] #归属母公司股东的净利润同比增长率(%)(单季度)
    #dff=dff[dff['q_netprofit_qoq']>0] #归属母公司股东的净利润环比增长率(%)(单季度)
    dff=dff[dff['q_gr_yoy']>0]  #营业总收入同比增长率(%)(单季度)
    #dff=dff[dff['q_gr_qoq']>0]  #营业总收入环比增长率(%)(单季度)
    dff=dff[dff['q_profit_yoy']>0]  #净利润同比增长率(%)(单季度)
    #dff=dff[dff['q_profit_qoq']>0]  #净利润环比增长率(%)(单季度)
    dff=dff[dff['q_sales_yoy']>0]  # 营业收入同比增长率(%)(单季度)
    #dff=dff[dff['q_sales_qoq']>0]  # 营业收入环比增长率(%)(单季度)

    dff = dff[dff['current_ratio'] > 1]  # 流动比率正常值2:1以上，不低于1不超过5
    dff = dff[dff['current_ratio'] < 5]
    # dff=dff[dff['current_ratio']<=M_current_ratio]
    dff = dff[dff['quick_ratio'] > 0.3]  # 速动比率正常值1，不低于0.5不超过1
    dff = dff[dff['quick_ratio'] < 1.5]
    # dff=dff[dff['quick_ratio']<=M_quick_ratio]
    # dff=dff[dff['cash_to_liqdebt']>0.20]    #cash_to_liqdebt现金比率=现金类资产/流动负债  20%以上，但不宜过高
    # debt_to_assets资产负债率 负债净值比率=总负债/资产净值=总负债/（总资产-总负债)=total_liab（负债合计）/total_liab_hldr_eqy（股东权益合计(含少数股东权益)）
    dff=dff[dff['debt_to_assets']<=60]
    # eqt_to_debt归属于母公司的股东权益/负债合计  越小越好

    # df04=pd.merge(dff,dfm, on=['ts_code'],how='left')
    df05 = pd.merge(dfb, dff, on=['ts_code'])
    df06 = pd.concat([df05, df66], ignore_index=True)
    # print(df06)
    stock_pool = df06.ts_code.values.tolist()
    #stock_pool = list_ths
    ##############################################
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1200)
    start_dt1 = time_temp.strftime('%Y%m%d')
    n = len(stock_pool)
    #print(stock_pool)
    #del_sql('stock_data_brandes')
    print('选出数量：',n)
    result= pd.DataFrame()
    #print(result)
    for code in stock_pool:
        n = n - 1
        dfd = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt1,end_date=dt_now)
        #adx = ta.ADX(dfd['high'].values, dfd['low'].values,dfd['close'].values, timeperiod=14)
        dfvr = dfd.head(26)
        dfvrs = dfvr[dfvr['change'] > 0]
        dfvrj = dfvr[dfvr['change'] <= 0]
        vr = dfvrs['vol'].sum() / dfvrj['vol'].sum() * 100
        # time.sleep(1)
        dfy = ts.pro_bar(ts_code=code, freq='M', adj='qfq', start_date=start_dt1, end_date=dt_now)
        dfd = dfd.sort_values(by=['trade_date'], ascending=1)
        dfy = dfy.sort_values(by=['trade_date'], ascending=1)
        dfDMI = DMI(dfd, N=14, MM=6)

        # 调用talib计算MACD指标  hist红柱绿柱，signal 红线 慢线，macd 黑线 快线
        dfd['MACD'], dfd['MACDsignal'], dfd['MACDhist'] = ta.MACD(dfd['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
        # dfy['MACD'],dfy['MACDsignal'],dfy['MACDhist'] = ta.MACD(dfy['close'].values,fastperiod=13, slowperiod=21, signalperiod=8)
        dfd['dMA55'] = ta.MA(dfd['close'].values, timeperiod=55)
        dfy['yMA34'] = ta.MA(dfy['close'].values, timeperiod=34)
        dfd['RSI6'] = ta.RSI(dfd['close'].values, timeperiod=6)  # RSI的天数一般是6、12、24
        dfd['RSI12'] = ta.RSI(dfd['close'].values, timeperiod=12)
        dfd['RSI24'] = ta.RSI(dfd['close'].values, timeperiod=24)
        dfd['WILLR'] = ta.WILLR(dfd['high'].values, dfd['low'].values, dfd['close'].values, timeperiod=10)
        dfd['upper'], dfd['middle'], dfd['lower'] = ta.BBANDS(dfd['close'].values,timeperiod=20,nbdevup=2,nbdevdn=2,matype=0)
        # dfd['MA90'] = ta.MA(dfd['close'].values, timeperiod=90)
        # dfd['MA360'] = ta.MA(dfd['close'].values, timeperiod=360)
        dfd = dfd.tail(2)
        dfy = dfy.tail(1)
        dfd['WILLRabs'] = dfd['WILLR'].abs()
        dfd = dfd.sort_values(by=['trade_date'], ascending=0)
        dfy = dfy.sort_values(by=['trade_date'], ascending=0)
        dfd['dvs'] = dfd['close'] / dfd['dMA55']
        dfy['yvs'] = dfy['close'] / dfy['yMA34']
        dfd = dfd[['ts_code','trade_date', 'dMA55', 'dvs', 'MACD', 'MACDsignal', 'MACDhist', 'RSI6', 'RSI12', 'RSI24', 'WILLRabs','upper', 'middle','lower']]#, 'PDI','MDI','ADX','ADXR','S1','S2','S3','S4','S5','S6','S7','S8','S9' ]]
        dfy = dfy[['ts_code','yMA34', 'yvs']]
        #print(dfd)
        #print(dfy)
        df = pd.merge(dfd, dfy, on=['ts_code'])
        df['vr'] = vr

        # ===找出买入信号
        s11 = (df['MACD'] < 0)  # macd 黑线 快线
        s12 = (df['MACD'] > 0)
        s21 = (df['MACDsignal'] < 0)  # signal 红线 慢线
        s22 = (df['MACDsignal'] > 0)
        s31 = (df['MACDhist'] > 0)  # hist红柱
        s32 = (df['MACDhist'] < 0)  # hist绿柱
        s41 = (df['MACDhist'] > df['MACDhist'].shift(-1))
        s42 = (df['MACDhist'] < df['MACDhist'].shift(-1))
        df.loc[s11 & s21 & s31 & s41, 'signal'] = '0轴以下启动'
        df.loc[s11 & s21 & s31 & s42, 'signal'] = '0轴以下诱多'
        df.loc[s11 & s21 & s32 & s41, 'signal'] = '0轴以下抄底'
        df.loc[s11 & s21 & s32 & s42, 'signal'] = '0轴以下下探'
        df.loc[s12 & s22 & s31 & s41, 'signal'] = '0轴以上追涨'
        df.loc[s12 & s22 & s31 & s42, 'signal'] = '0轴以上逃顶'
        df.loc[s12 & s22 & s32 & s41, 'signal'] = '0轴以上反弹'
        df.loc[s12 & s22 & s32 & s42, 'signal'] = '0轴以上死叉'
        df.loc[s11 & s22, 'signal'] = '下跌观望'
        df.loc[s12 & s21, 'signal'] = '上升追涨'

        condition1 = (df['MACDhist'] > 0)
        # 当天的短期均线大于等于长期均线
        condition2 = (df['MACD'] >= df['MACDsignal'])
        # 上个交易日的短期均线小于长期均线
        # print(dfd['MACD'])
        # print(dfd['MACDsignal'])
        # print(dfd['MACD'].shift(-1))
        # print(dfd['MACDsignal'].shift(-1))
        condition3 = (df['MACD'].shift(-1) < df['MACDsignal'].shift(-1))
        # 将买入信号当天的signal设置为1
        df.loc[condition1 & condition2 & condition3, 'signal'] = '金叉'

        # ===找出卖出信号
        # 当天的短期均线小于等于长期均线
        condition1 = (df['MACD'] <= df['MACDsignal'])
        # 上个交易日的短期均线大于长期均线
        condition2 = (df['MACD'].shift(-1) > df['MACDsignal'].shift(-1))
        # 将买入信号当天的signal设置为-1
        df.loc[condition1 & condition2, 'signal'] = '死叉'
        df=df.head(1)
        #print(dfDMI)

        df = pd.merge(df, dfDMI, on=['trade_date'])
        temdf = pd.concat([result, df], ignore_index=True)
        result=temdf
        print(n)
        print(df)
    #########################################################
    df = pd.merge(df06, result, on=['ts_code'], how='right')
    df.loc[df['debt_to_assets'] < 45, 'Qfzjz'] = 1  # 资产负债率
    df.loc[df['debt_to_assets'] >= 45, 'Qfzjz'] = -1
    # 归属于母公司的股东权益/负债合计
    df.loc[df['eqt_to_debt'] < M_eqt_to_debt, 'Qeqt_to_debt'] = 1
    df.loc[df['eqt_to_debt'] > M_eqt_to_debt, 'Qeqt_to_debt'] = -1
    # cash_to_liqdebt现金比率=现金类资产/流动负债  20%以上，但不宜过高
    condition001 = df['cash_to_liqdebt'] > 0.20
    condition002 = df['cash_to_liqdebt'] < M_cash_to_liqdebt
    df.loc[condition001 & condition002, 'Qcash_to_liqdebt'] = 1
    condition001 = df['cash_to_liqdebt'] <= 0.20
    condition002 = df['cash_to_liqdebt'] > M_cash_to_liqdebt
    df.loc[condition001 & condition002, 'Qcash_to_liqdebt'] = -1
    # 流动比率正常值2:1，不低于1不超过5
    condition001 = df['current_ratio'] >= 1
    condition002 = df['current_ratio'] < 5
    df.loc[condition001 & condition002, 'Qcurrent_ratio'] = 1
    condition001 = df['current_ratio'] >= 5
    condition002 = df['current_ratio'] < 1
    df.loc[condition001 & condition002, 'Qcurrent_ratio'] = -1
    # 速动比率正常值1，不低于0.5不超过1
    condition001 = df['quick_ratio'] > 0.5
    condition002 = df['quick_ratio'] < 1.5
    df.loc[condition001 & condition002, 'Qquick_ratio'] = 1
    condition001 = df['quick_ratio'] > 1
    condition002 = df['quick_ratio'] < 1.5
    df.loc[condition001 & condition002, 'Qquick_ratio'] = -1
    # dfsgv=df[df['sgv'] >0]
    # df.loc[df['sgv']>dfsgv['sgv'].median(), 'Qsgv'] = 1
    # df.loc[df['sgv']<=0, 'Qsgv'] = -1
    # df.loc[df['sgv']>df['elgv'].median(), 'Qelgv'] = 1
    # df.loc[df['elgv']<=0, 'Qelgv'] = -1
    # df.loc[df['lgv']>df['lgv'].median(), 'Qlgv'] = 1
    # df.loc[df['lgv']<=0, 'Qlgv'] = -1
    # df.loc[df['dvs']>1, 'Qdvs'] = 1
    # dfyvs=df[df['yvs'] >0]
    # condition001 = df['yvs']<1
    # condition002 = df['yvs']>0
    # df.loc[ condition001 & condition002 , 'Qyvs'] = 1
    # df.loc[df['yvs']>=1 , 'Qyvs'] = -1
    df.loc[df['dt_eps'] > 0, 'Qdt_eps'] = 1
    df.loc[df['dt_eps'] <= 0, 'Qdt_eps'] = -1
    df.loc[df['dt_eps_yoy'] > 0, 'Qdt_eps_yoy'] = 1
    df.loc[df['dt_eps_yoy'] <= 0, 'Qdt_eps_yoy'] = -1
    df.loc[df['bps'] > 0, 'Qbps'] = 1
    df.loc[df['bps'] <= 0, 'Qbps'] = -1
    df.loc[df['cfps'] > 0, 'Qcfps'] = 1
    df.loc[df['cfps'] <= 0, 'Qcfps'] = -1
    df.loc[df['ocfps'] > 0, 'Qocfps'] = 1
    df.loc[df['ocfps'] <= 0, 'Qocfps'] = -1
    df.loc[df['roe'] > 0, 'Qroe'] = 1
    df.loc[df['roe'] <= 0, 'Qroe'] = -1
    df.loc[df['roa'] > 0, 'Qroe'] = 1
    df.loc[df['roa'] <= 0, 'Qroe'] = -1
    df.loc[df['bps_yoy'] > 0, 'Qbps_yoy'] = 1
    df.loc[df['bps_yoy'] <= 0, 'Qbps_yoy'] = -1
    df.loc[df['cfps_yoy'] > 0, 'Qcfps_yoy'] = 1
    df.loc[df['cfps_yoy'] <= 0, 'Qcfps_yoy'] = -1
    df.loc[df['op_yoy'] > 0, 'Qop_yoy'] = 1
    df.loc[df['op_yoy'] <= 0, 'Qop_yoy'] = -1
    df.loc[df['ebt_yoy'] > 0, 'Qebt_yoy'] = 1
    df.loc[df['ebt_yoy'] <= 0, 'Qebt_yoy'] = -1
    df.loc[df['or_yoy'] > 0, 'Qor_yoy'] = 1
    df.loc[df['or_yoy'] <= 0, 'Qor_yoy'] = -1
    df.loc[df['tr_yoy'] > 0, 'Qtr_yoy'] = 1
    df.loc[df['tr_yoy'] <= 0, 'Qtr_yoy'] = -1
    df.loc[df['netprofit_yoy'] > 0, 'Qnetprofit_yoy'] = 1
    df.loc[df['netprofit_yoy'] <= 0, 'Qnetprofit_yoy'] = -1

    #df.loc[df['q_sales_qoq']>df['q_sales_qoq'].median(), 'Qq_sales_qoq'] = 1
    #df.loc[df['q_sales_yoy']>df['q_sales_yoy'].median(), 'Qq_sales_yoy'] = 1
    #df.loc[df['q_profit_qoq']>df['q_profit_qoq'].median(), 'Qq_profit_qoq'] = 1
    #df.loc[df['q_profit_yoy']>df['q_profit_yoy'].median(), 'Qq_profit_yoy'] = 1
    #df.loc[df['q_netprofit_qoq']>df['q_netprofit_qoq'].median(), 'Qq_netprofit_qoq'] = 1
    #df.loc[df['q_netprofit_yoy']>df['q_netprofit_yoy'].median(), 'Qq_netprofit_yoy'] = 1
    #df.loc[df['q_gr_yoy']>df['q_gr_yoy'].median(), 'Qq_gr_yoy'] = 1
    #df.loc[df['q_gr_qoq']>df['q_gr_qoq'].median(), 'Qq_gr_qoq'] = 1

    ###########################################################

    df.loc[df['q_sales_qoq'] > 0, 'Qq_sales_qoq'] = 1
    df.loc[df['q_sales_qoq'] <= 0, 'Qq_sales_qoq'] = -1
    df.loc[df['q_sales_yoy'] > 0, 'Qq_sales_yoy'] = 1
    df.loc[df['q_sales_yoy'] <= 0, 'Qq_sales_yoy'] = -1
    df.loc[df['q_profit_qoq'] > 0, 'Qq_profit_qoq'] = 1
    df.loc[df['q_profit_qoq'] <= 0, 'Qq_profit_qoq'] = -1
    df.loc[df['q_profit_yoy'] > 0, 'Qq_profit_yoy'] = 1
    df.loc[df['q_profit_yoy'] <= 0, 'Qq_profit_yoy'] = -1
    df.loc[df['q_netprofit_qoq'] > 0, 'Qq_netprofit_qoq'] = 1
    df.loc[df['q_netprofit_qoq'] <= 0, 'Qq_netprofit_qoq'] = -1
    df.loc[df['q_netprofit_yoy'] > 0, 'Qq_netprofit_yoy'] = 1
    df.loc[df['q_netprofit_yoy'] <= 0, 'Qq_netprofit_yoy'] = -1
    df.loc[df['q_gr_yoy'] > 0, 'Qq_gr_yoy'] = 1
    df.loc[df['q_gr_yoy'] <= 0, 'Qq_gr_yoy'] = -1
    df.loc[df['q_gr_qoq'] > 0, 'Qq_gr_qoq'] = 1
    df.loc[df['q_gr_qoq'] <= 0, 'Qq_gr_qoq'] = -1

    ##############################################################
    df['Qf'] = df['Qq_sales_qoq'] + df['Qq_sales_yoy'] + df['Qq_profit_qoq'] + df['Qq_profit_yoy'] + df[
        'Qq_netprofit_qoq'] + df['Qq_netprofit_yoy'] + df['Qq_gr_yoy'] + df['Qq_gr_qoq']
    dfc2 = df[df.ts_code.isin(list_cc)]
    #df = df[df['Qf'] > 7]
    df = pd.concat([df, dfc2], ignore_index=True)
    df = df.fillna(0)
    df['qz'] = df['Qfzjz'] + df['Qeqt_to_debt'] + df['Qdt_eps'] + df['Qdt_eps_yoy'] + df['Qbps'] + df['Qcfps'] + df[
        'Qocfps'] + df['Qroe'] + df['Qbps_yoy'] + df['Qcfps_yoy'] + df['Qop_yoy'] + df['Qebt_yoy'] + df['Qor_yoy'] + df[
                   'Qtr_yoy'] + df['Qnetprofit_yoy'] + df['Qcurrent_ratio'] + df['Qquick_ratio'] + df[
                   'Qcash_to_liqdebt'] + df['Qq_sales_qoq'] + df['Qq_sales_yoy'] + df['Qq_profit_qoq'] + df[
                   'Qq_profit_yoy'] + df['Qq_netprofit_qoq'] + df['Qq_netprofit_yoy'] + df['Qq_gr_qoq'] + df[
                   'Qq_gr_yoy']  # +df['Qdvs']+df['Qyvs']
    df = df.drop_duplicates(subset='ts_code', keep='first', inplace=False)
    df = df.sort_values(by=['qz'], ascending=0)
    df = df.reset_index(drop=True)
    # df=df[df['qz'] >0]
    # df=df.head(10)
    # df=df[df['qz'] >df['qz'].median()]
    df.loc[df['RSI6'] >75, 'QRSI'] = -1
    df.loc[df['RSI6'] <37, 'QRSI'] = 1
    df.loc[df['WILLRabs'] < 37, 'Qwillr'] = -1
    df.loc[df['WILLRabs'] > 75, 'Qwillr'] = 1
    df.loc[df['vr'] <50, 'Qvr'] = 1
    df.loc[df['PDI'] < 10, 'QPDI'] = 1
    #########################################################
    dt_h = str(time.strftime("%H", time.localtime()))
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    if dt_now == get_trade_cal:
        if dt_h < '17':
            dt03 = get_last_trade_cal
            dt04 = get_last_trade_cal1
        else:
            dt03 = dt_now
            dt04 = get_last_trade_cal
    else:
        dt03 = get_trade_cal
        dt04 = get_last_trade_cal
    tbname1 = 'stock_data_brandes' + dt03
    #insert_sql(df, tbname1, 'replace')
    #df.to_sql(tbname1, engine, index=False, if_exists='replace')
    tbname2 = 'stock_data_brandes' + dt04
    #dfy = check_sql(tbname2)
    '''
    if type(dfy)!= int :
        dfy = dfy[['ts_code', 'name', 'signal']]
        # print(dt_now,get_trade_cal,get_last_trade_cal,get_last_trade_cal1,dt03,dt04,ssql)
        list_tody = df.ts_code.values.tolist()
        dfy2 = dfy[~dfy.ts_code.isin(list_tody)]
        list_yesterday = dfy.ts_code.values.tolist()
        df2 = df[~df.ts_code.isin(list_yesterday)]
        list_new = df2.ts_code.values.tolist()
        # dff=pd.concat([dff1, dff2], ignore_index=True)
        print('今天新增数据:', len(list_new))
        print(list_new)
        # print(df2)
        #print('昨天数据:')
        #print(dfy)
        print('淘汰数据:')
        print(dfy2)
        dft = pd.merge(df, dfy, on=['ts_code'], how='outer')
    else:
    '''
    dft = df
    ########################################################################
    dft1 = dft
    dft1 = dft1[dft1['dvs'] < 1]
    dft1 = dft1[dft1['dvs'] > 0]
    dft1 = dft1[dft1['yvs'] < 1]
    dft1 = dft1[dft1['yvs'] > 0]
    '''
    if type(dfy) != int:
        dft1 = dft1[
        ['ts_code','trade_date_x','name_x','QRSI','Qwillr','Qvr','QPDI', 'signal_y', 'signal_x','S1','S2','S3','S4','S5','S6','S7','S8','S9', 'yvs', 'dvs', 'close', 'area', 'industry', 'market', 'pe_ttm', 'pb', 'turnover_rate', 'qz', 'ocf_to_or','salescash_to_or',  'vr', 'RSI6','RSI12','RSI24', 'WILLRabs','PDI','MDI','ADX','ADXR','upper', 'middle','lower']]
    else:
    '''
    dft1 = dft1[['ts_code','trade_date_x','name','QRSI','Qwillr','Qvr','QPDI', 'signal','S1','S2','S3','S4','S5','S6','S7','S8','S9', 'yvs', 'dvs', 'close', 'area', 'industry', 'market', 'pe_ttm', 'pb', 'turnover_rate','qz', 'ocf_to_or','salescash_to_or', 'vr','RSI6','RSI12','RSI24', 'WILLRabs', 'PDI','MDI','ADX','ADXR','upper', 'middle','lower']]
    dft1 = dft1.sort_values(by=['vr'], ascending=0)
    dft1 = dft1.reset_index(drop=True)
    dft1.to_csv(dt03 + '月线34日线55以下.csv', encoding='utf_8_sig')
    '''
    if type(dfy) != int:
        dft2 = dft[
            ['ts_code', 'trade_date_x', 'name_x','QRSI','Qwillr','Qvr', 'QPDI','signal_y', 'signal_x','S1','S2','S3','S4','S5','S6','S7','S8','S9', 'yvs', 'dvs', 'close', 'area', 'industry', 'market', 'pe_ttm', 'pb', 'turnover_rate', 'ocf_to_or','salescash_to_or', 'qz', 'vr', 'RSI6','RSI12','RSI24', 'WILLRabs','PDI','MDI','ADX','ADXR','upper', 'middle','lower']]
    else:
    '''
    dft2 = dft[['ts_code','trade_date_x', 'name','QRSI','Qwillr','Qvr','QPDI', 'signal','S1','S2','S3','S4','S5','S6','S7','S8','S9',  'yvs', 'dvs', 'close', 'area', 'industry', 'market', 'pe_ttm', 'pb', 'turnover_rate', 'ocf_to_or','salescash_to_or', 'qz', 'vr','RSI6','RSI12','RSI24', 'WILLRabs','PDI','MDI','ADX','ADXR','upper', 'middle','lower']]
    dft2 = dft2.sort_values(by=['qz'], ascending=0)
    dft2 = dft2.reset_index(drop=True)
    dft2.to_csv(dt03 + 'all.csv', encoding='utf_8_sig')
    #dfty = dft.sort_values(by=['pb'], ascending=1)
    #dfty = dfty.reset_index(drop=True)
    #print(dfty)
    #dfty.to_excel(dt03 + '.xls')
    df300 = dft.sort_values(by=['pb'], ascending=1)
    df300 = df300[df300.ts_code.isin(get_code_list_300())]
    df300.to_csv(dt03 + 'hs300.csv')
    '''
    dftv=dft[dft['volume_ratio'] >0.7]
    dftv=dftv[dftv['circ_mv'] <1500000]
    dftv = dftv[dftv['turnover_rate'] > 5.9]
    dftv = dftv[dftv['turnover_rate'] < 9]
    dftv = dftv[dftv['RSI'] >33]
    dftv = dftv[dftv['WILLR'] > 21]
    print(dftv)
    dftv.to_excel('turnover.xls')
    '''
    ##################################
    # from hc.smtp import send_mail
    # send_mail(path,dfty)
