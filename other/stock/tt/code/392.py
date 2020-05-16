#! -*- coding: utf-8 -*-
# 导入函数库
#import time
#import datetime
#import pymysql
#import talib as ta
import numpy as np
import pandas as pd
#import tushare as ts
#from sqlalchemy import create_engine
from jqdata import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设定指数
    g.stockindex = '000300.XSHG'   
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 最大持仓数量
    g.stocknum = 2
    # 为股票设定滑点为百分比滑点
    set_slippage(PriceRelatedSlippage(0.00246),type='stock')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    #log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    #避免未来数据模式
    #set_option("avoid_future_data", True)
    ## 手动设定调仓月份（如需使用手动，注释掉上段）
    #g.Transfer_date = (4,7,10)
    # 调仓频率
    #g.period = 34
    # 计时器
    #g.days = 0 
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    #run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
      # 开盘时运行
    run_daily(market_open, time='14:30', reference_security='000300.XSHG')    
      # 收盘后运行
    #run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    # 按月运行    
    #run_monthly(trade,-1, time='10:30', reference_security='000300.XSHG')
   
def before_market_open(context):
    # 输出运行时间
    #log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))
    #date=context.current_dt.strftime("%Y-%m-%d")
    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    #send_message('美好的一天~')
    #list_stocks=check_stocks(context)
    #print(date,len(list_stocks))
    #print(list_stocks)
    # 要操作的股票：g.为全局变量
    #g.security = list_stocks    
            

## 开盘时运行函数
def market_open(context):
    #last_date =  context.previous_date.strftime("%Y-%m-%d")
    #log.info('函数运行时间(market_open):'+str(context.current_dt.time()))
    #stocks_list = g.security  
    ## 获得Buylist
    stock_list = check_stocks(context)    
    ## 卖出
    if len(context.portfolio.positions) > 0:
        for stock in list(context.portfolio.positions):
            if stock not in stock_list:
                order_target(stock, 0)                
                log.info('%s 个股不在买入列表：%s' %(stock))  
    # 止损：个股跌幅超过1%，卖出
    if len(context.portfolio.positions) > 0:        
        securities = list(context.portfolio.positions)
        for stock in securities:
            price = history(1,unit='1d',field='close', security_list=stock, df=False,skip_paused=True,fq='pre')
            # 获取subportfolios[0]中多头仓位的security的持仓成本
            #last_date =  context.previous_date.strftime("%Y-%m-%d")
            hold_cost = context.subportfolios[0].long_positions[stock].hold_cost
            if hold_cost/price[stock]-1 < -0.05:
                order_target(stock, 0)
                log.info('%s 个股止损：%s' %(stock))  
            #if price[stock]/hold_cost > 1.2:
            #    order_target(stock, 0)
            #    log.info('%s 个股获利了结：%s' %(last_date,stock)) 
    #######################################################################      
    ## 买入
    ## 根据局大盘止损，具体用法详见dp_stoploss函数说明  
    stoploss1 = dp_stoploss(kernel=1, n=60, zs=0.07)
    stoploss2 = dp_stoploss(kernel=2, n=3, zs=0.07)    
    if stoploss1 : 
        if len(context.portfolio.positions)>0:
            for stock in list(context.portfolio.positions.keys()):
                order_target(stock, 0)
                log.info('%s 大盘股止损：%s' %(stock)) 
    if (not stoploss1) : #or (not stoploss2):
        if len(stock_list) > 0:
            for stock in stock_list:
                if stock not in list(context.portfolio.positions):
                    if len(context.portfolio.positions) < g.stocknum : 
                        # 取得当前的现金
                        #cash = context.portfolio.cash
                        # 用所有 cash 买入股票 
                        #order_value(stock, cash)
                        number = g.stocknum  - len(context.portfolio.positions)
                        order_value(stock,context.portfolio.available_cash/number)                        
                        # 记录这次买入
                        log.info("Buying %s" % (stock))                                                                      
##############################################################################   
## 大盘止损函数
def dp_stoploss(kernel=2, n=10, zs=0.03):
    '''
    方法1：当大盘N日均线(默认60日)与昨日收盘价构成“死叉”，则发出True信号
    方法2：当大盘N日内跌幅超过zs，则发出True信号
    '''
    # 止损方法1：根据大盘指数N日均线进行止损
    if kernel == 1:
        t = n+2
        hist = get_bars('000300.XSHG', count=t, unit='1d', fields=['close'])
        temp1 = sum(hist['close'][1:-1])/float(n)
        temp2 = sum(hist['close'][0:-2])/float(n)
        close1 = hist['close'][-1]
        close2 = hist['close'][-2]
        if (close2 > temp2) and (close1 < temp1):
            return True
        else:
            return False
    # 止损方法2：根据大盘指数跌幅进行止损
    elif kernel == 2:
        hist1 = get_bars('000300.XSHG', count=n, unit='1d', fields=['close'])
        if ((1-float(hist1['close'][-1]/hist1['close'][0])) >= zs):
            return True
        else:
            return False


## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    log.info('一天结束')
    log.info('##############################################################')

################## 以下为功能函数, 在主要函数中调用 ##########################
# 1.筛选股票列表
def check_stocks(context):
    #last_date =  context.previous_date.strftime("%Y-%m-%d") 
    date=context.current_dt.strftime("%Y-%m-%d")   
    #print(get_all_securities(['stock']))
    # 获取沪深成分股
    list_stocks = get_index_stocks(g.stockindex)    
    df = get_fundamentals(query(
            valuation.code,
            valuation.pe_ratio, #市盈率(PE, TTM)
            valuation.pb_ratio, #市净率(PB)
            valuation.ps_ratio, #市销率(PS, TTM)
            valuation.pcf_ratio, #市现率(PCF, 现金净流量TTM)
            valuation.turnover_ratio, #换手率(%)
            valuation.market_cap,    #总市值
            indicator.roe,
            indicator.eps,
            indicator.operating_profit_to_profit,
            indicator.net_profit_margin,
            indicator.gross_profit_margin,
            indicator.expense_to_total_revenue,
            indicator.operation_profit_to_total_revenue,
            indicator.net_profit_to_total_revenue,
            indicator.adjusted_profit_to_profit,
            indicator.inc_total_revenue_year_on_year,
            indicator.inc_total_revenue_annual,
            indicator.inc_revenue_year_on_year,
            indicator.inc_revenue_annual,
            indicator.inc_operation_profit_year_on_year,
            indicator.inc_operation_profit_annual,
            indicator.inc_net_profit_year_on_year,
            indicator.inc_net_profit_annual,
            indicator.inc_net_profit_to_shareholders_year_on_year,
            indicator.inc_net_profit_to_shareholders_annual,
        ).filter(
            valuation.code.in_(list_stocks),       
            valuation.pe_ratio.between(0,20),
            valuation.pb_ratio.between(0,2),
            indicator.roe>0,
            indicator.eps>0,
            indicator.operating_profit_to_profit>0,
            indicator.net_profit_margin>0,
            indicator.gross_profit_margin>0,
            indicator.expense_to_total_revenue>0,
            indicator.operation_profit_to_total_revenue>0,
            indicator.net_profit_to_total_revenue>0,
            indicator.adjusted_profit_to_profit>0,
            indicator.inc_total_revenue_year_on_year>0,
            indicator.inc_total_revenue_annual>0,
            indicator.inc_revenue_year_on_year>0,
            indicator.inc_revenue_annual>0,
            indicator.inc_operation_profit_year_on_year>0,
            indicator.inc_operation_profit_annual>0,
            indicator.inc_net_profit_year_on_year>0,
            indicator.inc_net_profit_annual>0,
            indicator.inc_net_profit_to_shareholders_year_on_year>0,
            indicator.inc_net_profit_to_shareholders_annual>0,
        ).order_by(
            valuation.pb_ratio.asc()
        ))#,date = last_date)  
    if len(list(df['code']))>0:
        # 去除停牌
        buylist =unpaused(list(df['code']))   
        if len(buylist)>0:
            # 去除ST，*ST
            st=get_extras('is_st', buylist, start_date=date, end_date=date, df=True)
            st=st.loc[date]
            buylist=list(st[st==False].index)   
    elif len(list(df['code']))==0:
        buylist=[]        
    return buylist
###################################################
# 去除停牌
def unpaused(stockspool):
    current_data=get_current_data()
    return [s for s in stockspool if not current_data[s].paused]



