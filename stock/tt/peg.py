#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

import time
import datetime
import pymysql
import threading
import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

# 设置token
ts.set_token('809eb4ebed2618990f0da9579f701706196b84216a9d7302fa08ac26')
pro=ts.pro_api()

# 初始化数据库连接，使用pymysql模块
engine = create_engine('mysql+pymysql://py:123456@127.0.0.1:3306/stock?charset=utf8mb4')

dt_now = str(time.strftime("%Y%m%d", time.localtime()))
time_temp =datetime.datetime.now() - datetime.timedelta(days=100)
start_dt = time_temp.strftime('%Y%m%d')  
time_temp_sql =datetime.datetime.now() - datetime.timedelta(days=1)
start_dt_sql = time_temp.strftime('%Y%m%d')  

#the_year = int(str(dt_now)[0:4])
#the_month = str(dt_now)[4:6]

# 获取最后一个交易日的日期
def get_trade_cal():
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp =datetime.datetime.now() - datetime.timedelta(days=10)
    start_dt = time_temp.strftime('%Y%m%d')  
    df_trade = pro.trade_cal(exchange='', start_date=start_dt, end_date=dt_now)       
    df_trade=df_trade.sort_values(by=['cal_date'], ascending=0)
    df_trade=df_trade[df_trade['is_open'] == 1]
    df_trade= df_trade.reset_index(drop=True)
    trade_date=df_trade.loc[0,'cal_date']
    return trade_date
    

def insert_sql(data,tb_name,exists):
    #使用try...except..continue避免出现错误，运行崩溃
    try:
        data.to_sql(tb_name,engine,index=False,if_exists=exists)
        #print( code +'写入数据库成功')
    except:
        pass


def get_code_list():    
    #获取上市超过100天的股票
    codes_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,market,industry,list_date')
    codes_basic.to_sql('stock_basic',engine,index=False,if_exists='replace')
    time_temp =datetime.datetime.now() - datetime.timedelta(days=100)
    dt_100 = time_temp.strftime('%Y%m%d')    
    codes_basic_100 = codes_basic.loc[codes_basic['list_date']<dt_100].ts_code.values.tolist()
    
    #获取ST股票
    st=codes_basic[codes_basic['name'].str.contains('ST')].ts_code.values.tolist()  
    n=len(st)
    for code in st:        
        if codes_basic_100.count(code)>0:
            codes_basic_100.remove(code)
            print('st:',n,len(codes_basic_100),code)
            n=n-1
        else:
            pass
        
    
    #获取停牌的股票
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    codes_suspend = pro.suspend(ts_code='', suspend_date=dt_now, resume_date='', fields='').ts_code.values.tolist()    
    n=len(codes_suspend)
    for code in codes_suspend:
        if codes_basic_100.count(code)>0:
            codes_basic_100.remove(code)
            print('tp:',n,len(codes_basic_100),code)
            n=n-1
        else:
            pass
    
    #pe大于0
    #dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    #df = pro.daily_basic(ts_code='', trade_date=dt_now, fields='ts_code,trade_date,pe_ttm')
    #df=df[df.ts_code.isin(codes_basic_100)]     #筛选出ts_code列值在codes_basic_100列表的数据条
    #stock_list=df[df.pe_ttm>0].ts_code.values.tolist()
            
    return codes_basic_100


if __name__== '__main__':    
    
    stock_pool =get_code_list()   
    dt_h = str(time.strftime("%H", time.localtime()))  
    if dt_h<'17':
        time_temp =datetime.datetime.now() - datetime.timedelta(days=1)
        dt = time_temp.strftime('%Y%m%d') 
    else:
        dt=str(time.strftime("%Y%m%d", time.localtime()))
    pe_df = pro.daily_basic(ts_code='', trade_date=dt, fields='ts_code,trade_date,pe_ttm,pb,total_mv')
    print('pe_df:',pe_df)
    pe_df=pe_df[pe_df.ts_code.isin(stock_pool)]     #筛选出ts_code列值在stock_pool列表的数据条    
    #pe_df=pe_df[pe_df['pe_ttm']<50]
    pe_df=pe_df[pe_df['pe_ttm']>0]  
    #pe_df=pe_df[pe_df['pb']<pe_df['total_mv'].median()]  
    #pe_df=pe_df[pe_df['pb']>0]  
    #pe_df=pe_df[pe_df.total_mv<pe_df['total_mv'].median()]# 筛选市值小于中位数    
    stock_list=pe_df.ts_code.values.tolist()    
    df1 = pro.fina_indicator_vip(period='20190930',fields='ts_code,end_date,ocfps,q_netprofit_yoy,q_netprofit_qoq') 
    df2 = pro.fina_indicator_vip(period='20190630',fields='ts_code,end_date,ocfps,q_netprofit_yoy,q_netprofit_qoq')
    list_g=df1.ts_code.values.tolist()           
    df2=df2[~df2.ts_code.isin(list_g)]
    g_df=pd.concat([df1, df2], ignore_index=True)
    g_df=g_df[g_df['q_netprofit_yoy']>0]  
    #g_df=g_df[g_df['q_netprofit_qoq']>0]  
    peg_df=g_df[g_df.ts_code.isin(stock_list)]
    df=pd.merge(peg_df,pe_df, on=['ts_code'])
    df['peg']=df['pe_ttm']/df['q_netprofit_yoy']
    df=df.loc[df['peg']<0.5]
    ############################################## 
    print(df)    
    insert_sql(df,'stock_data_peg'+dt_now,'replace')
    #df.to_csv('stock_peg_list.csv',encoding='utf_8_sig')
    stock_pool=df.ts_code.values.tolist()
    n=len(stock_pool)
    for code in  stock_pool:
        n=n-1
        #time.sleep(1)         
        # 查询语句，选出employee表中的所有数据
        #sql = ''' SELECT  * FROM stock_data_day  WHERE ts_code="''' +str(code)+'''"; '''
        # read_sql_query的两个参数: sql语句， 数据库连接
        #df = pd.read_sql_query(sql, engine)
        #df=df.sort_values(by=['trade_date'],ascending=True)   
        df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_dt, end_date=dt_now)
        #df = ts.pro_bar(ts_code=code, freq='M', adj='qfq', start_date=start_dt, end_date=dt_now)
        df=df.sort_values(by=['trade_date'], ascending=1)
        # 调用talib计算MACD指标  hist红柱绿柱，signal 红线 慢线，macd 黑线 快线
        df['MACD'],df['MACDsignal'],df['MACDhist'] = ta.MACD(df['close'].values,fastperiod=13, slowperiod=21, signalperiod=8)
        df['MA21'] = ta.MA(df['close'].values, timeperiod=21) 
        df['MA34'] = ta.MA(df['close'].values, timeperiod=34) 
        df['MA55'] = ta.MA(df['close'].values, timeperiod=55) 
        df=df.tail(3)
        #print(df)
        df['vs'] =df['close']/df['MA55']
        #df2=df[df['trade_date'] == end_dt]
        #print(df2)
        # ===找出买入信号
        condition01 = (df['MACD']<0)
        condition02 = (df['MACDsignal']<0)
        condition03 = (df['MACDhist']>0)
        #condition04 = (df['MA21']>df['close'])
        #condition05 = (df['MA34']>df['close'])
        condition06 = (df['close']<df['MA55'])
        # 当天的短期均线大于等于长期均线   
        condition1 = (df['MACD'] >= df['MACDsignal'])
        # 上个交易日的短期均线小于长期均线
        condition2 = (df['MACD'].shift(1) < df['MACDsignal'].shift(1))   
        # 将买入信号当天的signal设置为1   
        df.loc[condition03 & condition1 & condition2, 'signal'] = '买入'
       
        #===找出卖出信号    
        # 当天的短期均线小于等于长期均线
        condition1 = (df['MACD'] <= df['MACDsignal'])
        # 上个交易日的短期均线大于长期均线
        condition2 = (df['MACD'].shift(1) > df['MACDsignal'].shift(1))
        # 将买入信号当天的signal设置为0
        df.loc[condition1 & condition2, 'signal'] = '卖出'
        # 将无关的变量删除
        #df.drop(['ma_short', 'ma_long'], axis=1, inplace=True)
        df=df[['ts_code', 'trade_date',   'open' ,  'high'   , 'low' , 'close' ,'MA55','vs', 'MACD',  'MACDsignal' , 'MACDhist' ,'signal']]        
        df2=df[(df['signal'] == '买入') ]
        df3=df2[df2['trade_date'] == get_trade_cal()]
        dt_now = str(time.strftime("%Y%m%d", time.localtime()))
        df3['signal_date'] = dt_now
        print(df3)
        print(n)
        insert_sql(df3,'stock_data_signal','append')    
    ##################################    
    from smtplib import SMTP_SSL 
    from email.header import Header   
    from email.mime.text import MIMEText 
    
    sql='''SELECT DISTINCT a.ts_code,b.name,b.area,b.industry,b.market,a.close,a.trade_date,a.signal,a.signal_date
        FROM stock_data_signal AS a
        LEFT JOIN  stock_basic AS b on a.ts_code=b.ts_code
        ORDER BY a.signal,a.trade_date,industry,a.ts_code;'''

    df = pd.read_sql_query(sql, engine)   
    df= df.loc[df['trade_date']==get_trade_cal()]
    df= df.to_html(header = True,index = False)

    email_from = "2147607204@qq.com" #改为自己的发送邮箱 
    email_to = "400523@qq.com" #接收邮箱 
    hostname = "smtp.qq.com" #不变，QQ邮箱的smtp服务器地址 
    login = "2147607204@qq.com" #发送邮箱的用户名 
    password = "gnvgurnojddbdjde" #发送邮箱的密码，即开启smtp服务得到的授权码。注：不是QQ密码。 
    subject = dt_now+" peg推荐" #邮件主题 

    text = "<html><body>"+df+"</body></html>" #邮件正文内容 
  

    smtp = SMTP_SSL(hostname)#SMTP_SSL默认使用465端口 
    smtp.login(login, password)   

    msg = MIMEText(text, 'html', 'utf-8') #发送含HTML内容的邮件
    msg["Subject"] = Header(subject, "utf-8") 
    msg["from"] = email_from 
    msg["to"] = email_to   

    smtp.sendmail(email_from, email_to, msg.as_string()) 
    smtp.quit()

    
