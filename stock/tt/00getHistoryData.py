#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

import time
import datetime
import pymysql
import threading
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine


def get_code():    
    codes = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,market,industry,list_date')
    codes.to_sql('stock_basic',engine,index=False,if_exists='replace')
    codes = codes.ts_code.values   
    #print(codes,len(codes))
    return codes

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


def del_sql(tb_name):
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # SQL 更新语句
    #cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
    sql = "drop table IF EXISTS " +tb_name    
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        # db.rollback()
        pass   
    # 关闭数据库连接
    # db.close()
    
    
def update_sql(tb_name):
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # SQL 更新语句   
    #sql = "drop table IF EXISTS " +tb_name    
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()        
    # 关闭数据库连接
    db.close()


def get_data(threadID,tb_name,stock_pool,start_dt,end_dt):
    n=len(stock_pool)
    for code in  stock_pool:
        n=n-1
        if threadID ==1001:
            df= pro.daily_basic(ts_code=code, start_date=start_dt,  fields='ts_code,trade_date,pe,pe_ttm') #每日指标     
        elif threadID ==1002:
            df= pro.query('fina_indicator', ts_code=code, start_date=start_dt, end_date=end_dt,fields='ts_code,end_date,q_netprofit_yoy,q_netprofit_qoq') # 财务指标数据           
        elif threadID ==1003:
            df = pro.income(ts_code=code, start_date=start_dt, end_date=end_dt)  #利润表
        elif threadID ==1004:
            df= pro.balancesheet(ts_code=code, start_date=start_dt, end_date=end_dt) #资产负债表
        elif threadID ==1005:
            df = pro.cashflow(ts_code=code, start_date=start_dt, end_date=end_dt) # 现金流量表
        elif threadID ==1006:
            df= pro.express(ts_code=code, start_date=start_dt, end_date=end_dt) #业绩快报
        elif threadID ==1007:
            df = pro.forecast(ts_code=code, start_date=start_dt, end_date=end_dt) # 业绩预告  
        elif threadID ==1008:
            df = ts.pro_bar(ts_code=code, adj='qfq',start_date=start_dt, end_date=end_dt, ma=[55])
            df['closevsma55']=df['close']/df['ma55']
        else:
            pass

        insert_sql(df,tb_name,'append')
        #time.sleep(1)  # 数据接口限制,每分钟最多访问该接口80次
        print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+'  完成:',start_dt,end_dt,code,threadID,tb_name,'还剩下：'+str(n))

# 创建进程类
class myThread(threading.Thread):
    # 构造函数
    def __init__(self, threadID, tb_name,stock_pool,start_dt,end_dt):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.tb_name = tb_name
        self.stock_pool = stock_pool
        self.start_dt = start_dt
        self.end_dt = end_dt

    # 重写run()
    def run(self):
        print("Thread Strat：" + str(self.threadID) +"  "+ self.tb_name)
        get_data(self.threadID,self.tb_name,self.stock_pool,self.start_dt,self.end_dt)
        print("Thread Exit：" + str(self.threadID) +"  "+ self.tb_name)


if __name__== '__main__':
    historydate=5  #下载往前多少天的历史数据
    # 设置token及数据库连接
    ts.set_token('809eb4ebed2618990f0da9579f701706196b84216a9d7302fa08ac26')    
    pro=ts.pro_api()
    db = pymysql.connect("127.0.0.1","py","123456","stock" )
    engine = create_engine('mysql+pymysql://py:123456@127.0.0.1:3306/stock?charset=utf8mb4')
    #engine = create_engine('mysql+pymysql://py:123456@111.231.120.149:3306/stock?charset=utf8')
    #engine = create_engine('mysql+pymysql://py:123456@123.207.253.118:3306/stock?charset=utf8')
    
    
    #del_sql("stock_data_income")
    #del_sql("stock_data_balancesheet")
    #del_sql("stock_data_cashflow")
    #del_sql("stock_data_fina_indicator")
    #del_sql("stock_data_forecast")
    #del_sql("stock_data_express")
    #del_sql("stock_data_daily_basic")  
    #del_sql("stock_data_adj_factor")  

    # 取值时间节点      
    end_dt = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp =datetime.datetime.now() - datetime.timedelta(days=historydate)
    start_dt = time_temp.strftime('%Y%m%d')
    
    stock_pool =get_code()    
    start_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    thread1 = myThread(1001, "stock_data_daily_basic", stock_pool,start_dt,end_dt) #每日指标
    thread2 = myThread(1002, "stock_data_fina_indicator", stock_pool,start_dt,end_dt) # 财务指标数据
    thread3 = myThread(1003, "stock_data_income", stock_pool,start_dt,end_dt)   #利润表
    thread4 = myThread(1004, "stock_data_balancesheet", stock_pool,start_dt,end_dt) #资产负债表
    thread5 = myThread(1005, "stock_data_cashflow", stock_pool,start_dt,end_dt) # 现金流量表
    thread6 = myThread(1006, "stock_data_express", stock_pool,start_dt,end_dt) #业绩快报
    thread7 = myThread(1007, "stock_data_forecast", stock_pool,start_dt,end_dt) # 业绩预告
    thread8 = myThread(1008, "stock_data_day_qfq", stock_pool,start_dt,end_dt) # 业绩预告
      
  
    
    # 开启线程    
    #thread1.start()
    #thread2.start()
    #thread3.start() 
    #thread4.start() 
    #thread5.start() 
    #thread6.start()  
    #thread7.start()  
    #thread8.start()     
    #thread1.join()        
    #thread2.join()
    #thread3.join()
    #thread4.join() 
    #thread5.join()
    #thread6.join()    
    #thread7.join() 
    #thread8.join() 
    
    end_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start Time:",start_time)
    print("All over Time:" ,end_time)






