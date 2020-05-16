#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

import time
import datetime
import pymysql
import numpy as np
import pandas as pd





def remove_duplicates_sql(tb_name):
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # SQL 更新语句
    #cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
    sql1 = '''drop table if exists t1;'''
    sql2 ='''drop table if exists t2;'''
    sql3 ='''CREATE TABLE t1 AS (SELECT  ts_code,end_date FROM '''+tb_name+''' GROUP BY ts_code,end_date HAVING count(*) >1); '''
    sql4 ='''CREATE TABLE t2 AS (select min(id) as t2_id from '''+tb_name+''' group by ts_code,end_date having count(*)>1); '''
    sql5 ='''delete from '''+tb_name+''' where (ts_code,end_date) in (select ts_code,end_date from t1 ) AND id not in (select t2_id from t2);'''
   
    # 执行SQL语句
    cursor.execute(sql1)
    print(sql1)
    # 提交到数据库执行
    db.commit()
     # 执行SQL语句
    cursor.execute(sql2)
    print(sql2)
    # 提交到数据库执行
    db.commit()
     # 执行SQL语句
    cursor.execute(sql3)
    print(sql3)
    # 提交到数据库执行
    db.commit()
     # 执行SQL语句
    cursor.execute(sql4)
    print(sql4)
    # 提交到数据库执行
    db.commit()
     # 执行SQL语句
    cursor.execute(sql5)
    print(sql5)
    # 提交到数据库执行
    db.commit()
    '''
    try:
        # 执行SQL语句
        cursor.execute(sql)
        print(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        # db.rollback()
        pass   
    '''
    # 关闭数据库连接
    db.close()

if __name__== '__main__':
   
    db = pymysql.connect("127.0.0.1","py","123456","stock" )
    #engine = create_engine('mysql+pymysql://py:123456@127.0.0.1:3306/stock?charset=utf8mb4')
    remove_duplicates_sql('stock_data_fina_indicator')       # 财务指标数据 
    remove_duplicates_sql('stock_data_income')         # 利润表
    remove_duplicates_sql('stock_data_balancesheet')   # 资产负债表
    remove_duplicates_sql('stock_data_cashflow')       # 现金流量表
    remove_duplicates_sql('stock_data_express')        # 业绩快报
    remove_duplicates_sql('stock_data_forecast')       # 业绩预告

   
   
  

