#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import pymysql
import numpy as np
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# 初始化数据库连接，使用pymysql模块
#db = pymysql.connect("111.231.120.149", "py", "shmily789", "stock")
db = pymysql.connect("127.0.0.1", "root", "shmily789", "stock")
# engine = create_engine('mysql+pymysql://root:shmily789@127.0.0.1:3306/stock?charset=utf8')
engine = create_engine('mysql+mysqlconnector://root:shmily789@127.0.0.1:3306/stock?charset=utf8')
#engine = create_engine('mysql+mysqlconnector://py:shmily789@111.231.120.149:3306/stock?charset=utf8')

#append：追加
#replace：删除原表，建立新表再添加
#fail：什么都不干

def check_sql(tb_name):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    ssql = "SELECT DISTINCT * FROM " + tb_name + " order by ts_code;"
    # sql = "select" + tb_name + "from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'stock' and TABLE_NAME = " + tb_name
    try:
        # 执行SQL语句
        cursor.execute(ssql)
        # 获取查询结果
        # data_sql=cursor.fetchall()
        data_sql = pd.read_sql_query(ssql, engine)
    except:
        # 发生错误时回滚
        # db.rollback()
        data_sql = 0
        print(tb_name + '不存在！')
        pass
    # 关闭游标
    cursor.close()
    # 关闭数据库连接
    db.close()
    return  data_sql



'''
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
'''