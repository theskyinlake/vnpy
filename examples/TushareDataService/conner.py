#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import pymysql
import numpy as np
import pandas as pd
from mysql import connector
from sqlalchemy import create_engine

# 初始化数据库连接，使用pymysql模块
#db_myself = pymysql.connect("111.231.120.149", "py", "shmily789", "stock")
db_myself = pymysql.connect("127.0.0.1", "vnpy", "shmily789", "vnpy")
#engine = create_engine('mysql+pymysql://vnpy:shmily789@127.0.0.1:3306/vnpy?charset=utf8')
engine = create_engine('mysql+mysqlconnector://vnpy:shmily789@127.0.0.1:3306/vnpy?charset=utf8')
#engine = create_engine('mysql+pymysql://nvpy:shmily789@127.0.0.1:3306/stock?charset=utf8mb4')
#engine = create_engine('mysql+mysqlconnector://py:shmily789@111.231.120.149:3306/stock?charset=utf8')



def check_sql(tb_name,ssql):
    # 使用cursor()方法获取操作游标
    cursor = db_myself.cursor()
    #ssql = "SELECT DISTINCT * FROM " + tb_name + " order by ts_code;"
    # sql = "select" + tb_name + "from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'stock' and TABLE_NAME = " + tb_name
    try:
        # 执行SQL语句
        cursor.execute(ssql)
        # 获取查询结果
        # data_sql=cursor.fetchall()
        data_sql = pd.read_sql_query(ssql, engine)
    except:
        # 发生错误时回滚
        # db_myself.rollback()
        data_sql = 0
        print(tb_name + '不存在！')
        pass
    # 关闭游标
    cursor.close()
    # 关闭数据库连接
    db_myself.close()
    return  data_sql

#if_exists字段定义:
#append：追加
#replace：删除原表，建立新表再添加
#fail：什么都不干

#fail的意思如果表存在，啥也不做
#replace的意思，如果表存在，删了表，再建立一个新表，把数据插入
#append的意思，如果表存在，把数据插入，如果表不存在创建一个表！！
def insert_sql( code,data,tb_name,exists):
    #使用try...except..continue避免出现错误，运行崩溃
    try:
        data.to_sql(tb_name,engine,index=False,if_exists=exists)
        print( code+'写入数据库成功')
    except:
        pass

def del_sql(tb_name):
    # 使用cursor()方法获取操作游标
    cursor = db_myself.cursor()
    # SQL 更新语句
    # cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
    sql = "drop table IF EXISTS " + tb_name
    try:
        # 执行SQL语句
        cursor.execute(sql)
        print(tb_name + '删掉完成')
        # 提交到数据库执行
        db_myself.commit()
    except:
        # 发生错误时回滚
        # db_myself.rollback()
        print(tb_name + 'sql wrong')
        pass
        # 关闭数据库连接
    # db_myself.close()
