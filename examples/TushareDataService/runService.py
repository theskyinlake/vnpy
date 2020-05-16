# encoding: UTF-8

"""
定时服务，可无人值守运行，实现每日自动下载更新历史行情数据到数据库中。
"""

import time
import datetime
import numpy as np
import pandas as pd
import tushare as ts
from examples.TushareDataService.config import pro
from examples.TushareDataService.conner import engine,db,insert_sql, del_sql
from examples.TushareDataService.getdata import get_trade_date  #,get_fina_indicator, get_basic, get_code_list_300, get_code_list_all #, get_moneyflow
from examples.TushareDataService.dataService import downloadAllMinuteBar

def get_neardownloaddate_sql():
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    ssql = "SELECT  trade_date  FROM  stock_data_day order by trade_date limit 1;"
    # sql = "select" + tb_name + "from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'stock' and TABLE_NAME = " + tb_name
    try:
        # 执行SQL语句
        cursor.execute(ssql)
        # 获取查询结果
        data_sql=cursor.fetchall()
        for row in data_sql:
            fdate = row[0]
        #data_sql = cursor.fetchone()
        #data_sql = pd.read_sql_query(ssql, engine)
        print(fdate)
    except:
        # 发生错误时回滚
        # db.rollback()
        data_sql = 0
        print('不存在！')
        pass
    # 关闭游标
    cursor.close()
    # 关闭数据库连接
    db.close()
    return  fdate

if __name__ == '__main__':
    '''
    taskCompletedDate = None
    # 生成一个随机的任务下载时间，用于避免所有用户在同一时间访问数据服务器
    #taskTime = datetime.time(hour=17, minute=0)
    
    # 进入主循环
    while True:
        t = datetime.datetime.now()
        
        # 每天到达任务下载时间后，执行数据下载的操作
        if t.time() > taskTime and (taskCompletedDate is None or t.date() != taskCompletedDate):
            downloadAllMinuteBar(start_dt,exists)
            
            # 更新任务完成的日期
            taskCompletedDate = t.date()
        else:
            print('当前时间%s，任务定时%s' %(t, taskTime))
    
        time.sleep(60)
    '''
    exists = 'append'
    fdate=get_neardownloaddate_sql()
    ftrade_date = get_trade_date(fdate)
    if ftrade_date=='False':
        print('数据库记录无日期')
    elif ftrade_date == 'True':
        print('当天数据已经下载')
    elif len(ftrade_date)==8:
        start_dt=ftrade_date
    downloadAllMinuteBar(start_dt, exists)