# encoding: UTF-8

"""
导入CSV历史数据到MongoDB中
"""

from time import time
from datetime import datetime
import csv

import pymongo

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import DAILY_DB_NAME


#----------------------------------------------------------------------
def loadCsv(fileName, dbName, symbol):
    """"""
    start = time()
    print(u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol))
    
    # 锁定集合，并创建索引
    client = pymongo.MongoClient() 
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # 读取数据和插入到数据库
    with open(fileName, 'r') as f:
        reader = csv.DictReader(f)
        for d in reader:
            bar = VtBarData()
            bar.vtSymbol = symbol
            bar.symbol = symbol
            bar.open = float(d['open'])
            bar.high = float(d['high'])
            bar.low = float(d['low'])
            bar.close = float(d['close'])
            bar.date = d['date']
            bar.time = d['time']
            bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
            bar.volume = d['volume']
    
            flt = {'datetime': bar.datetime}
            collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
    
    print(u'插入完毕，耗时：%s' % (time()-start))


if __name__ == '__main__':
    loadCsv('IF99.csv', DAILY_DB_NAME, 'IF99')
    loadCsv('RB99.csv', DAILY_DB_NAME, 'RB99')
    loadCsv('TA99.csv', DAILY_DB_NAME, 'TA99')
    loadCsv('I99.csv', DAILY_DB_NAME, 'I99')

