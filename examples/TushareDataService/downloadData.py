# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
#if_exists字段定义:
#append：追加
#replace：删除原表，建立新表再添加
#fail：什么都不干
"""

from examples.TushareDataService.dataService  import *


if __name__ == '__main__':
    start_dt='20200101'
    end_dt='20200501'
    exists = 'append'
    downloadAllMinuteBar(start_dt,end_dt,exists)
