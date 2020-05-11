# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from examples.TushareDataService.dataService  import *


if __name__ == '__main__':
    start_dt='20000001'
    exists = 'replace'
    downloadAllMinuteBar(start_dt,exists)
