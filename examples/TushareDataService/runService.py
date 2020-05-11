# encoding: UTF-8

"""
定时服务，可无人值守运行，实现每日自动下载更新历史行情数据到数据库中。
"""

import time
import datetime

from examples.TushareDataService.dataService import downloadAllMinuteBar


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
    end_dt = str(time.strftime("%Y%m%d", time.localtime()))
    time_temp =datetime.datetime.now() - datetime.timedelta(days=7)
    start_dt = time_temp.strftime('%Y%m%d')
    downloadAllMinuteBar(start_dt, exists)