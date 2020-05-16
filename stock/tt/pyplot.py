#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

#先引入后面可能用到的包（package）
import time
# import talib as ta
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from matplotlib import pyplot as plt
#import matplotlib.pyplot as plt

#正常显示画图时出现的中文
#from pylab import mpl
#这里使用微软雅黑字体
#mpl.rcParams['font.sans-serif']=['SimHei']



# 通用字体设置
from matplotlib import font_manager
myfont = font_manager.FontProperties(fname="/usr/share/fonts/chinese/msyhl.ttc")

#正常显示画图时出现的中文和负号
from pylab import mpl
mpl.rcParams['font.sans-serif']=['Microsoft YaHei'] # 用来正常显示中文标签
#mpl.rcParams['axes.unicode_minus']=False # 用来正常显示负号

#%matplotlib inline


pd.set_option('display.max_rows', 100)  # 显示的最大行数（避免只显示部分行数据）
pd.set_option('display.max_columns', 1000)  # 显示的最大列数（避免列显示不全）
pd.set_option("display.max_colwidth", 1000)  # 每一列最大的宽度（避免属性值或列名显示不全）
pd.set_option('display.width', 1000)  # 每一行的宽度（避免换行）

#画图时显示负号
#mpl.rcParams['axes.unicode_minus']=False
# import seaborn as sns  #画图用的
#Jupyter Notebook特有的magic命令
#直接在行内显示图形
# %matplotlib inline   


# 设置别人的token
ts.set_token('9ea47e33c602f7683e269d837afcf94b86b75aab939ab06bd7d99863')
pro=ts.pro_api()
engine = create_engine('mysql+pymysql://py:123456@127.0.0.1:3306/stock?charset=utf8')
# 取值时间节点
start_dt='20190101'
end_dt = str(time.strftime("%Y%m%d", time.localtime()))
#time_temp =datetime.datetime.now() - datetime.timedelta(days=1)
#end_dt = time_temp.strftime('%Y%m%d')
stock_pool ='600319.SH'
#df = ts.pro_bar(ts_code=stock_pool, adj='qfq', start_date=start_dt, end_date=end_dt)
sql="select * from stock_data_fina_indicator where ts_code='600319.SH' and end_date >20170101 "  
# and trade_date >20190101; "

# read_sql_query的两个参数: sql语句， 数据库连接
df = pd.read_sql_query(sql, engine)
print(df)


#将数据列表中的第3列'trade_date'设置为索引
df.index=pd.to_datetime(df.end_date) 
#画出收盘价的走势
df['eps'].plot(figsize=(12,6))
plt.title('zst')
plt.xlabel('date')
plt.show()
