# encoding: UTF-8
import numpy as np
import pandas as pd

from conner import engine,db_myself
from vnpy.trader.database.initialize import init_sql
from vnpy.trader.database.database import Driver
from vnpy.trader.constant import (Exchange, Interval)
from vnpy.trader.database import database_manager
from vnpy.trader.object import (BarData,TickData)

settings={
    "database": "vnpy",
    "host": "localhost",
    "port": 3306,
    "user": "vnpy",
    "password": "shmily789",
    "authentication_source": ""
}
mysql_manager = init_sql(driver=Driver.MYSQL, settings=settings)

def get_mysqldate_sql():
    # 使用cursor()方法获取操作游标
    cursor = db_myself.cursor()
    ssql = """SELECT DISTINCT right(ts_code,2) AS '市场代码',left(ts_code,6) AS '合约代码',CONCAT(DATE(trade_date),' 00:00:00')  AS '时间',`open` AS '开',high AS '高',low AS '低',`close` as '收',vol as '成交量' FROM stock_data_day ORDER BY ts_code,trade_date;"""
    try:
        # 执行SQL语句
        cursor.execute(ssql)
        # 获取查询结果
        #data_sql=cursor.fetchall()
        data_sql = pd.read_sql_query(ssql, engine)

    except:
        # 发生错误时回滚
        # db_myself.rollback()
        data_sql = 0
        print('不存在！')
        pass
    # 关闭游标
    cursor.close()
    # 关闭数据库连接
    db_myself.close()
    return  data_sql
#----------------------------------------------------------------------

# 封装函数
def move_df_to_mysql(imported_data:pd.DataFrame):#,collection_name:str):
    bars = []
    start = None
    count = 0

    for row in imported_data.itertuples():

        bar = BarData(
              symbol=row.symbol,
              exchange=row.exchange,
              datetime=row.datetime,
              interval=row.interval,
              volume=row.volume,
              open_price=row.open,
              high_price=row.high,
              low_price=row.low,
              close_price=row.close,
              open_interest=row.open_interest,
              gateway_name="DB",
        )
        bars.append(bar)

        # do some statistics
        count += 1
        if not start:
            start = bar.datetime
    end = bar.datetime

    # insert into database
    #database_manager.save_bar_data(bars, collection_name)
    # 替换函数 move_df_to_mongodb 的倒数第二行
    mysql_manager.save_bar_data(bars)
    print(f'Insert Bar: {count} from {start} - {end}')

if __name__ == "__main__":
    # 读取需要入库的csv文件，该文件是用gbk编码
    #imported_data = pd.read_csv('D:/1分钟数据压缩包/FutAC_Min1_Std_2016/ag主力连续.csv',encoding='gbk')
    imported_data=get_mysqldate_sql()
    imported_data['持仓量'] = 0
    print(imported_data.head(5))
    # 将csv文件中 `市场代码`的 SC 替换成 Exchange.SHFE
    imported_data.loc[imported_data['市场代码'] == 'SZ', 'exchange'] = "SZSE"
    imported_data.loc[imported_data['市场代码'] == 'SH', 'exchange'] = "SSE"
    #imported_data['市场代码'] = Exchange.SHFE
    # 增加一列数据 `inteval`，且该列数据的所有值都是 Interval.MINUTE
    imported_data['interval'] = "d"
    # 明确需要是float数据类型的列
    float_columns = ['开', '高', '低', '收', '成交量', '持仓量']
    for col in float_columns:
      imported_data[col] = imported_data[col].astype('float')

    float_columns = ['市场代码','合约代码','exchange']
    for col in float_columns:
      imported_data[col] = imported_data[col].astype('str')
    # 明确时间戳的格式
    # %Y/%m/%d %H:%M:%S 代表着你的csv数据中的时间戳必须是 2020/05/01 08:32:30 格式
    #datetime_format = '%Y%m%d %H:%M:%S'
    #imported_data['时间'] = pd.to_datetime(imported_data['时间'],format=datetime_format)
    # 因为没有用到 市场代码 这一列的数据，所以该列列名不变
    imported_data.columns = ['市场代码' ,'symbol','datetime','open','high','low','close','volume','open_interest','exchange','interval']
    #imported_data['symbol'] = imported_data['合约代码']
    print(imported_data.head(5))
    imported_data.head(5).to_csv('aaa.csv')
    code_df=imported_data['symbol'].drop_duplicates(keep='first', inplace=False)
    code_list=code_df.values.tolist()
    print(len(code_list))
    for code in code_list:
        move_df_to_mysql(imported_data)



