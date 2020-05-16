'''
import tushare as ts
#设置ts令牌
ts.set_token('809eb4ebed2618990f0da9579f701706196b84216a9d7302fa08ac26')
#300274为股票代号，直接通过get_hist_data()获取的数据是逆序的，正序数据需要加上sort.index()
data=ts.get_hist_data('300274',start='2019-01-01',end='2019-12-31').sort_index()
print(data)
#选择需要标签存储股票数据
data.to_csv('300274.csv',
columns=['open','high','low','close','volume'])
#data.to_csv('C:/testbacktrader/tushare/300274.csv')

from __future__ import (absolute_import, division, print_function, unicode_literals)
import datetime
import os.path
import sys
import backtrader as bt
import tushare as ts
import argparse
import pandas as pd

class TestStrategy(bt.Strategy):
    params = (('maperiod', 15),
        ('printlog', False))

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.maperiod)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f '%
                        (order.executed.price,
                        order.executed.value,
                        order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                        (order.executed.price,
                        order.executed.value,
                        order.executed.comm)
                        )
                self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:\
            #self.log('Order Canceled/Margin/Rejected')
            self.order = None
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                    (trade.pnl, trade.pnlcomm))

    def next(self):
        self.log('Close, %.2f' % self.dataclose[0])
        if self.order:
            return
        # 简单策略，当日收盘价大于简单滑动平均值买入，当日收盘价小于简单滑动平均值卖出
        if not self.position:
            if self.dataclose[0] > self.sma[0]:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.order = self.buy()
        else:
            if self.dataclose[0] < self.sma[0]:
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()
    def stop(self):
        self.log('(MA Period %2d) Ending Value %.2f' %
        (self.params.maperiod, self.broker.getvalue()), doprint=True)

if __name__ == '__main__':
        #创建主控制器
        cerebro = bt.Cerebro()
        #导入策略参数寻优
        cerebro.optstrategy(TestStrategy,maperiod=range(10, 31))
        #准备股票日线数据，输入到backtrader
        datapath = ('300274.csv')
        dataframe = pd.read_csv(datapath, index_col=0, parse_dates=True)
        dataframe['openinterest'] = 0
        data = bt.feeds.PandasData(dataname=dataframe,
                            fromdate=datetime.datetime(2019, 1, 1),
                            todate=datetime.datetime(2019, 12, 31)
        )
        cerebro.adddata(data)
        #broker设置资金、手续费
        cerebro.broker.setcash(1000.0)
        cerebro.broker.setcommission(commission=0.001)
        #设置买入设置，策略，数量
        cerebro.addsizer(bt.sizers.FixedSize, stake=10)
        print('Starting Portfolio Value: %.2f' %
        cerebro.broker.getvalue())
        cerebro.run(maxcpus=1)
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
'''

mpl.rcParams['axes.unicode_minus']=False
class my_strategy1(bt.Strategy):
    #全局设定交易策略的参数
    params=(
        ('maperiod',20),
           )

    def __init__(self):
        #指定价格序列
        self.dataclose=self.datas[0].close
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buyprice = None
        self.buycomm = None

        #添加移动均线指标，内置了talib模块
        self.sma = bt.indicators.SimpleMovingAverage(
                      self.datas[0], period=self.params.maperiod)
    def next(self):
        if self.order: # 检查是否有指令等待执行,
            return
        # 检查是否持仓
        if not self.position: # 没有持仓
            #执行买入条件判断：收盘价格上涨突破20日均线
            if self.dataclose[0] > self.sma[0]:
                #执行买入
                self.order = self.buy(size=500)
        else:
            #执行卖出条件判断：收盘价格跌破20日均线
            if self.dataclose[0] < self.sma[0]:
                #执行卖出
                self.order = self.sell(size=500)