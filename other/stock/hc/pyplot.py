#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

#先引入后面可能用到的包（package）
import time
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

# linux字体设置
from matplotlib import font_manager
myfont = font_manager.FontProperties(fname="/usr/share/fonts/chinese/simhei.ttf")

from pylab import mpl
# 用来正常显示中文标签
mpl.rcParams['font.sans-serif']=['SimHei'] 
# 用来正常显示负号
mpl.rcParams['axes.unicode_minus']=False 

#Jupyter Notebook特有的magic命令直接在行内显示图形
# %matplotlib inline  

#plt.title('净值走势',fontsize=12)#,fontproperties = myfont)
#plt.legend(prop = myfont)

