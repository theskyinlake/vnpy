#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import tushare as ts

# 设置token及数据库连接
ts.set_token('809eb4ebed2618990f0da9579f701706196b84216a9d7302fa08ac26')
pro = ts.pro_api()


pd.set_option('display.max_rows', 100)  # 显示的最大行数（避免只显示部分行数据）
pd.set_option('display.max_columns', 1000)  # 显示的最大列数（避免列显示不全）
pd.set_option("display.max_colwidth", 1000)  # 每一列最大的宽度（避免属性值或列名显示不全）
pd.set_option('display.width', 1000)  # 每一行的宽度（避免换行）