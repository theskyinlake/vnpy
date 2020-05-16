#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-

import time
import datetime
import talib as ta
import numpy as np
import pandas as pd
import tushare as ts



from hc.config import pro
from hc.DMI import DMI
#from hc.conner import engine,check_sql #,db,insert_sql, del_sql
from hc.getdata import get_trade_cal , get_last_trade_cal , get_last_trade_cal1,get_fina_indicator, get_basic, get_code_list_300, get_code_list_all #, get_moneyflow
print(get_trade_cal, get_last_trade_cal, get_last_trade_cal1)
