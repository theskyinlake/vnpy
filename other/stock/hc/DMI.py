#!/usr/bin/env python3
#! -*- coding: utf-8 -*-

import talib as ta
import pandas as pd

def DMI(dfd,N=14,MM=6):
    df=pd.DataFrame()
    #计算真实波幅（TR）
    #TR代表真实波幅，是当日价格较前一日价格的最大变动值。取以下三项差额的数值中的最大值（取绝对值）为当日的真实波幅：
    #A、当日的最高价减去当日的最低价的价差。
    #B、当日的最高价减去前一日的收盘价的价差。
    #C、当日的最低价减去前一日的收盘价的价差。
    #TR是A、B、C中的数值最大者
    df['trade_date']=dfd['trade_date']
    df['h-l']=dfd['high']-dfd['low']
    df['h-c']=abs(dfd['high']-dfd['close'].shift(1))
    df['l-c']=abs(dfd['close'].shift(1)-dfd['low'])
    df['tr']=df.max(axis=1)
    df['PDM']=dfd['high']-dfd['high'].shift(1)
    df['MDM']=dfd['low'].shift(1)-dfd['low']
    df['DPD']=0
    df['DMD']=0
    #df = df.set_index('trade_date')
    #df = pd.merge(dfd, df, on=['trade_date'])

    #计算当日动向值
    #1/上升动向（+DM）代表正趋向变动值即上升动向值，
    #其数值等于当日的最高价减去前一日的最高价，如果<=0 则+DM=0。
    #2/下降动向（-DM）代表负趋向变动值即下降动向值，
    #其数值等于前一日的最低价减去当日的最低价，如果<=0 则-DM=0。注意-DM也是非负数。
    #再比较+DM和-DM，较大的那个数字保持，较小的数字归0。
    #3/无动向代表当日动向值为“零”的情况，即当日的+DM和﹣DM同时等于零。
    #有两种股价波动情况下可能出现无动向。
    #一是当当日的最高价低于前一日的最高价并且当日的最低价高于前一日的最低价，
    #二是当上升动向值正好等于下降动向值。
    for i in range(len(df.index)):
        PDM=df.loc[i,'PDM']
        MDM=df.loc[i,'MDM']
        if PDM<0 or PDM<MDM:
            df.loc[i,'DPD']=0
        else:
            df.loc[i,'DPD']=PDM
        if MDM<0 or MDM<PDM:
            df.loc[i,'DMD']=0
        else:
            df.loc[i,'DMD']=MDM
    #print('#####################df2##############################')
    #print(df)

    #计算方向线DI
    #+DI（14）=（+DM14÷TR14）×100
    #-DI（14）=（-DM14÷TR14）×100
    N=14
    D=len(df.index)
    for i in range(D-N):
        #print(i,i+1,i+N+1,D,df.iloc[i+1:i+N+1,7])
        df.loc[D-i-N-1,'NTR']=sum(df.iloc[i+1:i+N+1,4])#'tr'
        df.loc[D-i-N-1,'NPDM']=sum(df.iloc[i+1:i+N+1,7])#'DPD'
        df.loc[D-i-N-1,'NMDM']=sum(df.iloc[i+1:i+N+1,8])#'DMD'

    df['PDI']=df['NPDM']/df['NTR']*100
    df['MDI']=df['NMDM']/df['NTR']*100
    #print('#####################df3##############################')
    #print(df)
    #计算动向平均数ADX
    #依据DI值可以计算出DX指标值。
    #其计算方法是将+DI和—DI间的差的绝对值除以总和的百分比得到动向指数DX。
    #由于DX的波动幅度比较大，一般以一定的周期的平滑计算，得到平均动向指标ADX。具体过程如下：
    #DX=(DI DIF÷DI SUM) ×100
    #其中，DI DIF为上升指标和下降指标的差的绝对值
    #DI SUM为上升指标和下降指标的总和
    #ADX就是DX的一定周期n的移动平均值。
    #ADXR=（当日的ADX+前n日的ADX）÷2
    df = df.sort_values(by=['trade_date'], ascending=1)
    df['DX']=abs(df['MDI']-df['PDI'])/(df['MDI']+df['PDI'])*100
    df['ADX']=0
    MM=6
    for i in range(MM,len(df.index)):
        summDX=0
        for j in range(i-MM,i):
            summDX+=df.loc[j,'DX']
        df.loc[i-MM,'ADX']=summDX/MM
    for i in range(MM, len(df.index)):
        summADX=df.loc[i-MM,'ADX']+df.loc[i,'ADX']
        df.loc[i-MM,'ADXR']=summADX/2
    #print('#####################df4##############################')
    #print(df)
    #df.to_csv('test.csv')
    dfd['dMA'] = ta.MA(dfd['close'].values, timeperiod=3)
    df = pd.merge(dfd, df, on=['trade_date'])
    df = df[[ 'trade_date',  'dMA', 'PDI', 'MDI', 'ADX', 'ADXR']]
    # print('#####################df4##############################')
    # df=df.tail(5)
    # df = df.set_index('trade_date')
    df = df.sort_values(by=['trade_date'], ascending=0)
    ######################################################################################################################################
    # 一、上升指标+DI和下降指标-DI的研判功能
    # 1、当股价走势向上发展，而同时+DI从下方向上突破-DI时，表明市场上有新多买家进场，为买入信号，如果ADX伴随上升，则预示股价的涨势可能更强劲。
    df.loc[(df['dMA'] > df['dMA'].shift(-1)) & (df['PDI'] > df['MDI']) & (
                df['PDI'].shift(-1) < df['MDI'].shift(-1)), 'S1'] = '买入'
    # 2、当股价走势向下发展时，而同时+DI从上向下突破-DI时，表明市场上做空力量在加强，为卖出信号，如果ADX伴随下降 [1]  ，则预示跌势将加剧。
    df.loc[(df['dMA'] < df['dMA'].shift(-1)) & (df['PDI'] < df['MDI']) & (
                df['PDI'].shift(-1) > df['MDI'].shift(-1)), 'S2'] = '卖出'
    # 3、当股价维持某种上升或下降行情时，+DI和﹣DI的交叉突破信号比较准确，但当股价维持盘整时，应将+DI和-DI交叉发出的买卖信号视为无效。
    # 二、平均动向指标ADX的研判功能
    # ADX为动向值DX的平均数，而DX是根据+DI和-DI两数值的差和对比计算出来的百分比，因此，利用ADX指标将更有效地判断市场行情的发展趋势。
    # 1、判断行情趋势
    # 当行情走势由横盘向上发展时，ADX值会不断递增。因此，当ADX值高于前一日时，可以判断当前市场行情仍在维持原有的上升趋势，即股价将继续上涨，如果+DI和﹣DI同时增加，则表明当前上升趋势将十分强劲。
    # 当行情走势进入横盘阶段时，ADX值会不断递减。因此，判断行情时，应结合股价走势（+DI和-DI）走势进行判断。
    # 当行情走势由盘整向下发展时，ADX值也会不断递减。因此，当ADX值低于前一日时，可以判断当前市场行情仍维持原有的下降趋势，即股价将继续下跌，如果+DI和-DI同时减少，则表示当前的跌势将延续。
    # 2、判断行情是否盘整
    # 当市场行情在一定区域内小幅横盘盘整时，ADX值会出现递减情况。当ADX值降至20以下，且呈横向窄幅移动时，可以判断行情为牛皮盘整，上升或下跌趋势不明朗，投资者应以观望为主，不可依据+DI和-DI的交叉信号来买卖股票。
    df.loc[df['ADX'] <= 20, 'S3'] = '横盘盘整'
    # 3、判断行情是否转势
    # 当ADX值在高点由升转跌时，预示行情即将反转。在涨势中的ADX在高点由升转跌，预示涨势即将告一段落；在跌势中的ADX值从高位回落，预示跌势可能停止。
    # 一、四线交叉原则
    # 1、当+DI线同时在ADX线和ADXR线及-DI线以下（特别是在50线以下的位置时），说明市场处于弱市之中，股市向下运行的趋势还没有改变，股价可能还要下跌，投资者应持币观望或逢高卖出股票为主，不可轻易买入股票。这点是DMI指标研判的重点。
    df.loc[(df['PDI'] < df['MDI']) & (df['PDI'] < df['ADX']) & (df['PDI'] < df['ADXR']), 'S4'] = '弱市持币观望或逢高卖出'
    # 2、当+DI线和-DI线同处50以下时，如果+DI线快速向上突破-DI线，预示新的主力已进场，股价短期内将大涨。如果伴随大的成交量放出，更能确认行情将向上，投资者应迅速短线买入股票。
    df.loc[(df['PDI'] < 50) & (df['MDI'] < 50) & (df['PDI'] > df['MDI']) & (
                df['PDI'].shift(-1) < df['MDI'].shift(-1)), 'S5'] = '主力进场查看成交量'
    # 3、当+DI线从上向下突破-DI线（即-DI线从下向上突破+DI线）时，此时不论+DI和-DI处在什么位置都预示新的空头进场，股价将下跌，投资者应短线卖出股票或以持币观望为主。
    df.loc[(df['PDI'] < df['MDI']) & (df['PDI'].shift(-1) > df['MDI'].shift(-1)), 'S6'] = '空头进场卖出股票或以持币观望'
    # 4、当+DI线、-DI线、ADX线和ADXR线等四线同时在50线以下绞合在一起窄幅横向运动，说明市场处于波澜不兴，股价处于横向整理之中，此时投资者应以持币观望为主。
    df.loc[(df['PDI'] < 50) & (df['MDI'] < 50) & (df['ADX'] < 50) & (df['ADXR'] < 50), 'S7'] = '横向整理持币观望'
    # 5、当+DI线、ADX线和ADXR线等三线同时在50线以下的位置，而此时三条线都快速向上发散，说明市场人气旺盛，股价处在上涨走势之中，投资者可逢低买入﹣或持股待涨。（这点中因为-DI线是下降方向线，其对上涨走势反应不灵，故不予以考虑）。
    df.loc[(df['PDI'].shift(-1) < 50) & (df['ADX'].shift(-1) < 50) & (df['ADXR'].shift(-1) < 50) & (
                df['PDI'] > df['PDI'].shift(-1)) & (df['ADX'] > df['ADX'].shift(-1)) & (
                       df['ADXR'] > df['ADXR'].shift(-1)), 'S8'] = '逢低买入或持股待涨'
    # 6、对于牛股来说，ADX在50以上向下转折，仅仅回落到40——60之间，随即再度掉头向上攀升，而且股价在此期间走出横盘整理的态势。随着ADX再度回升，股价向上再次大涨，这是股价拉升时的征兆。这种情况经常出现在一些大涨的牛股中，此时DMI指标只是提供一个向上大趋势即将来临的参考。在实际操作中，则必须结合均线系统和均量线及其他指标一起研判。
    df.loc[(df['PDI'] > df['MDI']) & (df['PDI'] > df['ADX']) & (df['PDI'] > df['ADXR']), 'S9'] = '逢低买入或持股待涨'
    df = df.fillna(0)
    df=df.head(1)
    return df#['PDI'],df['MDI'],df['ADX'],df['ADXR'],dfd['S1'],dfd['S2'],dfd['S3'],dfd['S4'],dfd['S5'],dfd['S6'],dfd['S7'],dfd['S8'],dfd['S9']

'''
# Momentum Indicator Functions
# ADX - Average Directional Movement Index
# 函数名：ADX
# 名称：平均趋向指数
# 简介：使用ADX指标，指标判断盘整、振荡和单边趋势。
# 公式：
# 一、先决定股价趋势（Directional Movement，DM）是上涨或下跌：
# “所谓DM值，今日股价波动幅度大于昨日股价波动幅部分的最大值，可能是创高价的部分或创低价的部分；如果今日股价波动幅度较前一日小，则DM = 0。”
# 若股价高点持续走高，为上涨趋势，记作 +DM。
# 若为下跌趋势，记作 -DM。-DM的负号（–）是表示反向趋势（下跌），并非数值为负数。
# 其他状况：DM = 0。
# 二、寻找股价的真实波幅（True Range，TR）：
# 所谓真实波幅（TR）是以最高价，最低价，及前一日收盘价三个价格做比较，求出当日股价波动的最大幅度。
# 三、趋势方向需经由一段时间来观察，研判上才有意义。一般以14天为指标的观察周期：
# 先计算出 +DM、–DM及TR的14日算术平均数，得到 +DM14、–DM14及TR14三组数据作为起始值，再计算各自的移动平均值（EMA）。
#     +DI14 = +DM/TR14*100
#     -DI14 = +DM/TR14*100
#     DX = |(+DI14)-(-DI14)| / |(+DI14)+(-DI14)|
#     DX运算结果取其绝对值，再将DX作移动平均，得到ADX。
# 特点：
# ADX无法告诉你趋势的发展方向。
# 如果趋势存在，ADX可以衡量趋势的强度。不论上升趋势或下降趋势，ADX看起来都一样。
# ADX的读数越大，趋势越明显。衡量趋势强度时，需要比较几天的ADX 读数，观察ADX究竟是上升或下降。ADX读数上升，代表趋势转强；如果ADX读数下降，意味着趋势转弱。
# 当ADX曲线向上攀升，趋势越来越强，应该会持续发展。如果ADX曲线下滑，代表趋势开始转弱，反转的可能性增加。
# 单就ADX本身来说，由于指标落后价格走势，所以算不上是很好的指标，不适合单就ADX进行操作。可是，如果与其他指标配合运用，ADX可以确认市场是否存在趋势，并衡量趋势的强度。
# 指标应用：
# +DI与–DI表示多空相反的二个动向，当据此绘出的两条曲线彼此纠结相缠时，代表上涨力道与下跌力道相当，多空势均力敌。当 +DI与–DI彼此穿越时，由下往上的一方其力道开始压过由上往下的另一方，此时出现买卖讯号。
# ADX可作为趋势行情的判断依据，当行情明显朝多空任一方向进行时，ADX数值都会显著上升，趋势走强。若行情呈现盘整格局时，ADX会低于 +DI与–DI二条线。若ADX数值低于20，则不论DI表现如何，均显示市场没有明显趋势。
# ADX持续偏高时，代表“超买”（Overbought）或“超卖”（Oversold）的现象，行情反转的机会将增加，此时则不适宜顺势操作。当ADX数值从上升趋势转为下跌时，则代表行情即将反转；若ADX数值由下跌趋势转为上升时，行情将止跌回升。
# 总言之，DMI指标包含4条线：+DI、-DI、ADX和ADXR。+DI代表买盘的强度、-DI代表卖盘的强度；ADX代表趋势的强度、ADXR则为ADX的移动平均。
# NOTE: The ADX function has an unstable period.
# real = ADX(high, low, close, timeperiod=14)
df["ADX"] = talib.ADX(high, low, close, timeperiod=14)

# ADXR- Average Directional Movement Index Rating
# 函数名：ADXR
# 名称：平均趋向指数的趋向指数
# 简介：使用ADXR指标，指标判断ADX趋势。 NOTE: The ADXR function has an unstable period.
# real = ADXR(high, low, close, timeperiod=14)
df["ADXR"] = talib.ADXR(high, low, close, timeperiod=14)

# DX - Directional Movement Index DMI指标又叫动向指标或趋向指标
# 函数名：DX
# 名称：动向指标或趋向指标
# 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。
# 分析和应用：百度百科 维基百科 同花顺学院
# NOTE: The DX function has an unstable period.
# real = DX(high, low, close, timeperiod=14)
df["DX"] = talib.DX(high, low, close, timeperiod=14)

# MINUS_DI - Minus Directional Indicator
# 函数名：DMI 中的DI指标 负方向指标
# 名称：下升动向值
# 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。
# 分析和应用：百度百科 维基百科 同花顺学院 NOTE: The MINUS_DI function has an unstable period.
# real = MINUS_DI(high, low, close, timeperiod=14)
df["MINUS_DI"] = talib.MINUS_DI(high, low, close, timeperiod=14)

# MINUS_DM - Minus Directional Movement
# 函数名：MINUS_DM
# 名称： 上升动向值 DMI中的DM代表正趋向变动值即上升动向值
# 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，从而提供对趋势判断依据的一种技术指标。
# 分析和应用：百度百科 维基百科 同花顺学院
# NOTE: The MINUS_DM function has an unstable period.
# real = MINUS_DM(high, low, timeperiod=14)
df["MINUS_DM"] = talib.MINUS_DM(high, low, timeperiod=14)

# MOM - Momentum 动量
# 函数名：MOM
# 名称： 上升动向值
# 简介：投资学中意思为续航，指股票(或经济指数)持续增长的能力。研究发现，赢家组合在牛市中存在着正的动量效应，输家组合在熊市中存在着负的动量效应。
# real = MOM(close, timeperiod=10)
df["MOM"] = talib.MOM(close, timeperiod=10)

# PLUS_DI - Plus Directional Indicator
# NOTE: The PLUS_DI function has an unstable period.
# real = PLUS_DI(high, low, close, timeperiod=14)
df["PLUS_DI"] = talib.PLUS_DI(high, low, close, timeperiod=14)

# PLUS_DM - Plus Directional Movement
# NOTE: The PLUS_DM function has an unstable period.
# real = PLUS_DM(high, low, timeperiod=14)
df["PLUS_DM"] = talib.PLUS_DM(high, low, timeperiod=14)


ADX - 平均定向运动指数
real = ADX(high, low, close, timeperiod=14)
ADXR - 平均定向运动指数评级
real = ADXR(high, low, close, timeperiod=14)
DX - 定向运动指数
real = DX(high, low, close, timeperiod=14)
MINUS_DI - 减去定向指标
real = MINUS_DI(high, low, close, timeperiod=14)
MINUS_DM - 减去定向运动
real = MINUS_DM(high, low, timeperiod=14)        
PLUS_DI - 再加上定向指标
real = PLUS_DI(high, low, close, timeperiod=14)
PLUS_DM - 再加上定向运动
real = PLUS_DM(high, low, timeperiod=14)
CCI - 商品通道索引
real = CCI(high, low, close, timeperiod=14)
'''

