#去除DataFrame重复项
#df_income = df_income_p.drop_duplicates(subset=None,keep='first',inplace=False)  
df=pd.merge(peg_df,pe_df, on=['ts_code'])

多重条件查询：查找 age<3 且为 cat 的全部数据。
df[(df['animal'] == 'cat') & (df['age'] < 3)]
关键字查询：
df3[df3['animal'].isin(['cat', 'dog'])]

按照 age 降序，visits 升序排列
df.sort_values(by=['age', 'visits'], ascending=[False, True])

多值替换：
将 priority 列的 yes 值替换为 True，no 值替换为 False。
df['priority'].map({'yes': True, 'no': False})

分组求和：​
df4.groupby('animal').sum()





index={'上证综指': '000001.SH','深证成指': '399001.SZ',
        '沪深300': '000300.SH','创业板指': '399006.SZ',
        '上证50': '000016.SH','中证500': '000905.SH',
        '中小板指': '399005.SZ','上证180': '000010.SH'}
#获取当前交易的股票代码和名称
def get_code():
    df = pro.stock_basic(exchange='', list_status='L')
    codes=df.ts_code.values
    names=df.name.values
    stock=dict(zip(names,codes))
    #合并指数和个股成一个字典
    stocks=dict(stock,**index)
    return stocks    
#获取行情数据
def get_data(stock,start='20190101',end='20191231'):
    #如果代码在字典index里，则取的是指数数据
    code=get_code()[stock]
    if code in index.values():
        df=ts.pro_bar(ts_code=code,asset='I',start_date=start, end_date=end)
    #否则取的是个股数据
    else:
        df=ts.pro_bar(ts_code=code, adj='qfq',start_date=start, end_date=end)
    #将交易日期设置为索引值
    df.index=pd.to_datetime(df.trade_date)
    df=df.sort_index()
    return df

def get_csv():
    codes = pd.read_csv('stock.csv',encoding='utf_8_sig')  
    codes=codes.ts_code.values.tolist()
    #print(len(codes),codes)
    return codes

pd.set_option('display.max_rows', 100)  # 显示的最大行数（避免只显示部分行数据）
pd.set_option('display.max_columns', 1000)  # 显示的最大列数（避免列显示不全）
pd.set_option("display.max_colwidth", 1000)  # 每一列最大的宽度（避免属性值或列名显示不全）
pd.set_option('display.width', 1000)  # 每一行的宽度（避免换行）


pd.set_option('expand_frame_repr', False)  # 当列太多时不换行




1. EPS表示每股收益（一般按年计算）:
EPS=归属于普通股股东的当期净利润/当期实际发行在外的普通股加权平均数
2. PE表示市盈率PE是当前股价（P）相对每股收益（EPS）的比值：
PE =P/EPS
3. G表示企业的收益增长率G（Growth Rate of Expected Profit）
收益增长率的计算方法不一，简便起见，本文取EPS增长率：
G=EPS this year−EPS last year / EPS last year
4. 基于以上几个指标，得出PEG的计算公式:
PEG=PE/ （G∗100）

定义：市盈率相对盈利增长比率, 这个指标是用公司的市盈率除以公司的盈利增长速度。
公式：历史PEG值TTM＝市盈率TTM／G值TTM
市盈率TTM = 当日总市值/最近12个月净利润(TTM)
G值TTM = ((最近12个月净利润(TTM)/上年同期12个月净利润(TTM)) - 1) * 100%

筛选出符合PEG估值法运用条件的股票。
周期性行业： 利润基础稳定不住， 使用PEG容易造成误差
非成长股： 收益曾藏律或者市盈率为负的， 不符合高增长
项目类公司： 利润高低依赖于公司接的项目数，利润基础稳定不住
稳定的大型公司：这种公司往往稳定有余而成长不足
融资依赖型： 因融资带来的高增长，不能稳定持续



策略思路
布兰德斯在其著作《Value Investing Today：Proven Techniques for Long-term Growth》（今日的价值投资） 中阐述了自己的价值投资理念。而查尔斯·布兰德斯价值投资法正是其关键投资理念的具体实现。
该投资策略主要从三个角度对股票提出要求，
其一、公司要具备强力的资产负债表；
其二、要具备合理的估值；
其三、公司经营阶层具备诚信且持股充足。
以这三个角度出发选择六个指标，分别是
负债净值比；
董监事会持股比例；
PE（相对全市场）；
市现率（相对全市场）；
市净率；
市净率（相对全市场）。
这六个指标共同构建了选股准则，形成了查尔斯·布兰德斯价值投资法。这六条准则非常清晰明确，均为可直接量化实现的准则。每条准则具有明确的量化描述和判断条件，除个别部分需对中国市场调整外，我们的回测将严格按照这六条准则复制，不存在需主观判断而不能量化的准则。
策略细节
归纳查尔斯·布兰德斯价值投资策略初始版本，可以大致总结策略衡量的三个方面：
用市盈率、市净率、市现率来衡量股票的估值，要求买入的投资标的足够“便宜”，具有合理的估值；
用负债净值比来衡量公司的偿债能力和财务结构，保证买入的投资标的具备足够强大的资产负债表；
用董监事会持股比例来衡量董监事会对股东的诚信和尽职程度，保证管理层的管理水平。
其价值选股标准也主要围绕着这三个方面，具体为：
1）  查尔斯·布兰德斯的七条选股标准：
股票最近一季度负债净值比小于40%    
董监事会持股比例大于市场平均值
股票的市盈率小于市场平均值股票的股价/近四季现金流量
（市现率）小于市场平均值
股票的市净率小于市场平均值
股票的市净率小于1.0倍
以上标准是查尔斯·布兰德斯基于当时的市场条件，面对美国股票市场提出的标准，对于国内A股市场而言，有必要对其中一些标准的阈值进行调整。这样既可以保障查尔斯·布兰德斯投资理念得到实现，也可以保证选则出的股票数量处于合理的水平。
2）  调整后的查尔斯·布兰德斯选股标准：
股票最近一季度负债净值比小于8%  负债净值比率=总负债/资产净值=总负债/（总资产-总负债)=total_liab（负债合计）/total_liab_hldr_eqy（股东权益合计(含少数股东权益)）  balancesheet表
董监事会持股比例不低于市场平均值的50%
股票的市盈率不高于市场平均值的1.5倍         daily_basic表  pe_ttm 
（市现率）不高于市场平均值的1.5倍        市现率(PCF, 现金净流量)TTM＝(股票在指定交易日期的收盘价×当日人民币外汇挂牌价×截止当日公司总股本)／现金及现金等价物净增加额TTM  n_incr_cash_cash_equ
股票的市净率不高于市场平均值的1.5倍      市净率＝(股票在指定交易日期的收盘价×当日人民币外汇牌价×截至当日公司总股本)／最新报告期的归属母公司股东的权益
股票的市净率小于2.0倍