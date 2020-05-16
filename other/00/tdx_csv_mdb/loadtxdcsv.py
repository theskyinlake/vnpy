#----------------------------------------------------------------------
def loadTdxCsv(fileName, dbName, symbol):
    """将通达信导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    import csv
    
    start = time()
    print u'开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol)
    
    # 锁定集合，并创建索引
    host, port, logging = loadMongoSetting()
    
    client = pymongo.MongoClient(host, port)    
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # 读取数据和插入到数据库
    reader = csv.reader(file(fileName, 'r'))
    skip_line=0
    for d in reader:
        if skip_line<2:#222784:
            skip_line=skip_line+1
            continue
        bar = CtaBarData()
        bar.vtSymbol = symbol
        bar.symbol = symbol
        bar.open = float(d[2])
        bar.high = float(d[3])
        bar.low = float(d[4])
        bar.close = float(d[5])
        #为真时的结果 if 判定条件 else 为假时的结果 
        if len(str(d[1]))==3:
            bar.time = d[1][0]+':'+d[1][-2:]+':00'
            #print "d[1][1:]",d[1][1],"d[1][2:]",d[1][2:3]
        elif len(str(d[1]))==4:
            bar.time = d[1][:2]+':'+d[1][-2:]+':00'#:2前面2个字符串，:-2后面2个字符串.
        else:
            continue
            
        

        bar.date = datetime.strptime(d[0], '%Y/%m/%d').strftime('%Y%m%d')
        #print "bar.time",bar.time,"bar.date",bar.date,"d1",d[1]
        bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
        bar.volume = d[6]
        #bar.openInterest = d[7]

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)  
        print bar.date, bar.time
    
    print u'插入完毕，耗时：%s' % (time()-start)