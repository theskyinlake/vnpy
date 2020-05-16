#!/usr/bin/env python3
# ! -*- coding: utf-8 -*-
#----------------------------------------------------------------------
'''
def loadTdxCsv(fileName, dbName, symbol):
    """��ͨ���ŵ�����csv��ʽ����ʷ�������ݲ��뵽Mongo���ݿ���"""
    import csv
    
    start = time()
    print u'��ʼ��ȡCSV�ļ�%s�е����ݲ��뵽%s��%s��' %(fileName, dbName, symbol)
    
    # �������ϣ�����������
    host, port, logging = loadMongoSetting()
    
    client = pymongo.MongoClient(host, port)    
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)   
    
    # ��ȡ���ݺͲ��뵽���ݿ�
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
        #Ϊ��ʱ�Ľ�� if �ж����� else Ϊ��ʱ�Ľ�� 
        if len(str(d[1]))==3:
            bar.time = d[1][0]+':'+d[1][-2:]+':00'
            #print "d[1][1:]",d[1][1],"d[1][2:]",d[1][2:3]
        elif len(str(d[1]))==4:
            bar.time = d[1][:2]+':'+d[1][-2:]+':00'#:2ǰ��2���ַ�����:-2����2���ַ���.
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
    
    print u'������ϣ���ʱ��%s' % (time()-start)
'''