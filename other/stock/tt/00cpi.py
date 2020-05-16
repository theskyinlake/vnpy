#! /usr/bin/env python3
# -*- coding: UTF-8 -*-

import re
import time
import requests
from bs4 import BeautifulSoup

def get_one_page(url,stt):
    data_tt={}
    html = requests.get(url)  
    soup = BeautifulSoup(html.content, 'lxml')
    items = soup.find(id="tb").find_all(string=re.compile('20\d{2}年.*'+stt+'|.*%.*', re.S))
    for x in range(1, len(items)):
        if items[x-1].find(stt)>0:
            data_tt[items[x-1].strip()]=items[x].strip()
    return data_tt

# gdp数据清洗
def gdpdata(datagdp):
    datagdp2={}
    for key,values in datagdp.items():
        if key.find('1季度')>0:     
            t1='01月份'
            t2='02月份'
            t3='03月份'        
        elif key.find('2季度')>0:
            t1='04月份'
            t2='05月份'
            t3='06月份' 
        elif key.find('3季度')>0:
            t1='07月份'
            t2='08月份'
            t3='09月份' 
        elif key.find('4季度')>0:
            t1='10月份'
            t2='11月份'
            t3='12月份' 
        listkey=key.split('第')
        t1=listkey[0]+t1
        t2=listkey[0]+t2
        t3=listkey[0]+t3
        datagdp2[t1]=values
        datagdp2[t2]=values
        datagdp2[t3]=values
    return  datagdp2

# 写入csv文件

def write_csv():
    import csv
    csvfile= open('cpi.csv','w',newline='')
    writer = csv.writer(csvfile)
    writer.writerow(['年月', 'ppi同比', 'cpi同比', 'gdp同比'])
    for key,values in data_ppi.items():
      writer.writerow([key,values,data_cpi.get(key),data_gdp2.get(key)])
    csvfile.close()


# 写入excel文件
def write_excel():
    import xlwt
    workbook = xlwt.Workbook(encoding = 'utf-8')
    worksheet = workbook.add_sheet('中国')
    row0 = [u'年月', u'ppi同比', 'cpi同比', 'gdp同比']
    worksheet.col(0).width = 3333
    for i in range(0,len(row0)):
        worksheet.write(0,i,row0[i])
    
    n=1
    for key in data_ppi:            
        worksheet.write(n,0,key)
        worksheet.write(n,1,data_ppi.get(key))
        worksheet.write(n,2,data_cpi.get(key))
        worksheet.write(n,3,data_gdp2.get(key))
        n=n+1
        
    workbook.save('cpi.xls')
    

# def main():

def usql():
    import pymysql
    # 打开数据库连接
    db = pymysql.connect(host='111.231.120.149',port=3306,user='py',passwd='123456',db='stock',charset='utf8')
    #db = pymysql.Connect(host='localhost',port=3306,user='py',passwd='123456',db='stock',charset='utf8')
    
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    
    pp =[]
    for key,values in data_ppi.items():
        sdate=str(key)   #.encode("UTF-8")
        sppi=str(values)
        scpi=str(data_cpi.get(key))
        sgdp=str(data_gdp2.get(key))
        pp = (sppi, scpi, sgdp,sdate)
        print(pp)
        
        # SQL 插入语句
        sql = "INSERT into tbl_china (fppi, fcpi, fgdp, fdate) VALUES (%s,%s,%s,%s)"
        
        # 执行sql语句       
        cursor.execute(sql,pp)
        # 提交到数据库执行
        db.commit()
        
        '''
        try: 
            # 执行sql语句
            cursor.execute(sql,pp)
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()
        '''
    
    # 关闭数据库连接
    db.close()


if __name__ == '__main__':
    url_ppi = "http://data.eastmoney.com/cjsj/productpricesindex.aspx?p="
    url_cpi = "http://data.eastmoney.com/cjsj/consumerpriceindex.aspx?p="
    url_gdp ="http://data.eastmoney.com/cjsj/grossdomesticproduct.aspx?p="
    
    data_cpi={}
    data_ppi={}
    data_gdp={}
    data_gdp2={}
    
    for i in range(1,10):
        data_ppi.update(get_one_page(url_ppi+str(i),'月份'))
        data_cpi.update(get_one_page(url_cpi+str(i),'月份'))
        data_gdp.update(get_one_page(url_gdp+str(i),'季度'))
        time.sleep(1)
        
    data_gdp2=gdpdata(data_gdp) # gdp数据清洗
    usql()
    # np.savetxt('d:\\sss\\ChinaCpiPpiGdp1np.csv', my_matrix, delimiter = ',')
    write_csv()  # 写入csv文件
    #write_excel()
    # print(data_ppi)
    # print(data_cpi)
    # print(data_gdp2)
 






