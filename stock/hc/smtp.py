#!/usr/bin/env python3
#! -*- coding: utf-8 -*-
import time
import smtplib
from smtplib import SMTP_SSL 
from email.header import Header   
from email.mime.text import MIMEText   
from email.mime.multipart import MIMEMultipart

def send_mail(path,df):   
    df= df.to_html(header = True,index = False)
    dt_now = str(time.strftime("%Y%m%d", time.localtime()))
    email_from = "2147607204@qq.com" #改为自己的发送邮箱 
    email_to = "400523@qq.com" #接收邮箱 
    hostname = "smtp.qq.com" #不变，邮箱的smtp服务器地址 
    login = "2147607204@qq.com" #发送邮箱的用户名 
    password = "gnvgurnojddbdjde" #发送邮箱的密码，即开启smtp服务得到的授权码。注：不是QQ密码。 
    subject = dt_now+"推荐" #邮件主题 
    text = "<html><body>"+df+"</body></html>" #邮件正文内容  
    
    #创建一个带附件的实例
    message = MIMEMultipart()    
    # 构造附件1，传送当前目录下的 test.txt 文件
    att1 = MIMEText(open(path, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1["Content-Disposition"] = 'attachment; filename="stock"'
    message.attach(att1)    
    
    smtp = SMTP_SSL(hostname)#SMTP_SSL默认使用465端口 
    smtp.login(login, password)   
    msg = MIMEText(text, 'html', 'utf-8') #发送含HTML内容的邮件
    msg["Subject"] = Header(subject, "utf-8") 
    msg["from"] = email_from 
    msg["to"] = email_to   
    smtp.sendmail(email_from, email_to, msg.as_string()) 
    smtp.quit()