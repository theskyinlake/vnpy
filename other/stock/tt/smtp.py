from smtplib import SMTP_SSL 

from email.mime.text import MIMEText 

from email.header import Header 

  

email_from = "2147607204@qq.com" #改为自己的发送邮箱 

email_to = "400523@qq.com" #接收邮箱 

hostname = "smtp.qq.com" #不变，QQ邮箱的smtp服务器地址 

login = "2147607204@qq.com" #发送邮箱的用户名 

password = "gnvgurnojddbdjde" #发送邮箱的密码，即开启smtp服务得到的授权码。注：不是QQ密码。 

subject = "今天peg推荐" #邮件主题 

text = "send email" #邮件正文内容 

  

smtp = SMTP_SSL(hostname)#SMTP_SSL默认使用465端口 

smtp.login(login, password) 

  

msg = MIMEText(text, "plain", "utf-8") 

msg["Subject"] = Header(subject, "utf-8") 

msg["from"] = email_from 

msg["to"] = email_to 

  

smtp.sendmail(email_from, email_to, msg.as_string()) 

smtp.quit()