# 总的运行文件，实现将统计报告发送邮件到自己的邮箱，将这个文件放到Jenkin上每个交易日下午3点之后运行就可以收到当天满足行情的股票了
import win_rates
import write_everyday
import time
import os, sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


# 获取最新的文件
def new_file(test_report_dir):
    lists = os.listdir(test_report_dir)
    lists.sort(key=lambda fn: os.path.getmtime(test_report_dir + fn))
    file_path = os.path.join(test_report_dir, lists[-1])
    return file_path


# 发送邮件
def send_email(test_report_dir) -> object:
    f = open(new_file(test_report_dir), 'rb')
    mail_body = f.read()
    # print(mail_body)
    f.close()
    # 设置自己邮件服务器和账号密码
    smtpserver = 'smtp.163.com'
    user = 'lisonzou@163.com'
    password = '1szfblz.'
    # 设置接收邮箱和主题
    sender = user
    receiver = 'lisonzou@163.com'
    subject = '今天的股票行情来啦'

    msg = MIMEMultipart('mixed')
    att = MIMEText(mail_body, 'txt', 'utf-8')
    att['Content-Type'] = 'application/octet-stream'
    att['Content-Disposition'] = 'attachment; filename = "%s.txt"' % todays

    msg.attach(att)

    msg['From'] = user
    msg['To'] = receiver
    msg['Subject'] = Header(subject, 'utf-8')

    smtp = smtplib.SMTP()
    smtp.connect(smtpserver, 25)
    smtp.login(user, password)
    smtp.sendmail(sender, receiver, msg.as_string())
    smtp.quit()


if __name__ == '__main__':
    # test_report_dir = 'D:\\python\\work\\stock\\WD\\run\\report\\'
    dirname, selffilename = os.path.split(os.path.abspath(sys.argv[0]))
    test_report_dir = dirname + '\\report\\'

    # 如果执行的不是当天的日期的话请将第一个todays注释掉
    # todays = time.strftime('%Y-%m-%d')
    todays = '2018-03-8'
    # 如果不是交易日执行的话write_everyday会报错，会报tushare获取不到行情，所以请手动输入日期并将下面一行注释掉
    write_everyday.everystock()
    time.sleep(3)
    win_rates.rate(todays)

    send_email(test_report_dir)
