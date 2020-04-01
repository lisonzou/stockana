import datetime
import logging.handlers
from stocklogger import init_mylogger
from gethistdatam import *

# 获取股票日线、周线等历史数据，板块board(sza,sha,cyb,zxb,kcb)，股票代码code(code,all)、开始日期(date,all)、结束日期
# gethistdata(board, stockcode, starttime, endtime)

default_loglevel = 'INFO'
logger1 = logging.getLogger('stockana')
if len(logger1.handlers) > 0:
    mylogger, myhandler = init_mylogger('empty', logger1, default_loglevel)
else:
    mylogger, myhandler = init_mylogger('init', logger1, default_loglevel)

nowtime = datetime.datetime.now()
mylogger.info('开始每日数据下载 ' + "开始时间：" + str(nowtime))
# starthreads(mylogger, 'cyb', '300001', '2019-09-27', '2019-09-27')

# connpool = MysqlConn()

curday = datetime.date.today()
oneday = datetime.timedelta(days = 1)
yesterday = (curday-oneday).strftime("%Y-%m-%d")
print(yesterday)

yesterday = '2020-02-12'
for bd in ['kcb', 'sza', 'cyb', 'zxb', 'sha']:
    time1 = datetime.datetime.now()
    starthreads(mylogger, bd, 'all', yesterday, yesterday)
    time2 = datetime.datetime.now()
    mylogger.info(bd + "开始时间：" + str(time1))
    timeuse = (time2 - time1).seconds
    mylogger.info(("%s 结束时间：%s 用时%s秒") % (bd, str(time2), str(timeuse)))
