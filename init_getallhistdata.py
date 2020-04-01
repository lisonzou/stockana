from gethistdatam import *
from getstockinfo import *

mylogger = getmylogger()

nowtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
mylogger.info('初始化历史数据 ' + "开始时间：" + str(nowtime))

# 初始化所有股票信息
getstockinfo()

# 获取股票日线、周线等历史数据，板块board(sza,sha,cyb,zxb,kcb)，股票代码code(code,all)、开始日期(date,all)、结束日期
# starthreads(mylogger， board, stockcode, starttime, endtime)
# 例子：starthreads(mylogger, 'cyb', '300001', '2019-09-27', '2019-09-27') 获取某一天某只股票的所有K线数据
# 下面是按板块初始化所有股票的历史数据，包括日线、周线、月线、5分钟、15分钟、30分钟、60分钟K线
for bd in ['sza', 'kcb','cyb', 'zxb', 'sha']:
    time1 = datetime.datetime.now()
    starthreads(mylogger, bd, 'all', 'all', 'all')
    time2 = datetime.datetime.now()
    mylogger.info(bd + "开始时间：" + str(time1))
    timeuse = (time2 - time1).seconds
    mylogger.info(("%s 结束时间：%s 用时%s秒") % (bd, str(time2), str(timeuse)))
