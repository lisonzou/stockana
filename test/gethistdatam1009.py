import tushare as ts
import mysql.connector
import datetime, time, re, sys, os
import threading
import logging, logging.handlers

conf_filename = 'stocktrade.conf'
global mylogger
global myhandler
global default_loglevel
global progress_percent
stockmount = 0
stocknums = 0
totalknums = 0
global mrlock

def init_mylogger(action, logger):
    selfdirname, selffilename = os.path.split(os.path.abspath(sys.argv[0]))
    log_filename = selfdirname + '\\log\\stocktrade.log'
    formatter = logging.Formatter('%(asctime)s - %(levelname)s  - %(message)s')
    handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1024 * 1024, backupCount=5)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if default_loglevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif default_loglevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif default_loglevel == 'ERROR':
        logger.setLevel(logging.ERROR)
    elif default_loglevel == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif default_loglevel == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)

    if action == 'empty':
        logger.removeHandler(handler)
    return logger, handler

def table_exists(con, table_name):
    # 这个函数用来判断表是否存在
    sql = "show tables;"
    con.execute(sql)
    tables = [con.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return 1
    else:
        return 0


def inserthistdata(board, df, value_code, x, period):
    mylogger.info("插入股票" + value_code[x][1] + period + " K线数据")
    cursor, conn = mysqlconn()
    for i in range(0, len(df)):
        if period == 'D' or period == 'M' or period == 'W':
            times = time.strptime(df.index[i], '%Y-%m-%d')
            time_new = time.strftime('%Y%m%d', times)
        else:
            times = time.strptime(df.index[i], '%Y-%m-%d %H:%M:%S')
            time_new = time.strftime('%Y%m%d %H:%M:%S', times)
        # 对于字符串的字段，%s要加单引号
        cursor.execute(
            "insert into stock_hist_data_" + board + "(code, name, date, open, close, high, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20, period) values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')" % (
                value_code[x][0], value_code[x][1], time_new, df.open[i], df.close[i], df.high[i], df.low[i],
                df.volume[i],
                df.price_change[i], df.p_change[i], df.ma5[i], df.ma10[i], df.ma20[i], df.v_ma5[i],
                df.v_ma10[i], df.v_ma20[i], period))
    conn.commit()
    cursor.close()
    conn.close
    return len(df)


def gethistdatam(board, value_code, startrows, endrows, startday, endday):
    b = 0

    # 通过for循环遍历每一只股票
    for x in range(startrows, endrows):
        # print(value_code[x][0])
        if startday == 'all':
            df = ts.get_hist_data(value_code[x][0])
            # print(df)
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'D')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='W')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'W')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='M')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'M')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='5')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '5')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='15')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '15')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='30')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '30')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='60')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '60')
                b += datanum
        else:
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday)
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'D')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='W')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'W')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='M')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, 'M')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='5')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '5')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='15')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '15')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='30')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '30')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='60')
            if df is not None:
                datanum = inserthistdata(board, df, value_code, x, '60')
                b += datanum
        global totalknums
        global stocknums
        global mrlock
        global stockmount
        mrlock.acquire()
        totalknums += b
        stocknums += 1
        progress_percent = (stocknums / stockmount) * 100
        mylogger.info("stock_hist_data_" + board + " 股票'%s' 共插入%d条数据。该板目前完成股票数量为%d，插入总条数为%d，完成进度为%.2f%%" % (value_code[x][1], b, stocknums, totalknums, progress_percent))
        mrlock.release()
        b = 0


def starthreads(board, stockcode, startday, endday):
    if board == 'sza': cs = "where code like \'000%\' or code like \'001%\'"
    if board == 'sha': cs = "where code like \'60%\'"
    if board == 'zxb': cs = "where code like \'002%\'"
    if board == 'cyb': cs = "where code like \'30%\'"
    if board == 'kcb': cs = "where code like \'68%\'"

    cursor, conn = mysqlconn()

    if stockcode == 'all':
        cursor.execute("SELECT code,name FROM stockinfo " + cs)
    else:
        cursor.execute("SELECT code,name FROM stockinfo WHERE code = %s" % (stockcode))

    value_code = cursor.fetchall()
    # print(len(value_code))

    if table_exists(cursor, 'stock_hist_data_' + board) != 1:
        cursor.execute(
            "create table stock_hist_data_" + board + " (code varchar(32), name varchar(32), date varchar(32), open varchar(32), close  varchar(32), high varchar(32), low varchar(32), volume varchar(32), price_change varchar(32), p_change varchar(32), ma5 varchar(32), ma10 varchar(32), ma20 varchar(32), v_ma5 varchar(32), v_ma10 varchar(32), V_ma20 varchar(32), period varchar(32))")
        mylogger.info('stock_hist_data_%s表格创建完成' % board)

    conn.commit()
    cursor.close()
    conn.close()

    global stockmount
    stockmount = len(value_code)
    mylogger.info(board + "股票数量： " + str(stockmount))
    global mrlock
    mrlock = threading.Lock()
    threads = []
    if stockcode == 'all':
        threadnums = len(value_code) // 100
        remainders = len(value_code) % 100
        if threadnums == 0:
            startrows = 0
            endrows = len(value_code)
            # gethistdatam(board, value_code, startrows, endrows, startday, endday)
            threads.append(
                threading.Thread(target=gethistdatam, args=(board, value_code, startrows, endrows, startday, endday)))

        else:
            startrows = 0
            cid = 1
            while cid <= threadnums:
                endrows = cid * 100
                # print("starthread%d" % cid)
                # print(endrows)
                threads.append(threading.Thread(target=gethistdatam, args=(board, value_code, startrows, endrows, startday, endday)))
                startrows = cid * 100
                # print(startrows)
                cid += 1

            if remainders > 0:
                endrows = len(value_code)
                # print("remainer")
                # print(startrows, endrows)
                threads.append(threading.Thread(target=gethistdatam, args=(board, value_code, startrows, endrows, startday, endday)))

        for tn in threads:
            tn.start()
        for tn in threads:
            tn.join()
    else:
        startrows = 0
        endrows = 1
        # print(value_code)
        gethistdatam(board, value_code, startrows, endrows, startday, endday)

    # 统计总共插入了多少张表的数据
    mylogger.info("stock_hist_data_" + board + "共插入%d个股票%d条数据" % (stocknums, totalknums))
    return 0

def mysqlconn():
    mysqlconfig = dict(host='127.0.0.1', user='root', password='123', port=3306, database='stock', charset='utf8')
    conn = mysql.connector.connect(**mysqlconfig)
    cursor = conn.cursor()
    return cursor, conn

# 获取股票日线、周线等历史数据，板块board(sza,sha,cyb,zxb,kcb)，股票代码code(code,all)、开始日期(date,all)、结束日期
# gethistdata(board, stockcode, starttime, endtime)

default_loglevel = 'INFO'
logger1 = logging.getLogger('stocktrade')
if len(logger1.handlers) > 0:
    mylogger, myhandler = init_mylogger('empty', logger1)
else:
    mylogger, myhandler = init_mylogger('init', logger1)

mylogger.info('starthreads')
mylogger.info('开始创业板数据下载 ' + "开始时间：" + str(datetime))
starthreads('cyb', 'all', 'all', 'all')

exit(0)

for bd in ['kcb', 'sza', 'cyb', 'zxb', 'sha']:
    time1 = datetime.datetime.now()
    starthreads(bd, 'all', 'all', 'all')
    time2 = datetime.datetime.now()
    mylogger.info(bd + "开始时间：" + str(time1))
    mylogger.info(bd + "结束时间：" + str(time2))


"""
获取历史行情数据 get_hist_data()
获取个股历史交易数据（包括均线数据），可以通过参数设置获取日k线、周k线、月k线，以及5分钟、15分钟、30分钟和60分钟k线数据。本接口只能获取近3年的日线数据，适合搭配均线数据进行选股和分析。

参数说明：
code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
start：开始日期，格式YYYY-MM-DD
end：结束日期，格式YYYY-MM-DD
ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
retry_count：当网络异常后重试次数，默认为3
pause:重试时停顿秒数，默认为0
"""
