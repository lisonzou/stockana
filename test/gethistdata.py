import tushare as ts
import mysql.connector
import datetime, time, re


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


def inserthistdata(cursor, board, df, value_code, x, period):
    print("插入股票" + value_code[x][1] + period + " K线数据")
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
    return len(df)


def gethistdata(cursor, conn, board, stockcode, startday, endday):
    if board == 'sza': cs = "where code like \'000%\' or code like \'001%\'"
    if board == 'sha': cs = "where code like \'60%\'"
    if board == 'zxb': cs = "where code like \'002%\'"
    if board == 'cyb': cs = "where code like \'30%\'"
    if board == 'kcb': cs = "where code like \'68%\'"

    if stockcode == 'all':
        cursor.execute("SELECT code,name FROM stockinfo " + cs)
    else:
        cursor.execute("SELECT code,name FROM stockinfo WHERE code = %s" % (stockcode))

    value_code = cursor.fetchall()
    # print(len(value_code))

    if table_exists(cursor, 'stock_hist_data_' + board) != 1:
        cursor.execute(
            "create table stock_hist_data_" + board + " (code varchar(32), name varchar(32), date varchar(32), open varchar(32), close  varchar(32), high varchar(32), low varchar(32), volume varchar(32), price_change varchar(32), p_change varchar(32), ma5 varchar(32), ma10 varchar(32), ma20 varchar(32), v_ma5 varchar(32), v_ma10 varchar(32), V_ma20 varchar(32), period varchar(32))")
        print('stock_hist_data_%s表格创建完成' % board)

    a = 0
    b = 0
    c = 0
    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in range(0, len(value_code)):
        #print(value_code[x][0])
        if startday == 'all':
            df = ts.get_hist_data(value_code[x][0])
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'D')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='W')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'W')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='M')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'M')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='5')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '5')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='15')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '15')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='30')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '30')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], ktype='60')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '60')
                b += datanum
        else:
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday)
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'D')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='W')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'W')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='M')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, 'M')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='5')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '5')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='15')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '15')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='30')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '30')
                b += datanum
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='60')
            if df is not None:
                datanum = inserthistdata(cursor, board, df, value_code, x, '60')
                b += datanum
        print("stock_hist_data_" + board + " 股票'%s' 共插入%d条数据" % (value_code[x][1], b))
        conn.commit()
        c += b
        b = 0
        a += 1


    # 统计总共插入了多少张表的数据
    print("stock_hist_data_" + board + "共插入%d个股票%d条数据" % (a, c))
    return 0


mysqlconfig = dict(host='127.0.0.1', user='root', password='123', port=3306, database='stock', charset='utf8')
conn = mysql.connector.connect(**mysqlconfig)
cursor = conn.cursor()

# 获取股票日线、周线等历史数据，板块board(sza,sha,cyb,zxb,kcb)，股票代码code(code,all)、开始日期(date,all)、结束日期
# gethistdata(cursor, board, stockcode, starttime, endtime

# gethistdata(cursor, conn, 'kcb', 'all', 'all', 'all')

for bd in ['kcb', 'sza', 'cyb', 'zxb', 'sha']:
    time1 = datetime.datetime.now()
    gethistdata(cursor, conn, bd, 'all', 'all', 'all')
    time2 = datetime.datetime.now()
    print(bd + "开始时间：" + str(time1))
    print(bd + "结束时间：" + str(time2))


conn.close()
cursor.close()

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
