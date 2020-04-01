import tushare as ts
import mysql.connector
import time, re


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


def inserthistdata(cursor, df, value_code, x, period):
    print("period:" + period)
    print(df)
    for i in range(0, len(df)):
        if period == 'D' or period == 'M' or period == 'W':
            times = time.strptime(df.index[i], '%Y-%m-%d')
            time_new = time.strftime('%Y%m%d', times)
        else:
            times = time.strptime(df.index[i], '%Y-%m-%d %H:%M:%S')
            time_new = time.strftime('%Y%m%d %H:%M:%S', times)
        # 对于字符串的字段，%s要加单引号
        cursor.execute("insert into stock_hist_" + value_code[x][
            0] + "(date, open, close, high, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20, period) values('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')" % (
                                 time_new, df.open[i], df.close[i], df.high[i], df.low[i], df.volume[i],
                                 df.price_change[i], df.p_change[i], df.ma5[i], df.ma10[i], df.ma20[i], df.v_ma5[i],
                                 df.v_ma10[i], df.v_ma20[i], period))


def gethistdata(cursor, conn, stockcode, startday, endday):

    if stockcode == 'all':
        cursor.execute("SELECT code,name FROM stockinfo")
    else:
        cursor.execute("SELECT code,name FROM stockinfo WHERE code = %s" % (stockcode))

    value_code = cursor.fetchall()

    a = 0
    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in range(0, len(value_code)):
        if table_exists(cursor, 'stock_hist_' + value_code[x][0]) != 1:
            cursor.execute('create table stock_hist_' + value_code[x][0] + '(date varchar(32),open varchar(32), close '
                                                                           'varchar(32), high varchar(32), '
                                                                           'low varchar(32), volume varchar(32), '
                                                                           'price_change varchar(32), '
                                                                           'p_change varchar(32), ma5 varchar(32), '
                                                                           'ma10 varchar(32), ma20 varchar(32), '
                                                                           'v_ma5 varchar(32), v_ma10 varchar(32), '
                                                                           'V_ma20 varchar(32), period varchar(32))')
            print('%s的表格创建完成' % value_code[x][0])

        if startday == 'all':
            df = ts.get_hist_data(value_code[x][0])
            inserthistdata(cursor, df, value_code, x, 'D')
            df = ts.get_hist_data(value_code[x][0], ktype='W')
            inserthistdata(cursor, df, value_code, x, 'W')
            df = ts.get_hist_data(value_code[x][0], ktype='M')
            inserthistdata(cursor, df, value_code, x, 'M')
            df = ts.get_hist_data(value_code[x][0], ktype='5')
            inserthistdata(cursor, df, value_code, x, '5')
            df = ts.get_hist_data(value_code[x][0], ktype='15')
            inserthistdata(cursor, df, value_code, x, '15')
            df = ts.get_hist_data(value_code[x][0], ktype='30')
            inserthistdata(cursor, df, value_code, x, '30')
            df = ts.get_hist_data(value_code[x][0], ktype='60')
            inserthistdata(cursor, df, value_code, x, '60')
        else:
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday)
            inserthistdata(cursor, df, value_code, x, 'D')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='W')
            inserthistdata(cursor, df, value_code, x, 'W')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='M')
            inserthistdata(cursor, df, value_code, x, 'M')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='5')
            inserthistdata(cursor, df, value_code, x, '5')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='15')
            inserthistdata(cursor, df, value_code, x, '15')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='30')
            inserthistdata(cursor, df, value_code, x, '30')
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='60')
            inserthistdata(cursor, df, value_code, x, '60')
        a += 1

    conn.commit()

    # 统计总共插入了多少张表的数据
    print('所有股票总共插入数据库%d张表格' % a)


mysqlconfig = dict(host='127.0.0.1', user='root', password='123', port=3306, database='stock', charset='utf8')
conn = mysql.connector.connect(**mysqlconfig)
cursor = conn.cursor()


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

# 获取股票日线、周线等历史数据，股票代码、开始日期、结束日期
gethistdata(cursor, conn, '300005', 'all', 'all')

conn.close()
cursor.close()