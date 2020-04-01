import re
import threading
import time
import tushare as ts

from mysqldbpool import dbpool

totalknums = 0
stocknums = 0
mrlock = 0
stockmount = 0
progress_percent = 0
succnums = 0


def table_exists(cursor, table_name):
    # 这个函数用来判断表是否存在
    sql = "show tables;"
    cursor.execute(sql)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return 1
    else:
        return 0


def inserthistdata(db_pool, mylogger, df, value_code, x, period):
    mylogger.info("插入股票" + value_code[x][1] + period + " K线数据。")

    conn = db_pool.connection()
    cursor = conn.cursor()

    if table_exists(cursor, 'stock_hist_data_' + str(value_code[x][0])) != 1:
        cursor.execute(
            'create table stock_hist_data_' + str(value_code[x][0]) + "(code varchar(16), name varchar(32), date "
                                                                      "varchar(32), open varchar(32), close  varchar("
                                                                      "32), high varchar(32), low varchar(32), "
                                                                      "volume varchar(32), price_change varchar(32), "
                                                                      "p_change varchar(32), ma5 varchar(32), "
                                                                      "ma10 varchar(32), ma20 varchar(32), "
                                                                      "v_ma5 varchar(32), v_ma10 varchar(32), "
                                                                      "V_ma20 varchar(32), period varchar(8))")
        mylogger.info('stock_hist_data_%s表格创建完成。' % str(value_code[x][0]))

    conn.commit()

    for i in range(0, len(df)):
        if period == 'D' or period == 'M' or period == 'W':
            times = time.strptime(df.index[i], '%Y-%m-%d')
            time_new = time.strftime('%Y-%m-%d', times)
        else:
            times = time.strptime(df.index[i], '%Y-%m-%d %H:%M:%S')
            time_new = time.strftime('%Y-%m-%d %H:%M:%S', times)
        # 对于字符串的字段，%s要加单引号
        cursor.execute(
            'insert into stock_hist_data_' + str(value_code[x][
                                                     0]) + "(code, name, date, open, close, high, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20, period) values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')" % (
                value_code[x][0], value_code[x][1], time_new, df.open[i], df.close[i], df.high[i], df.low[i],
                df.volume[i],
                df.price_change[i], df.p_change[i], df.ma5[i], df.ma10[i], df.ma20[i], df.v_ma5[i],
                df.v_ma10[i], df.v_ma20[i], period))
    conn.commit()
    cursor.close()
    conn.close
    return len(df)


def gethistdatam(db_pool, mylogger, board, value_code, startrows, endrows, startday, endday):
    b = 0
    skn = 0
    datanum = 0
    global totalknums
    global stocknums
    global mrlock
    global stockmount
    global progress_percent
    global succnums

    # 通过for循环遍历每一只股票
    for x in range(startrows, endrows):
        if startday == 'all':
            df = ts.get_hist_data(value_code[x][0])
            # print(df)
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'D')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无日线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='W')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'W')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无周线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='M')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'M')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无月线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='5')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '5')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无5分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='15')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '15')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无15分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='30')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '30')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无30分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], ktype='60')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '60')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无60分钟线数据。" % (value_code[x][0], value_code[x][1]))
        else:
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday)
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'D')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无日线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='W')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'W')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无周线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], start=startday, end=endday, ktype='M')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, 'M')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无月线数据。" % (value_code[x][0], value_code[x][1]))
            starttime = startday + " 09:30:00"
            endtime = endday + " 15:00:00"
            df = ts.get_hist_data(value_code[x][0], start=starttime, end=endtime, ktype='5')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '5')
                    b += datanum
            else:
                mylogger.warning("stock_hist_data_" + board + " 股票%s%s 无5分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], start=starttime, end=endtime, ktype='15')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '15')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无15分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], start=starttime, end=endtime, ktype='30')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '30')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无30分钟线数据。" % (value_code[x][0], value_code[x][1]))
            df = ts.get_hist_data(value_code[x][0], start=starttime, end=endtime, ktype='60')
            if df is not None:
                if not df.empty:
                    datanum = inserthistdata(db_pool, mylogger, df, value_code, x, '60')
                    b += datanum
            else:
                mylogger.warning(
                    "stock_hist_data_" + board + " 股票%s%s 无60分钟线数据。" % (value_code[x][0], value_code[x][1]))

        if datanum > 0:
            skn = 1
        mrlock.acquire()
        succnums += skn
        totalknums += b
        stocknums += 1
        progress_percent = (stocknums / stockmount) * 100
        mylogger.info("stock_hist_data_" + board + " 股票%s%s 共插入%d条数据。该板目前完成股票数量为%d，成功股票数量为%d，插入总条数为%d，完成进度为%.2f%%。" % (
            value_code[x][0], value_code[x][1], b, stocknums, succnums, totalknums, progress_percent))
        print("stock_hist_data_" + board + " 股票%s%s 共插入%d条数据。该板目前完成股票数量为%d，成功股票数量为%d，插入总条数为%d，完成进度为%.2f%%。" % (
            value_code[x][0], value_code[x][1], b, stocknums, succnums, totalknums, progress_percent))
        mrlock.release()
        b = 0
        skn = 0
        datanum = 0


def starthreads(mylogger, board, stockcode, startday, endday):
    if board == 'sza': cs = "where code like \'000%\' or code like \'001%\'"
    if board == 'sha': cs = "where code like \'60%\'"
    if board == 'zxb': cs = "where code like \'002%\'"
    if board == 'cyb': cs = "where code like \'30%\'"
    if board == 'kcb': cs = "where code like \'68%\'"
    db_pool = dbpool()
    conn = db_pool.connection()
    cursor = conn.cursor()

    if stockcode == 'all':
        cursor.execute("SELECT code,name FROM stockinfo " + cs)
    else:
        cursor.execute("SELECT code,name FROM stockinfo WHERE code = %s" % (stockcode))

    value_code = cursor.fetchall()
    print(len(value_code))

    cursor.close()
    conn.close()

    global stockmount
    global stocknums
    global totalknums
    global progress_percent
    global succnums
    stockmount = len(value_code)
    stocknums = 0
    totalknums = 0
    succnums = 0
    progress_percent = 0

    mylogger.info(board + "股票数量： " + str(stockmount))

    if stockmount > 1000:
        step = 60
    elif stockmount > 500:
        step = 30
    elif stockmount > 200:
        step = 20
    else:
        step = 10

    global mrlock
    mrlock = threading.Lock()
    threads = []
    if stockcode == 'all':
        threadnums = len(value_code) // step
        remainders = len(value_code) % step
        if threadnums == 0:
            startrows = 0
            endrows = len(value_code)
            print("starthread single")
            threads.append(
                threading.Thread(target=gethistdatam,
                                 args=(db_pool, mylogger, board, value_code, startrows, endrows, startday, endday)))
        else:
            startrows = 0
            cid = 1
            while cid <= threadnums:
                endrows = cid * step
                print("starthread%d" % cid)
                # print(endrows)
                threads.append(threading.Thread(target=gethistdatam, args=(
                    db_pool, mylogger, board, value_code, startrows, endrows, startday, endday)))
                startrows = cid * step
                # print(startrows)
                cid += 1

            if remainders > 0:
                endrows = len(value_code)
                print("remainer")
                # print(startrows, endrows)
                threads.append(threading.Thread(target=gethistdatam, args=(
                    db_pool, mylogger, board, value_code, startrows, endrows, startday, endday)))

        for tn in threads:
            tn.start()
        for tn in threads:
            tn.join()
    else:
        startrows = 0
        endrows = 1
        print(value_code)
        gethistdatam(db_pool, mylogger, board, value_code, startrows, endrows, startday, endday)

    # 统计总共插入了多少张表的数据
    mylogger.info("stock_hist_data_" + board + "共插入%d个股票%d条数据。" % (stocknums, totalknums))
    # dbpool.dispose()
    return 0
