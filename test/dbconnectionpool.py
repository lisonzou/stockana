import pymysql
import threading
import re
import time
import tushare as ts
from queue import Queue
from DBUtils.PooledDB import PooledDB

class mysql_conn(object):
    "多线程并发MySQL插入数据"
    def __init__(self):
        start_time = time.time()
        self.pool = self.mysql_connection()
        # self.data = self.getData()
        # self.mysql_delete()
        # self.task()
        # print("========= 数据插入,共耗时:{}'s =========".format(round(time.time() - start_time, 3)))

    def mysql_connection(self):
        maxconnections = 15  # 最大连接数
        pool = PooledDB(
            pymysql,
            maxconnections,
            host='localhost',
            user='root',
            port=3306,
            passwd='123',
            db='stock',
            charset = 'utf8',
            use_unicode=True)
        return pool

    def getData(self):
        st = time.time()
        with open("10w.txt", "rb") as f:
            data = []
            for line in f:
                line = re.sub("\s", "", str(line, encoding="utf-8"))
                line = tuple(line[1:-1].split("\"\""))
                data.append(line)
        n = 100000    # 按每10万行数据为最小单位拆分成嵌套列表
        result = [data[i:i + n] for i in range(0, len(data), n)]
        print("共获取{}组数据,每组{}个元素.==>> 耗时:{}'s".format(len(result), n, round(time.time() - st, 3)))
        return result

    def mysql_delete(self):
        st = time.time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "TRUNCATE TABLE allstock"
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()
        print("清空原数据.==>> 耗时:{}'s".format(round(time.time() - st, 3)))

    def mysql_insert(self, args, value_code):
        con = self.pool.connection()
        cur = con.cursor()
        period = 'D'
        df = args
        board = 'sza'
        x = 0
        print(len(df))
        stockcode = '000001'
        cur.execute("SELECT code,name FROM stockinfo WHERE code = %s" % (stockcode))
        value_code = cur.fetchall()
        try:
            for i in range(0, len(df)):
                print(df.open[i])
                if period == 'D' or period == 'M' or period == 'W':
                    times = time.strptime(df.index[i], '%Y-%m-%d')
                    time_new = time.strftime('%Y-%m-%d', times)
                else:
                    times = time.strptime(df.index[i], '%Y-%m-%d %H:%M:%S')
                    time_new = time.strftime('%Y-%m-%d %H:%M:%S', times)
                # 对于字符串的字段，%s要加单引号
                cur.execute(
                    "insert into stock_hist_data_" + board + "(code, name, date, open, close, high, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20, period) values('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')" % (
                        value_code[x][0], value_code[x][1], time_new, df.open[i], df.close[i], df.high[i], df.low[i],
                        df.volume[i],
                        df.price_change[i], df.p_change[i], df.ma5[i], df.ma10[i], df.ma20[i], df.v_ma5[i],
                        df.v_ma10[i], df.v_ma20[i], period))
            # cur.executemany(sql, *args)
            con.commit()
        except Exception as e:
            con.rollback()  # 事务回滚
            print('SQL执行有误,原因:', e)
        finally:
            cur.close()
            con.close()


if __name__ == '__main__':
    # ThreadInsert()
    connpool = mysql_conn()
    cursor = connpool.cursor()

    cursor.execute("SELECT code,name FROM stockinfo where code like '000%'")

    value_code = cursor.fetchall()
    print(len(value_code))

    board = 'sza'
    stockcode = 'all'
    global stockmount
    global stocknums
    global totalknums
    global progress_percent
    stockmount = len(value_code)
    stocknums = 0
    totalknums = 0
    progress_percent = 0

    print("股票数量： " + str(stockmount))

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
                threading.Thread(target=connpool.mysql_insert, args=(board, value_code, startrows, endrows)))
        else:
            startrows = 0
            cid = 1
            while cid <= threadnums:
                endrows = cid * step
                print("starthread%d" % cid)
                # print(endrows)
                threads.append(
                threading.Thread(target=connpool.mysql_insert, args=(board, value_code, startrows, endrows)))
                startrows = cid * step
                # print(startrows)
                cid += 1

            if remainders > 0:
                endrows = len(value_code)
                print("remainer")
                # print(startrows, endrows)
                threads.append(threading.Thread(target=connpool.mysql_insert, args=(board, value_code, startrows, endrows)))
        for tn in threads:
            tn.start()
        for tn in threads:
            tn.join()
    else:
        startrows = 0
        endrows = 1
        print(value_code)
        connpool.mysql_insert(board, value_code, startrows, endrows)

        i = 0
        thread_list = []
        # q = Queue(maxsize=10)  # 设定最大队列数和线程数
        st = time.time()
        value_code = '000001'
        content = ts.get_hist_data(value_code)
        print(content)
        while i < 1:
            # content = self.data.pop()
            # content = [('608760', 'yunhong', 'jisuanji', 'guangzhou'), ('608763', 'yunhong', 'jisuanji', 'guangzhou'), ('608765', 'yunhong', 'jisuanji', 'guangzhou'), ('608766', 'yunhong', 'jisuanji', 'guangzhou'), ('608769','yunhong', 'jisuanji', 'guangzhou' )]
            # print(content)
            t = threading.Thread(target=conn.mysql_insert, args=(content, value_code))
            print(t)
            thread_list.append(t)
            # q.put(t)
            #if (q.full() == True):
            i += 1

                # while q.empty() == False:
                    #t = q.get()
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()

        print("数据插入完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))

"""
版权声明：本文为CSDN博主「Test_Box」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/joson1234567890/article/details/90730193
"""
