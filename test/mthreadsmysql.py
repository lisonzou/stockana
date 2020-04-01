#!/usr/bin/env python
# -*- coding:utf-8 -*-
import MySQLdb
import threading
import time
from DBUtils.PooledDB import PooledDB

"""
建立连接池，返回连接池地址
"""


def dbpool(ip,port,username,password,dbname,char_set='utf8'):
    connKwargs = {'host': ip, 'port': port, 'user': username, 'passwd': password, 'db': dbname,'charset': char_set}
    print(connKwargs)
    pool = PooledDB(MySQLdb, mincached=10, maxcached=10, maxshared=10, maxconnections=10, **connKwargs)
    return pool

'''
从连接池中取出一个连接，执行SQL
num：用于统计总的影响行数
'''
num=0
def dbconnect(db_pool):
    k = 0
    global num
    conn = db_pool.connection()
    cur = conn.cursor()
    try:
        while k < 10000:
            cur.execute("insert into zlx VALUE ('0001','云红')")
            lock.acquire()
            num += cur.rowcount
            lock.release()
            k += 1
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    """
	lock：生成全局锁，用于执行语句中，被影响行数的统计值加锁使用，每次只允许一个线程修改被锁变量
	"""

    lock = threading.Lock()
    st = time.time()
    db_pool = dbpool('localhost',3306,'root','123','stock','utf8')
    '''
    同时连接MySQL执行的线程数要小于等于前面PooledDB中设置的maxconnections，如果大于这个量，会报异常：TooManyConnections。
    设置每次只跑10个线程，跑完后再循环。
    '''
    thread_list = []
    for i in range(10):
        t = threading.Thread(target=dbconnect,args=(db_pool,))
        thread_list.append(t)
    while len(thread_list)!=0:
        if len(thread_list)>10:
            thread_list_length = 10
        else:
            thread_list_length = len(thread_list)
        sub_thread_list = []
        for n in range(thread_list_length):
            sub_thread_list.append(thread_list[0])
            thread_list.remove(thread_list[0])
        for i in sub_thread_list:
            i.start()
        for j in sub_thread_list:
            j.join()
    et = time.time()
    print(et - st,num)