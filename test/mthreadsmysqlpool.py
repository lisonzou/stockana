#!/usr/bin/env python
# -*- coding:utf-8 -*-
import MySQLdb
import threading
import time
from DBUtils.PooledDB import PooledDB
from readconfig import get_config

host = get_config('DATABASE', 'host')
port = int(get_config('DATABASE', 'port'))
user = get_config('DATABASE', 'user')
passwd = get_config('DATABASE', 'passwd')
database = get_config('DATABASE', 'database')
dbchar = get_config('DATABASE', 'dbchar')

dbconfig = {
    'creator': MySQLdb,
    'host': host,
    'port': port,
    'user': user,
    'password': passwd,
    'db': database,
    'charset': dbchar,
    'mincached': 1,
    'maxcached': 50,
    'maxconnections': 70,  # 连接池最大连接数量
}


"""
建立连接池，返回连接池地址
"""

class Mydb_pool(object):
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = Mydb_pool.getConn()
        self._cursor = self._conn.cursor()

    @staticmethod
    def getConn():
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if Mydb_pool.__pool is None:
            __pool = PooledDB(**dbconfig)
        return __pool.connection()


    def dispose(self, isend=1):
        """
        @summary: 释放连接池资源
        """
        if isend == 1:
            self.end('commit')
        else:
            self.end('rollback')
        print('close')
        self._cursor.close()
        self._conn.close()


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
    #conn = db_pool.connection()
    #cur = conn.cursor()
    #db_pool = Mydb_pool()
    conn = db_pool.getConn()
    cur = db_pool._cursor

    try:
        while k < 1000:
            cur.execute("insert into zlx VALUE ('0001','云红')")
            lock.acquire()
            num += cur.rowcount
            lock.release()
            k += 1
        conn.commit()
        print("insert 1000")
    except Exception as e:
        print("error")
        print(e)


if __name__ == '__main__':
    """
	lock：生成全局锁，用于执行语句中，被影响行数的统计值加锁使用，每次只允许一个线程修改被锁变量
	"""

    lock = threading.Lock()
    st = time.time()
    # db_pool = dbpool('localhost',3306,'root','123','stock','utf8')
    db_pool = Mydb_pool()
    '''
    同时连接MySQL执行的线程数要小于等于前面PooledDB中设置的maxconnections，如果大于这个量，会报异常：TooManyConnections。
    设置每次只跑10个线程，跑完后再循环。
    '''
    thread_list = []
    for i in range(2):
        t = threading.Thread(target=dbconnect,args=(db_pool,))
        thread_list.append(t)
    print(thread_list)

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
            print('start')
            i.start()
        for j in sub_thread_list:
            j.join()
            print('join')

    et = time.time()
    print(et - st,num)