import threading
from queue import Queue
import pymysql
from pymysql.cursors import DictCursor
import time


db_conf = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123',
    'db': 'stock',
    'charset': 'utf8'
}


class ConnPoolException(Exception):
    """连接池出错 """


class MariaDBPool(object):
    _inst_lock = threading.RLock()


    def __init__(self, connections):
        print(connections)
        self.__connections = connections
        self.__pool = Queue(connections)
        # 在init阶段，就已经创建好指定的连接，全部put到共享队列
        for i in range(self.__connections):
            try:
                db_conf = {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': '123',
                    'db': 'stock',
                    'charset': 'utf8'
                }
                #print(db_conf)
                conn = pymysql.connect(**db_conf)
                self.__pool.put(conn)
            except ConnPoolException as e:
                raise IOError

                # 单例模式创建连接池，个人喜欢用__new__方法创建，简洁，且使用了递归锁，保证在多线程方式创建单例模式的对象都是同一对象

    def __new__(cls, *args, **kwargs):
        with cls._inst_lock:
            if not hasattr(cls, '_inst'):
                cls._inst = object.__new__(cls)
        return cls._inst

    def execute_insert(self, sql, data_dict=None):
        conn = self.__pool.get()
        cursor = conn.cursor(DictCursor)
        try:
            result = cursor.execute(sql, data_dict) if data_dict else cursor.execute(sql)
            conn.commit()
        except ConnPoolException as e:
            print(e)
            # 这里就是重点，只是关闭了游标，连接对像又返回池里
            conn.rollback()
            cursor.close()
            self.__pool.put(conn)
            return False
        else:
            # 这里就是重点，只是关闭了游标，连接对像又返回池里
            cursor.close()
            self.__pool.put(conn)
            return result

    def executemany_insert(self, sql, data_dict_list=None):
        conn = self.__pool.get()
        cursor = conn.cursor(DictCursor)
        try:
            result = cursor.execute(sql, data_dict_list) if data_dict_list else cursor.executemany(sql)
        except ConnPoolException as e:
            conn.rollback()
            cursor.close()
            self.__pool.put(conn)
            return False
        else:
            cursor.close()
            self.__pool.put(conn)
            return result

    # 这里才是真正的关闭所有连接池
    def close(self):
        for i in range(self.__connections):
            self.__pool.get().close()

def inserdata(db_pool, insert_sql):
    j = 0
    while j < 100:
        db_pool.execute_insert(insert_sql)
        j += 1



def multi_insert(nums=151):
    insert_sql = "insert into zlx VALUE ('0001','云红')"
    #insert_data = ('0001', '云宏')
    dp_pool = MariaDBPool(100)
    treads = []
    for i in range(nums):
        t = threading.Thread(target=inserdata, args=(dp_pool,insert_sql,))
        treads.append(t)
        print(i)
        i += 1
    for t in treads:
        t.start()
    for t in treads:
        t.join()

def run(request_nums):
    start = time.time()
    multi_insert(request_nums)
    end = time.time()
    cost = end - start
    print('cost:{0:.3} s'.format(cost))

if __name__ == '__main__':
    run(100)

# 原文链接：https://blog.csdn.net/pysense/article/details/100127896