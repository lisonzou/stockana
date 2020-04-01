import mysql.connector
import MySQLdb
from DBUtils.PooledDB import PooledDB
from readconfig import get_config

# from queue import Queue

"""
Config是一些数据库的配置文件,通过调用我们写的readConfig来获取配置文件中对应值
"""
host = get_config('DATABASE', 'host')
port = int(get_config('DATABASE', 'port'))
user = get_config('DATABASE', 'user')
passwd = get_config('DATABASE', 'passwd')
database = get_config('DATABASE', 'database')
dbchar = get_config('DATABASE', 'dbchar')


class MysqlConn(object):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = self.__getconn()
        self._cursor = self._conn.cursor()

    @staticmethod
    def __getconn():
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if MysqlConn.__pool is None:
            __pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=20, host=host, port=port, user=user,
                              passwd=passwd, db=database, charset = dbchar)
        return __pool.connection()

    def dispose(self):
        """
        @summary: 释放连接池资源
        """
        self._cursor.close()
        self._conn.close()

def mysql_conn():
    mysqlconfig = dict(host=host, user=user, password=passwd, port=port, database=database, charset=dbchar)
    try:
        conn = mysql.connector.connect(**mysqlconfig)
        cursor = conn.cursor()
    except Exception as e:
        print(e)
    return cursor, conn
