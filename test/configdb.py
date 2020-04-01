"""
1、执行带参数的ＳＱＬ时，请先用sql语句指定需要输入的条件列表，然后再用tuple/list进行条件批配
２、在格式ＳＱＬ中不需要使用引号指定数据类型，系统会根据输入参数自动识别
３、在输入的值中不需要使用转意函数，系统会自动处理
"""
import MySQLdb
from DBUtils.PooledDB import PooledDB
from readconfig import get_config
import threading
from queue import Queue
import time

"""
Config是一些数据库的配置文件,通过调用我们写的readConfig来获取配置文件中对应值
"""

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
    'maxcached': 20,
    'maxconnections': 70,  # 连接池最大连接数量
}

class MysqlConn(object):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Mysql.getConn()
            释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = MysqlConn.__getconn()
        self._cursor = self._conn.cursor()

    @staticmethod
    def __getconn():
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if MysqlConn.__pool is None:
            __pool = PooledDB(**dbconfig)
        return __pool.connection()

    def getall(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    def getone(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        print(result)
        return result

    def getmany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    def insertone(self, sql):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        self._cursor.execute(sql)
        self._conn.commit()
        print('insert')
        return 0
        # return self.__getinsertid()

    def insertmany(self, sql, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql, values)
        return count

    def __getinsertid(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

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


mysql = MysqlConn()

sqlinsert = "insert into zlx (code,name) values('%s', '%s')" % (str('0006'),'yunhong')
print(sqlinsert)
# sqlvalues = ('0003','yunhong')
# result = mysql.insertone(sqlinsert)

nums = 3
treads = []
for i in range(nums):
    t = threading.Thread(target=mysql.insertone, args=(sqlinsert,))
    treads.append(t)
    print(i)
    i += 1
for t in treads:
    t.start()
for t in treads:
    t.join()

print('end')

sql = "select * from zlx"    #sql语句，具体根据实际情况填写真实信息
result = mysql.getall(sql, None)
if result:
    print("get all")
    for row in result:
        print(row[0], row[1])
mysql.dispose()#释放连接池资源

"""
版权声明：本文为CSDN博主「songlh1234」的原创文章，遵循
CC
4.0
BY - SA
版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/songlh1234/article/details/84247951


        :param mincached:连接池中空闲连接的初始数量
        :param maxcached:连接池中空闲连接的最大数量
        :param maxshared:共享连接的最大数量
        :param maxconnections:创建连接池的最大数量
        :param blocking:超过最大连接数量时候的表现，为True等待连接数量下降，为false直接报错处理
        :param maxusage:单个连接的最大重复使用次数
        :param setsession:optional list of SQL commands that may serve to prepare
            the session, e.g. ["set datestyle to ...", "set time zone ..."]
        :param reset:how connections should be reset when returned to the pool
            (False or None to rollback transcations started with begin(),
            True to always issue a rollback for safety's sake)
        :param host:数据库ip地址
        :param port:数据库端口
        :param db:库名
        :param user:用户名
        :param passwd:密码
        :param charset:字符编码
"""
