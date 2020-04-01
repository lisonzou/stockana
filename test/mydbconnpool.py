import pymysql
from DBUtils.PooledDB import PooledDB
from readconfig import get_config

host = get_config('DATABASE', 'host')
port = int(get_config('DATABASE', 'port'))
user = get_config('DATABASE', 'user')
passwd = get_config('DATABASE', 'passwd')
database = get_config('DATABASE', 'database')
dbchar = get_config('DATABASE', 'dbchar')
dbconfig = {
        'creator': pymysql,
        'host': host,
        'port': port,
        'user': user,
        'password': passwd,
        'db': database,
        'charset': dbchar,
        'maxconnections': 70,  # 连接池最大连接数量
        'cursorclass': pymysql.cursors.DictCursor
    }


class MysqlPool(object):
    pool = PooledDB(**dbconfig)

    def __enter__(self):
        self.conn = MysqlPool.pool.connection()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, type, value, trace):
        self.cursor.close()
        self.conn.close()

"""
#sql = "insert into allstock(code, name, industry, area) VALUE ('100010', '云宏', 'jisuanji', 'gz')"
sql = "select * from allstock"
with MysqlPool() as db:
    db.cursor.execute(sql)
    res = db.cursor.fetchall()
    print(res)
    #db.conn.commit()
"""



# 原文链接：https: // blog.csdn.net / qq_29113041 / article / details / 99690070