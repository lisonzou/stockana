import datetime
import time

oldtime=datetime.datetime.now()
print(oldtime)
time.sleep(1)
newtime=datetime.datetime.now()
print(newtime)

print(('相差：%s') % (newtime - oldtime))
print(('相差：%s微秒') % (newtime - oldtime).microseconds)
print(('相差：%s秒') % (newtime - oldtime).seconds)
