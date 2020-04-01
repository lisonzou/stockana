import datetime

d = datetime.datetime.now()

def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    date_from = datetime.datetime(day.year, day.month, day.day, 0, 0, 0)
    date_to = datetime.datetime(day.year, day.month, day.day, 23, 59, 59)
    print('---'.join([str(date_from), str(date_to)]))

def week_get(d):
    dayscount = datetime.timedelta(days=d.isoweekday())
    dayto = d - dayscount
    sixdays = datetime.timedelta(days=6)
    dayfrom = dayto - sixdays
    date_from = datetime.datetime(dayfrom.year, dayfrom.month, dayfrom.day, 0, 0, 0)
    date_to = datetime.datetime(dayto.year, dayto.month, dayto.day, 23, 59, 59)
    print('---'.join([str(date_from), str(date_to)]))

def month_get(d):
    """
    返回上个月第一个天和最后一天的日期时间
    :return
    date_from: 2016-01-01 00:00:00
    date_to: 2016-01-31 23:59:59
    """
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime.datetime(dayto.year, dayto.month, 1, 0, 0, 0)
    date_to = datetime.datetime(dayto.year, dayto.month, dayto.day, 23, 59, 59)
    print('---'.join([str(date_from), str(date_to)]))
    return date_from, date_to


def getYesterday():
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1)
    yesterday=today-oneday
    return yesterday

print(getYesterday())

from datetime import datetime, date, timedelta

curday = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() + timedelta(days = -1)).strftime("%Y-%m-%d")
print(curday, yesterday)

# day_get(d)