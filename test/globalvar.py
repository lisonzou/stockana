a = 1
b = [2, 3]
c = 1


def func():
    a = 2
    print("in func a:", a)
    b[0] = 1
    print("in func b:", b)
    global c
    c = 3
    print("in func c:", c)


if __name__ == '__main__':
    print("before func a:", a)
    print("before func b:", b)
    print("before func c:", c)
    func()
    print("after func a:", a)
    print("after func b:", b)

print("after func c:", c)
