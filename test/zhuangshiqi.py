

class Screen(object):
    @property
    def width(self):
        return self._width

    @width.setter
    def width(self, value):
        self._width = value

    @property
    def height(self):
        return self._height

    @height.setter
    def height(self, values):
        self._height = values

    @staticmethod
    def ind():
        a = 3
        b = 2
        print(a*b)

    @property
    def resolution(self):
        return self._width * self._height


s = Screen()
s.ind()
s.width = 1024
s.height = 768
print('resolution = ',s.resolution)
