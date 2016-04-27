import time
from random import randint

class Timer:
    def __init__(self):
        self.start = time.time()

    def restart(self):
        self.start = time.time()

    def get_time_hhmmss(self):
        end = time.time()
        m, s = divmod(end - self.start, 60)
        h, m = divmod(m, 60)
        time_str = "%02d:%02d:%02d" % (h, m, s)
        return time_str

class Timestamp:
    def __init__(self):
        self.startTime = 1262304000
        self.currentTime = int(time.time())

    def random(self):
        randomTime = randint(self.startTime, self.currentTime)
        return randomTime