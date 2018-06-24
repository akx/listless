import secrets
import time

from callisto2 import Callisto

N = 15000
marker = secrets.token_hex(16)

db_names = [
    secrets.token_hex(16)
    for x in range(32)
]


def time_dur(label, fn):
    t0 = time.perf_counter()
    fn()
    t1 = time.perf_counter()
    print(label, t1 - t0)


def writes():
    with Callisto('./foocaltest') as c:
        for db_name in db_names:
            for x in range(N):
                c.put(db_name, ('%s = %08d' % (marker, x)).encode('utf-8'))
            assert c.count(db_name) >= N


def reads():
    with Callisto('./foocaltest') as c:
        for db_name in db_names:
            x = list(c.get(db_name))
            assert x[-1] == ('%s = %08d' % (marker, N - 1)).encode('utf-8')
            assert len(x) >= N


time_dur('writes', writes)
time_dur('reads', reads)
