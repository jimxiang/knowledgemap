import hashlib
import random
import string
import time
import uuid


def gen_uuid():
    return str(uuid.uuid1())


def gen_id(node_type=''):
    if node_type == 'UNKNOWN':
        return None

    s = ''.join(random.sample(string.ascii_lowercase, 10))
    t = ''.join([s, random_date(), gen_uuid()])
    m = hashlib.md5()
    m.update(bytes(str(t)))
    ran_id = m.hexdigest()
    return ''.join([node_type, ran_id])


def random_date():
    a1 = (2000, 1, 1, 0, 0, 0, 0, 0, 0)
    a2 = (2018, 12, 31, 23, 59, 59, 0, 0, 0)

    start = time.mktime(a1)
    end = time.mktime(a2)

    t = random.randint(start, end)
    date_tuple = time.localtime(t)
    date = time.strftime("%Y-%m-%d", date_tuple)
    return date


def random_int():
    return random.randint(10000, 10000000)


def random_index(rate):
    start = 0
    index = 0
    rand_num = random.randint(1, sum(rate))

    for index, scope in enumerate(rate):
        start += scope
        if rand_num <= start:
            break
    return index
