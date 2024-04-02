from redis import Redis

# 命令行：
# 1) 对Key批量设置过期时间
#   redis-cli -h [ip] -p [port] -a [pwd] keys "key_*" | xargs -i redis-cli -h ip -p [port] -a [pwd] expire {}[秒]
#   eg:
#       redis-cli -h 127.0.0.1 -p 6379 -a pwd123 keys "celery-*" | xargs -i redis-cli -h 127.0.0.1 -p 6379 -a pwd123 expire {} 3600

# 2) 批量删除key
#   NUM: redis库的下标
#   redis-cli -h IP -p PORT -a PASSWORD [-n NUM] keys 'key*' | xargs redis-cli -h IP -p PORT -a PASSWORD [-n NUM] del
#   eg:
#       redis-cli -h 127.0.0.1 -p 6379 -a pwd123 keys "celery-task-*" | xargs redis-cli -h 127.0.0.1 -p 6379 -a pwd123 del
#       redis-cli KEYS "your_prefix*" | xargs redis-cli DEL


def batch_delete_keys():
    """ Production """
    # 适合生产删除，scan 不会阻塞服务，时间可能会长点; keys 大批量会阻塞
    r = Redis(host='localhost', port=6379, db=0)
    cursor = 0
    key_cnt = 0  # 统计 key 数量
    key_num = 1  # 默认有一个key

    while key_num > 0 or cursor != 0:
        _cursor, keys = r.scan(cursor, match='celery-task-*', count=1000)
        cursor = _cursor
        key_num = len(keys)
        key_cnt += key_num

        for i, key in enumerate(keys):
            print(f"key_cnt: {key_cnt}, i : {i} ==>>> cursor: {cursor}, key: {key}")
            r.delete(key)


if __name__ == "__main__":
    batch_delete_keys()



