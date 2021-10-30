import redis

redis_pool = redis.ConnectionPool(host='10.8.0.1', port=6379, db=0)


def makeredisconn():
    conn = redis.Redis(connection_pool=redis_pool)
    return conn


