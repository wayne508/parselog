#coding=utf-8
import MySQLdb
from DBUtils.PooledDB import PooledDB
import gl
import logging
import logging.config

logging.config.fileConfig("logging.conf")

dbpool = PooledDB(MySQLdb, 5, **gl.dbconfig)

def executeMany(query, args):
    global dbpool
    conn = dbpool.connection()
    cur = conn.cursor()
    cur.executemany(query, args)
    cur.close()
    conn.close()
    


def insert_status(data):
    executeMany("insert into host_state (StateTime, HostId, CurrStat,"\
    "CpuPercent, MemUsage, MemPercent, DiskUsage, DiskPercent, IORead, IOWrite,"\
    "Bandwidth, Interfaces, PortState) values "\
    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", data)

def test() :
    global dbpool
    conn = dbpool.connection()
    cur = conn.cursor()
    cur.execute('insert into host_traffic (HostId, TrafficTime, Website, DownSpd, UpSpd) values (%s, %s, %s, %s, %s)', ('0', '2014-02-19 11:24', 'baidu.com', '9', '5'))
    cur.close()
    conn.close()

if __name__ == '__main__':
    test()
