#coding=utf-8
import parseUtil
import dbUtil

def parseFlowLine(line):
    items = line.strip().split(',')
    #log_time = str2timestamp(items[0])
    log_time = items[0]
    website = items[1]
    downSpd = items[3]
    upSpd = items[4]
    return (log_time, website, downSpd, upSpd)

def insertFlow(data):
    dbUtil.executeMany('insert into host_traffic (HostId, TrafficTime, Website, DownSpd, UpSpd) values (%s, %s, %s, %s, %s)', data)

def test():
    flowParser = parseUtil.Parser('flow', parseFlowLine, insertFlow)
    flowParser.run()

if __name__ == '__main__':
    test()
