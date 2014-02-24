#coding=utf-8
import dbUtil
import parseUtil

def parseStatusLine(line):
    items = line.strip().split(',')
    log_time = items[0]
    stat = 1 if items[2] == 'running' else 0
    cpuPer = items[3]
    memUsage = items[4]
    memPer = items[5]
    diskUsage = items[6]
    diskPer = items[7]
    ioRead = items[8]
    ioWrite = items[9]
    bandw = items[10]
    interface = items[11]
    portStat = items[12]
    return (log_time, stat, cpuPer, memUsage, memPer, 
        diskUsage, diskPer, ioRead, ioWrite, bandw, interface, portStat)
def insertStatus(data):
    dbUtil.executeMany("insert into host_state (HostId, StateTime, CurrStat,"\
    "CpuPercent, MemUsage, MemPercent, DiskUsage, DiskPercent, IORead, IOWrite,"\
    "Bandwidth, Interfaces, PortState) values "\
    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", data)

def test():
    statusParser = parseUtil.Parser('status', parseStatusLine, insertStatus)
    statusParser.run()

if __name__ == '__main__':
    test()
