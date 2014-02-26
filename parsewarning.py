#coding=utf-8
import dbUtil
import parseUtil

watchType = {
      "CPU" : 1,
      "MEMERY": 2,
      "DISK": 3
    }

def parseWarningLine(line):
    items = line.strip().split(',')
    log_time = items[0]
    typeId = watchType[items[1]];
    threshold = items[2];
    duration = items[3];
    return (log_time, typeId, threshold, duration)

def insertWarning(data):
    dbUtil.executeMany("insert into warn_info (HostId, WarnTime, WatchTypeId,"\
    "Threshold, Duration) values "\
    "(%s,%s,%s,%s,%s)", data)

def test():
    statusParser = parseUtil.Parser('warning', parseWarningLine, insertWarning)
    statusParser.run()

if __name__ == '__main__':
    test()
