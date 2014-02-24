#coding=utf-8

import time
import os
import shutil
import gl
import logging
import logging.config

logging.config.fileConfig("logging.conf")

class LogParser():
    def __init__(self, parseLineFunc, insertFunc):
        self.buf = []
        assert(callable(parseLineFunc) and callable(insertFunc))
        self.parseLineFunc = parseLineFunc
        self.insertFunc = insertFunc

    def parseFile(self, logfile, ):
        fullname = os.path.join(gl.log_path, logfile)
        hostId = os.path.basename(fullname).split('_', 1)[0]
        f = None
        try:
            f = open(fullname)
            for line in f:
                try:
                    e = self.parseLineFunc(line)
                    self.buf.append((hostId,) + e)
                    if len(self.buf) > gl.max_cache:
                        self.insertFunc(self.buf)
                        self.buf = []
                except Exception, e:
                    print e
        except IOError, e:
            print 'Error %s' % str(e)
        finally:
            if f:
                f.close()

    def forceSave(self):
        if self.buf:
            self.insertFunc(self.buf)
            self.buf = []

class LogScanner():
    def __init__(self, dir, logtype):
        self.__dir = dir
        self.innerDirs = None
        self.__type = logtype
        self.logger = logging.getLogger(__name__)

    def __getDirs(self):
        files = os.listdir(gl.log_path)
        self.innerDirs = [dir for dir in files 
            if os.path.isdir(os.path.join(gl.log_path, dir)) 
            and isValidDate(dir)]
        print("Found dirs: \n\t%s" % str(self.innerDirs))

    def yieldLogs(self):
        self.__getDirs()
        tmp = []
        for dir in self.innerDirs:
            for log in os.listdir(os.path.join(gl.log_path, dir)):
                items = log.split('_', 2)
                if len(items) != 3:
                    continue
                if items[1] == self.__type:
                    tmp.append((dir, log))
                    if len(tmp) > 20:
                        yield tmp
                        tmp = []
        if tmp:
            yield tmp
        else:
            self.__deleteOldFolder()
            yield []

    def __deleteOldFolder(self):
        now = int(time.time())
        #东八区
        today = now - now % (3600 * 24) - 8 * 3600
        for dir in self.innerDirs:
            try:
                datestamp = int(time.mktime(time.strptime(dir, "%Y%m%d")))
            except:
                continue
            if today - datestamp > 3600 * 24 * 2:
                print("%d, %d", today, datestamp)
                try:
                    os.rmdir(os.path.join(gl.log_path, dir))
                except OSError, e:
                    print(e)


def isValidDate(timestr, format = "%Y%m%d"):
    try:
        time.strptime(timestr, format)
        return True
    except Exception, e:
        return False

class Parser:
    def __init__(self, logtype, parseLineFunc, insertFunc):
        self.scanner = LogScanner(gl.log_path, logtype)
        self.parser = LogParser(parseLineFunc, insertFunc)

    def run(self):
        seconds = 2
        last = time.time()
        while True:
            now = time.time()
            print("wait for %d" % seconds)
            if now - last < seconds:
                time.sleep(2)
                continue
            else:
                last = now
            for logs in self.scanner.yieldLogs():
                if not logs:
                    seconds *= 2
                    self.parser.forceSave()
                else:
                    seconds = 2
                if seconds > gl.max_interval:
                    seconds = gl.max_interval
                for (dir, log) in logs:
                    print("Parse file: %s" % log)
                    self.parser.parseFile(os.path.join(dir, log))
                    newdir = os.path.join(gl.new_path, dir)
                    if not os.path.isdir(newdir):
                        os.mkdir(newdir)
                    shutil.move(os.path.join(gl.log_path, dir, log), newdir)

if __name__ == '__main__':
    #scanPath()
    main()
    #parse_flow_log(os.path.join(log_path, '20140217/0_flow_2014-02-17_16_46'))
