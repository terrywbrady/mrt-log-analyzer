from pathlib import Path
from pathlib import PosixPath
from reporter import *
import urllib.parse
import re

KEY     = 'key'
REGEX   = 'regex'
POS     = 'positions'
SHOW    = 'show'
P_ARK   = 'ark-pos'
P_VER   = 'ver-pos'
P_FILE  = 'file-pos'
P_OTHER = 'other-pos'
P_NA    = ''

class StorageReporter(Reporter):

    def __init__(self):
        Reporter.__init__(self)
        self.prefix = r'^\d+\.\d+\.\d+\.\d+ - - \[\d+\/(Jan|Feb|Mar|Apr|May|June|Jul|Aug|Sep|Oct|Nov|Dec)\/\d\d\d\d:\d\d:\d\d:\d\d .\d\d\d\d\] "([^\?]+)(\?.*)? HTTP/1.1" (\d+) (\d+) .*$'
        self.keys = [
            {
              KEY:   'post',
              SHOW:  False,
              REGEX: r'^POST .*',
              POS:   []
            },
            {
              KEY:   'state',
              SHOW:  False,
              REGEX: r'^GET /state.*',
              POS:   []
            },
            {
              KEY:   'system-file',
              SHOW:  True,
              REGEX: r'^GET /content/(\d+)/([^/]+)/(\d+)/(system.*)$',
              POS:   [P_NA, P_ARK, P_VER, P_FILE]
            },
            {
              KEY:   'producer-file',
              SHOW:  True,
              REGEX: r'^GET /content/(\d+)/([^/]+)/(\d+)/(producer.*)$',
              POS:   [P_NA, P_ARK, P_VER, P_FILE]
            },
            {
              KEY:   'presign-file',
              SHOW:  True,
              REGEX: r'^GET /presign-file/(\d+)/([^/]+)%7C(\d+)%7C(.*)$',
              POS:   [P_NA, P_ARK, P_VER, P_FILE]
            },
            {
              KEY:   'version',
              SHOW:  True,
              REGEX: r'^GET /content/(\d+)/([^/]+)/(\d+)$',
              POS:   [P_NA, P_ARK, P_VER]
            },
            {
              KEY:   'object',
              SHOW:  True,
              REGEX: r'^GET /content/(\d+)/([^/]+)$',
              POS:   [P_NA, P_ARK]
            },
            {
              KEY:   'delete-object',
              SHOW:  True,
              REGEX: r'^DELETE /content/(\d+)/([^/]+)$',
              POS:   [P_NA, P_ARK]
            },
            {
              KEY:   'dryad-version',
              SHOW:  True,
              REGEX: r'^GET /producer/3041/([^/]+)/(\d+)$',
              POS:   [P_ARK, P_VER]
            },
            {
              KEY:   'produce-version',
              SHOW:  True,
              REGEX: r'^GET /producer/(\d+)/([^/]+)/(\d+)$',
              POS:   [P_NA, P_ARK, P_VER]
            },
            {
              KEY:   'manifest',
              SHOW:  True,
              REGEX: r'^GET /manifest/(\d+)/([^/]+)$',
              POS:   [P_NA, P_ARK]
            },
            {
              KEY:   'cloudcontainer',
              SHOW:  True,
              REGEX: r'^GET /cloudcontainer/(.*)$',
              POS:   [P_OTHER]
            },
            {
              KEY:   'total-records',
              SHOW:  False,
              REGEX: r'',
              POS:   []
            }
        ]
        self.arks = {}
        self.stats = {}
        self.reqsize = {}
        self.initKeys()
        self.csvHeader(
          [
            'key',
            'ark',
            'version',
            'file',
            'other',
            'size'
          ]
        )

    def csvHeader(self, header):
        self.csvrows.append(header)

    def initKeys(self):
        for config in self.keys:
            key = config[KEY]
            self.stats[key] = 0
            self.reqsize[key] = []

    def recordStat(self, stat, size = 0, ark = ""):
        self.stats[stat] += 1
        if size > 0:
            self.reqsize[stat].append(size)
        if (ark != ""):
            self.arks[ark] = self.arks[ark] + 1 if (ark in self.arks) else 1

    def reportFile(self, file):
        count = 0;
        with open(file) as fp:
            for cnt, line in enumerate(fp):
                m = re.search(self.prefix, line)
                if (m == None):
                    continue
                req = m.group(2)
                status = m.group(4)
                size = int(m.group(5))
                count += 1

                type = "n/a"
                self.stats['total-records'] += 1
                found = False
                for config in self.keys:
                    found = self.processRegexConfig(config, req, size)
                    if (found):
                        break
                if (found == False):
                    print(req)
        #print("{} {}".format(file, count))

    def processRegexConfig(self, config, req, size):
        key = config[KEY]
        regex = config[REGEX]
        pos = config[POS]
        if (regex == ''):
            return False
        if (not(re.match(regex, req))):
            return False
        m = re.search(regex, req)
        row = [
            key,
            '',
            0,
            '',
            '',
            size
        ]

        for i, k in enumerate(pos):
            if (k == P_ARK):
                row[1] = urllib.parse.unquote(m[i+1])
            if (k == P_VER):
                row[2] = m[i+1]
            if (k == P_FILE):
                row[3] = urllib.parse.unquote(m[i+1])
            if (k == P_OTHER):
                row[4] = m[i+1]
        if (config[SHOW]):
            self.csvrows.append(row)

        self.stats[key] += 1
        if size > 0:
            self.reqsize[key].append(size)
        ark = row[1]
        if (ark != ""):
            self.arks[ark] = self.arks[ark] + 1 if (ark in self.arks) else 1

        return True

    def csvName(self):
        return "storage.csv"

    def showResults(self):
        self.showResultHeader("Highly Requested Arks")

        print("{:>30s}: {:>10s} ".format("Ark", "Num Requests"))
        for k, v in sorted(self.arks.items(), key=lambda item: item[1], reverse=True):
            if (v < 100):
                break
            print("{:>30s} {:>10d}".format(k, v))

        self.showResultHeader("Storage Requests")

        for key, v in sorted(self.stats.items(), key=lambda item: item[1], reverse=True):
            avg = (sum(self.reqsize[key]) / len(self.reqsize[key])) if len(self.reqsize[key]) > 0 else 0
            print("{:>20s}: {:>10,d} {:>20,.1f}".format(key + " req", self.stats[key], avg))

    def getLogPath(self):
        return str(PosixPath('~/work/logs/').expanduser())
