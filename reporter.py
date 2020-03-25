import os
import csv

KEY     = 'key'
REGEX   = 'regex'
POS     = 'positions'
P_ARK   = 'ark-pos'
P_VER   = 'ver-pos'
P_FILE  = 'file-pos'
P_OTHER = 'other-pos'
P_NA    = ''

class Reporter:

    def __init__(self):
        self.prefix = r''
        self.keys = []
        self.stats = {}
        self.csvrows = []

    def initKeys(self):
        for config in self.keys:
            key = config[KEY]
            self.stats[key] = 0

    def recordStat(self, stat, size = 0, ark = ""):
        self.stats[stat] += 1

    def csvName(self):
        return ""

    def writeCsv(self):
        name = self.csvName()
        if (name == ""):
            return
        with open(name, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for row in self.csvrows:
                writer.writerow(row)

    def report(self):
        files = []
        self.reportDir(files, self.getLogPath())
        for entry in files:
            self.reportFile(entry)

    def reportDir(self, list, dir):
        with os.scandir(dir) as entries:
            for entry in entries:
                if (entry.is_dir()):
                    self.reportDir(list, entry)
                else:
                    list.append(os.path.join(dir, entry.name))

    def reportFile(self, file):
        count = 0;

    def showResultHeader(self, header):
        print("")
        print(header)
        print("=====================")

    def showResults(self):
        print("")

    def getLogPath(self):
        return '/tmp/logs'
