#!/usr/bin/python

# pip3 install pyyaml
# import configparser
import os
import re
import sys
import argparse
import datetime
import json
import pprint
from dateutil.parser import parse as dateparse

class MyReporter:
    def __init__(self):
        self.root = "/Users/tbrady/work/logs/"
        self.uiroot = "/Users/tbrady/work/uilogs/"
        self.prefix = r'^\d+\.\d+\.\d+\.\d+ - - \[\d+\/(Jan|Feb|Mar|Apr|May|June|Jul|Aug|Sep|Oct|Nov|Dec)\/\d\d\d\d:\d\d:\d\d:\d\d .\d\d\d\d\] "([^\?]+)(\?.*)? HTTP/1.1" (\d+) (\d+) .*$'
        self.keys = [
            'post',
            'state',
            'file',
            'version',
            'object',
            'dryad-version',
            'produce-version',
            'manifest',
            'cloudcontainer',
            'total-records'
        ]
        self.regex = {
            'post': r'^POST .*',
            'state': r'^GET /state.*',
            'file': r'^GET /content/(\d+)/([^/]+)/(\d+)/(.*)$',
            'version': r'^GET /content/(\d+)/([^/]+)/(\d+)$',
            'object': r'^GET /content/(\d+)/([^/]+)$',
            'dryad-version': r'^GET /producer/3041/([^/]+)/(\d+)$',
            'produce-version': r'^GET /producer/(\d+)/([^/]+)/(\d+)$',
            'manifest': r'^GET /manifest/(\d+)/([^/]+)',
            'cloudcontainer': r'^GET /cloudcontainer.*'
        }

        self.arkpos = {
            #'file': 2,
            'version': 2,
            'object': 2,
            'dryad-version': 1,
            'produce-version': 2,
        }
        self.arks = {}

        self.stats = {}
        self.reqsize = {}
        for key in self.keys:
            self.stats[key] = 0
            self.reqsize[key] = []

    def report(self):
        files = []
        self.reportDir(files, self.root)
        for entry in files:
            self.reportFile(entry)
        files = []
        self.reportDir(files, self.uiroot)
        for entry in files:
            self.reportUiFile(entry)

    def reportDir(self, list, dir):
        with os.scandir(dir) as entries:
            for entry in entries:
                if (entry.is_dir()):
                    self.reportDir(list, entry)
                else:
                    list.append(os.path.join(dir, entry.name))

    def recordStat(self, stat, size = 0, ark = ""):
        self.stats[stat] += 1
        if size > 0:
            self.reqsize[stat].append(size)
        if (ark != ""):
            self.arks[ark] = self.arks[ark] + 1 if (ark in self.arks) else 0

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
                for key in self.keys:
                    if (key in self.regex):
                        if (re.match(self.regex[key], req)):
                            ark = ""
                            if (key in self.arkpos):
                                m = re.search(self.regex[key], req)
                                ark = m.group(self.arkpos[key])
                            self.recordStat(key, size, ark)
                            found = True
                            break
                if (found == False):
                    print(req)
        print("{} {}".format(file, count))

    def reportUiFile(self, file):
        count = 0;
        with open(file) as fp:
            for cnt, line in enumerate(fp):
                if re.match(r'^Started GET', line):
                    print(line)

    def showResults(self):
        print("{:>20s}: {:>10s} {:>20s}".format("Key", "Num Requests",
        "Average Size"))
        for k, v in sorted(self.arks.items(), key=lambda item: item[1], reverse=True):
            if (v < 20):
                break
            print("{} {:>5d}".format(k, v))

        for key in self.keys:
            avg = (sum(self.reqsize[key]) / len(self.reqsize[key])) if len(self.reqsize[key]) > 0 else 0
            print("{:>20s}: {:>10,d} {:>20,.1f}".format(key + " req", self.stats[key], avg))

myReporter = MyReporter()
myReporter.report()
myReporter.showResults()
