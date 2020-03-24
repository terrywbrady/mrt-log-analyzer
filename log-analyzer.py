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
        self.prefix = r'^\d+\.\d+\.\d+\.\d+ - - \[\d+\/(Jan|Feb|Mar|Apr|May|June|Jul|Aug|Sep|Oct|Nov|Dec)\/\d\d\d\d:\d\d:\d\d:\d\d .\d\d\d\d\] "([^\?]+)(\?.*)? HTTP/1.1" (\d+) (\d+) .*$'
        self.keys = [
            'post',
            'state',
            'file',
            'version',
            'object',
            'produce-version',
            'dryad-version',
            'manifest',
            'cloudcontainer',
            'total-records'
        ]
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

    def reportDir(self, list, dir):
        with os.scandir(dir) as entries:
            for entry in entries:
                if (entry.is_dir()):
                    self.reportDir(list, entry)
                else:
                    list.append(os.path.join(dir, entry.name))

    def recordStat(self, stat, size = 0):
        self.stats[stat] += 1
        if size > 0:
            self.reqsize[stat].append(size)

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
                if (re.match(r'^POST .*', req)):
                    self.recordStat('post', size)
                elif (re.match(r'^GET /state.*', req)):
                    self.recordStat('state', size)
                elif (re.match(r'^GET /content/(\d+)/([^/]+)/(\d+)/(.*)$', req)):
                    self.recordStat('file', size)
                elif (re.match(r'^GET /content/(\d+)/([^/]+)/(\d+)$', req)):
                    self.recordStat('version', size)
                elif (re.match(r'^GET /content/(\d+)/([^/]+)$', req)):
                    self.recordStat('object', size)
                elif (re.match(r'^GET /producer/3041/([^/]+)/(\d+)$', req)):
                    self.recordStat('dryad-version', size)
                elif (re.match(r'^GET /producer/(\d+)/([^/]+)/(\d+)$', req)):
                    self.recordStat('produce-version', size)
                elif (re.match(r'^GET /manifest.*', req)):
                    self.recordStat('manifest', size)
                elif (re.match(r'^GET /cloudcontainer.*', req)):
                    self.recordStat('cloudcontainer', size)
                else:
                    print(req)
                #print("{}".format(type))
        print("{} {}".format(file, count))

    def showResults(self):
        print("{:>20s}: {:>10s} {:>20s}".format("Key", "Num Requests",
        "Average Size"))
        for key in self.keys:
            avg = (sum(self.reqsize[key]) / len(self.reqsize[key])) if len(self.reqsize[key]) > 0 else 0
            print("{:>20s}: {:>10,d} {:>20,.1f}".format(key + " req", self.stats[key], avg))

myReporter = MyReporter()
myReporter.report()
myReporter.showResults()
