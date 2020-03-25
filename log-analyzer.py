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
        self.uiprefix = r'^Started (GET|POST|PUT|DELETE) "(/[^\"]+)" for (\d+\.\d+\.\d+\.\d+) at (\d+-\d+-\d+ \d+:\d+:\d+ .\d+)$'
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

        self.uiregex = {
            'login': r'^/login$',
            'guest-login': r'^/guest_login$',
            'u-request': r'^/u/([^/]+)/\d+$',

            'view-object-page': r'^/m/([^/]+)$',
            'view-version-page': r'^/m/([^/]+)/\d+$',

            'download-file': r'^/d/([^/]+)/\d+/.*$',

            'download-object **': r'^/d/([^/]+)$',
            'download-version **': r'^/d/([^/]+)/\d+$',

            'async-request': r'^/async/([^/]+)/\d+$',
            'asyncd-request': r'^/asyncd/([^/]+)/\d+\?.*$',
            'lostorage-page': r'^/lostor.*$',

            'dm-request': r'^/dm/([^/]+)$',
            's-request': r'^/s/.*$',
            'collection-request': r'^/collection/.*$',
            'object-request': r'^/object/recent\.atom\?.*$',
            'object-ingest': r'^/object/ingest$',
            'object-update': r'^/object/update/$',
            'choose-collection': r'^/home/choose_collection$',
            'word-press-request': r'^/wp.*$',
            'misc-request': r'^/(https|css|stylesheets).*$',
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
        self.uistats = {}
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
            self.arks[ark] = self.arks[ark] + 1 if (ark in self.arks) else 1

    def recordUiStat(self, stat):
        self.uistats[stat] = self.uistats[stat] + 1 if (stat in self.uistats) else 1

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
        #print("{} {}".format(file, count))

    def reportUiFile(self, file):
        count = 0;
        with open(file) as fp:
            for cnt, line in enumerate(fp):
                if re.match(self.uiprefix, line):
                    m = re.search(self.uiprefix, line)
                    req = m.group(2)
                    found = False
                    for key in self.uiregex:
                        if (re.match(self.uiregex[key], req)):
                            self.recordUiStat(key)
                            found = True
                            break
                    #if (found == False):
                    #    print(req)

    def showResults(self):
        print("")
        print("Highly Requested Arks")
        print("=====================")

        print("{:>20s}: {:>10s} {:>20s}".format("Key", "Num Requests",
        "Average Size"))
        for k, v in sorted(self.arks.items(), key=lambda item: item[1], reverse=True):
            if (v < 20):
                break
            print("{} {:>5d}".format(k, v))

        print("")
        print("Storage Requests")
        print("================")

        for key, v in sorted(self.stats.items(), key=lambda item: item[1], reverse=True):
            avg = (sum(self.reqsize[key]) / len(self.reqsize[key])) if len(self.reqsize[key]) > 0 else 0
            print("{:>20s}: {:>10,d} {:>20,.1f}".format(key + " req", self.stats[key], avg))

        print("")
        print("UI Requests")
        print("================")
        for key, v in sorted(self.uistats.items(), key=lambda item: item[1], reverse=True):
            print("{:>30s}: {:>10,d}".format(key, self.uistats[key]))

myReporter = MyReporter()
myReporter.report()
myReporter.showResults()
