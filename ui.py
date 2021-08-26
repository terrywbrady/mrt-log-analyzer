from pathlib import Path
from pathlib import PosixPath
from reporter import *
import re
import os

class UIReporter(Reporter):

    def __init__(self):
        Reporter.__init__(self)
        self.uiprefix = r'^Started (GET|POST|PUT|DELETE) "(/[^\"]+)" for (\d+\.\d+\.\d+\.\d+) at (\d+-\d+-\d+ \d+:\d+:\d+ .\d+)$'
        self.uistats = {}
        self.uiregex = {
            'login': r'^/login$',
            'guest-login': r'^/guest_login$',
            'u-request': r'^/u/([^/]+)/\d+$',

            'view-object-page': r'^/m/([^/]+)$',
            'view-version-page': r'^/m/([^/]+)/\d+$',

            'presign-file': r'^/api/presign-file/.*$',
            'presign-version': r'^/api/assemble-obj/\d+.*$',
            'presign-object': r'^/api/assemble-obj/.*$',
            'presign-token': r'^/api/presign-obj-by-token/.*$',

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

    def getLogPath(self):
        path = os.environ.get('UILOG', '/tmp/ui-logs')
        print("UILOG={}".format(path))
        return path

    def recordStat(self, stat):
        self.uistats[stat] = self.uistats[stat] + 1 if (stat in self.uistats) else 1

    def reportFile(self, file):
        count = 0;
        with open(file) as fp:
            for cnt, line in enumerate(fp):
                line = line[89:]
                if re.match(self.uiprefix, line):
                    m = re.search(self.uiprefix, line)
                    req = m.group(2)
                    found = False
                    for key in self.uiregex:
                        if (re.match(self.uiregex[key], req)):
                            self.recordStat(key)
                            found = True
                            break
                    #if (found == False):
                    #    print(req)

    def showResults(self):
        self.showResultHeader("UI Requests")
        for key, v in sorted(self.uistats.items(), key=lambda item: item[1], reverse=True):
            print("{:>30s}: {:>10,d}".format(key, self.uistats[key]))
