#!/usr/bin/python3

# pip3 install pyyaml
# import configparser
import os
import re
import json
import pprint
import csv
from storage import StorageReporter
from ui import UIReporter

class MyReporter:
    def __init__(self):
        self.reporters = [
            #StorageReporter(),
            UIReporter()
        ]

    def report(self):
        for rpt in self.reporters:
            rpt.report()

    def showResults(self):
        for rpt in self.reporters:
            rpt.showResults()
            rpt.writeCsv()

myReporter = MyReporter()
myReporter.report()
myReporter.showResults()
