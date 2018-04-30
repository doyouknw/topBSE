# -*- coding: utf-8 -*-
"""
Created on Sat Apr 26 18:55:10 2018

@author: anupam
"""


import requests
import csv
from zipfile import ZipFile
import datetime
import redis
from os.path import join, exists
import os

def getDateToDownload():
    # get date
    todays_date = datetime.datetime.now()
    yesterday_date = datetime.datetime.now() - datetime.timedelta(days=1)
    todays_csv_ready = todays_date.replace(hour=23, minute=59, second=59, microsecond=0)
    week_day = datetime.datetime.today().weekday()
    #monday
    if week_day == 0:
        if todays_date <= todays_csv_ready:
            date_of_download = datetime.datetime.now() - datetime.timedelta(days=3)
        else:
            date_of_download = todays_date

    #tusday to friday
    if week_day in range(1,5):
        if todays_date <= todays_csv_ready:
            date_of_download = yesterday_date
        else:
            date_of_download = todays_date

    #sat
    if week_day == 5:
             date_of_download = yesterday_date

    #sun
    if week_day == 6:
            date_of_download = datetime.datetime.now() - datetime.timedelta(days=2)

    return date_of_download

class Download:

    def __init__(self, out_dir, redis_db=None):
        #self.date = datetime.date.today()
        self.date = getDateToDownload()
        self.out_dir = out_dir
        if not exists(self.out_dir):
            os.mkdir(self.out_dir)
        
        for the_file in os.listdir(self.out_dir):
            file_path = os.path.join(self.out_dir, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                #elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                print(e)
        
        self.fileToBeDownloaded = "EQ" + \
            self.date.strftime('%d%m%y') + "_CSV.ZIP"
        self.fallBackFileToBeDownloaded = "EQ270418_CSV.ZIP"
        self.fileToExtract = self.fileToBeDownloaded.split('_')[0] + ".CSV"
        self.csvFile = join(self.out_dir, self.fileToExtract)
        self.redis_db = redis_db

    def storeData(self):
        if self.redis_db is not None:
            try:
                with open(self.csvFile) as f:
                    reader = csv.DictReader(f)

                    pipeline = self.redis_db.pipeline()
                    pipeline.multi()
                    pipeline.flushall()
                    for idx, row in enumerate(reader):
                        name = row['SC_NAME'].strip()
                        if idx < 10:
                            pipeline.zadd('top10', idx, name)

                        d = dict({'code': row['SC_CODE'], 'open': row['OPEN'], 'high': row[
                            'HIGH'], 'low': row['LOW'], 'close': row['CLOSE']})

                        pipeline.hmset(name, d)

                    pipeline.save()
                    pipeline.execute()
            except:
                print("Error occured in storing data")

    def getData(self):

        print("Fetching file for {0}....".format(self.date))
        r = requests.get(
            'http://www.bseindia.com/download/BhavCopy/Equity/' + self.fileToBeDownloaded)

        print("Response code:", r.status_code)
        if r.status_code == requests.codes.ok:
            print("Creating zip file: ", self.fileToBeDownloaded)

            with open(join(self.out_dir, self.fileToBeDownloaded), 'wb+') as f:
                f.write(r.content)
                ZipFile(f).extract(self.fileToExtract, self.out_dir)
            return True
        else:
            self.fileToBeDownloaded = self.fallBackFileToBeDownloaded
            self.fileToExtract = self.fileToBeDownloaded.split('_')[0] + ".CSV"
            self.csvFile = join(self.out_dir, self.fileToExtract)

            print(
                "Error in fetching file, using fallback file url for 24th Jan 2018")
            r = requests.get(
                'http://www.bseindia.com/download/BhavCopy/Equity/' + self.fileToBeDownloaded)
            if r.status_code == requests.codes.ok:
                print("Creating zip file: ", self.fileToBeDownloaded)

                with open(join(self.out_dir, self.fileToBeDownloaded), 'wb+') as f:
                    f.write(r.content)
                    ZipFile(f).extract(self.fileToExtract, self.out_dir)
                return True
            else:
                return False

if __name__ == "__main__":
    r = redis.StrictRedis().from_url(
        os.environ.get("REDIS_URL"), decode_responses=True)
    d = Downloader(
        out_dir="data", redis_db=r)
    if d.GetData():
        print("Fetched data successfully")
        d.StoreData()
    else:
        print("!! Error in fetching data !!")
