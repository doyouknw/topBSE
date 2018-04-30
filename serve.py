# -*- coding: utf-8 -*-
"""
Created on Sat Apr 27 18:55:10 2018

@author: anupam
"""

import cherrypy
from datetime import datetime
import redis
import os
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from jinja2 import Environment, FileSystemLoader
from main import Download

scheduler = BackgroundScheduler()
scheduler.start()
#redis_db = redis.StrictRedis().from_url(os.environ.get("REDIS_URL"), decode_responses=True)
redis_db = redis.StrictRedis(host="localhost", port=6379,charset="utf-8", decode_responses=True, db=1)
env = Environment(autoescape=True, loader=FileSystemLoader('templates'))


class StockData(object):

    @staticmethod
    @scheduler.scheduled_job(trigger=CronTrigger(year='*', month='*',
                                                 day='*', week='*', hour=23, minute=19, second=0, start_date=datetime.now()))
    def updateDB():
        print("=" * 50 + "\nUpdating\n" + "=" * 50)
        dl = Download(out_dir="data", redis_db=redis_db)
        if dl.getData():
            dl.storeData()
        else:
            print("Error in updating DB")

    def __init__(self):
        self.updateDB()

    @classmethod
    @cherrypy.expose
    def index(self):

        possible_keys = redis_db.zrange("top10", 0, -1)
        pipeline = redis_db.pipeline()
        pipeline.multi()
        for key in possible_keys:
            pipeline.hgetall(key)
        ans = pipeline.execute()

        for idx, each_dict in enumerate(ans):
            each_dict['name'] = possible_keys[idx]

        tmpl = env.get_template('index.html')
        return tmpl.render(data=ans)

    @classmethod
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def searchResponse(self, name):
        name = name.upper()
        possible_keys = redis_db.keys(pattern='*' + name + '*')

        pipeline = redis_db.pipeline()
        pipeline.multi()
        for key in possible_keys:
            pipeline.hgetall(key)
        ans = pipeline.execute()
        print(type(ans))
        for idx, each_dict in enumerate(ans):
            each_dict['name'] = possible_keys[idx]

        return ans
        # return dict(zip(possible_keys, ans))


config = {
    '/': {
        "tools.staticdir.root": os.path.abspath(os.path.dirname(__file__))
    },
    '/assets': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': "assets"
    }
}
cherrypy.quickstart(StockData(), '/', config=config)
