import time
import sys
import cherrypy
import os
from cheroot import wsgi
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from app import create_app
from engine import *

conf = SparkConf().setAppName("movie_recommendation-server")
sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

movies_set_path = sys.argv[1] if len(sys.argv) > 1 else ""
ratings_set_path = sys.argv[2] if len(sys.argv) > 2 else ""

app = create_app(sc, movies_set_path, ratings_set_path)

cherrypy.tree.graft(app.wsgi_app, '/')
cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 5432,
    'engine.autoreload.on': False
})
cherrypy.engine.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5432)