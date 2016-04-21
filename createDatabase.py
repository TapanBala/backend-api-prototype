import pymysql.cursors
from config import dbConfig

def createDatabase(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbConfig['db']))
    except Exception as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def process():
    global connection
    global cursor
    try:
        connection = pymysql.connect(**dbConfig)
    except Exception as err:
        print(err)
        connection = pymysql.connect(user = dbConfig['root'], host = dbConfig['host'])
        cursor = connection.cursor()
        createDatabase(cursor)
    else:
        print("DATABASE `{}` exists".format(dbConfig['db']))

process()