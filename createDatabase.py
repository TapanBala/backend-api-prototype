import pymysql.cursors
from config import dbConfig

def createDatabase(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbConfig['db']))
    except Exception as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    else:
        print("DATABASE `{}` created".format(dbConfig['db']))

def process():
    global connection, cursor
    try:
        connection = pymysql.connect(**dbConfig)
    except Exception as err:
        print(err)
        connection = pymysql.connect(user = dbConfig['user'], host = dbConfig['host'])
        cursor = connection.cursor()
        createDatabase(cursor)
    else:
        print("DATABASE `{}` exists".format(dbConfig['db']))