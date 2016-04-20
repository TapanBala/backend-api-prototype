import pymysql.cursors

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

def createDatabase(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbConfig['db']))
    except Exception as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    connection = pymysql.connect(**dbConfig)
except Exception as err:
    print(err)
    connection = pymysql.connect(user = 'root')
    cursor = connection.cursor()
    createDatabase(cursor)
else:
    print("DATABASE `{}` exists".format(dbConfig['db']))