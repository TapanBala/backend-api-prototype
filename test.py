import pymysql.cursors

config = {
    'user': 'root',
    'host': 'localhost'
    # 'db'  : 'test'
}
connection = pymysql.connect(**config)
try:
    connection.db = 'test'
except Exception as e:
    print('ERROR:{}'.format(e))
# connection = pymysql.connect(**config)

# connection.close()

