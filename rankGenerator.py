import pymysql.cursors
from random import randint

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

config = {
    'batchSize': 500
}


ESRank = 0
USRank = 0
MXRank = 0
CORank = 0

def getCounts():

    query = "SELECT id FROM wp_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]

    query = "SELECT COUNT(*) FROM wp_tags"
    cursor.execute(query)
    config['totalTags'] = cursor.fetchone()[0]

def getDBData(limit, offset):
    query = ("SELECT id, ES, US, MX, CO FROM wp_posts ORDER BY id ASC LIMIT {} OFFSET {}".format(limit, offset))
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def ESQueryGenerator(dbQuery, postId, ES):
    global ESRank
    ESRank = ESRank + 1
    if dbQuery == '':
        query = (
            "INSERT INTO posts_queue ("
            "   post_id,"
            "   country,"
            "   rank    "
            ")  VALUES ({}, '{}', {})"
            .format(postId, 'ES', ESRank))
    else:
        query = dbQuery + (",({}, '{}', {})"
            .format(postId, 'ES', ESRank))
    return query

def USQueryGenerator(dbQuery, postId, US):
    global USRank
    USRank = USRank + 1
    if dbQuery == '':
        query = (
            "INSERT INTO posts_queue ("
            "   post_id,"
            "   country,"
            "   rank    "
            ")  VALUES ({}, '{}', {})"
            .format(postId, 'US', USRank))
    else:
        query = dbQuery + (",({}, '{}', {})"
            .format(postId, 'US', USRank))
    return query

def MXQueryGenerator(dbQuery, postId, MX):
    global MXRank
    MXRank = MXRank + 1
    if dbQuery == '':
        query = (
            "INSERT INTO posts_queue ("
            "   post_id,"
            "   country,"
            "   rank    "
            ")  VALUES ({}, '{}', {})"
            .format(postId, 'MX', MXRank))
    else:
        query = dbQuery + (",({}, '{}', {})"
            .format(postId, 'MX', MXRank))
    return query

def COQueryGenerator(dbQuery, postId, CO):
    global CORank
    CORank = CORank + 1
    if dbQuery == '':
        query = (
            "INSERT INTO posts_queue ("
            "   post_id,"
            "   country,"
            "   rank    "
            ")  VALUES ({}, '{}', {})"
            .format(postId, 'CO', CORank))
    else:
        query = dbQuery + (",({}, '{}', {})"
            .format(postId, 'CO', CORank))
    return query

def queryGenerator(dbQuery, dbData):


    if dbData[1] == 1:
        dbQuery = ESQueryGenerator(dbQuery, dbData[0], dbData[1])
    if dbData[2] == 1:
        dbQuery = USQueryGenerator(dbQuery, dbData[0], dbData[2])
    if dbData[3] == 1:
        dbQuery = MXQueryGenerator(dbQuery, dbData[0], dbData[3])
    if dbData[4] == 1:
        dbQuery = COQueryGenerator(dbQuery, dbData[0], dbData[4])
    return dbQuery

def rankGenerator(limit, offset):

    query = ''
    dbData = getDBData(limit, offset)

    for data in range(limit):
        query = queryGenerator(query, dbData[data])

    cursor.execute(query)
    connection.commit()

def process():

    getCounts()
    offset = 0
    limit = config['totalPosts']

    if (config['totalPosts'] < config['batchSize']):
        rankGenerator(limit, offset)

    else:
        batches = config['totalPosts']//config['batchSize']
        remainingPosts = config['totalPosts']%config['batchSize']
        limit = config['batchSize']

        
        for count in range(batches):
            rankGenerator(limit, offset)
            offset = (count + 1) * config['batchSize']
        if remainingPosts > 0:
            limit = remainingPosts
            rankGenerator(limit, offset)

    print("Total ES Ranks Generated : {}".format(ESRank))
    print("Total US Ranks Generated : {}".format(USRank))
    print("Total MX Ranks Generated : {}".format(MXRank))
    print("Total CO Ranks Generated : {}".format(CORank))

connection = pymysql.connect(**dbConfig)
cursor = connection.cursor()
process()
cursor.close()
connection.close()

