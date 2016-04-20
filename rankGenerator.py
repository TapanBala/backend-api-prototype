import pymysql.cursors
from random import randint
from config import dbConfig, rankGeneratorConfig as config

rankConfig = {
    'ES': 0,
    'US': 0,
    'MX': 0,
    'CO': 0
}

def getCounts():
    query = "SELECT id FROM wp_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]
    query = "SELECT id FROM wp_tags ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalTags'] = cursor.fetchone()[0]

def getPosts(limit, offset):
    query = ("SELECT id, ES, US, MX, CO FROM wp_posts ORDER BY id ASC LIMIT {} OFFSET {}".format(limit, offset))
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def rankQueryBuilder(queryList):
    query = ",".join(queryList)
    query = (
        "INSERT INTO posts_queue ("
        "   post_id,"
        "   country,"
        "   rank    "
        ")  VALUES  " + query)
    return query

def countryQueryListBuilder(queryList, postId, country):
    rankConfig[country] = rankConfig[country] + 1
    queryList.append("({}, '{}', {})"
        .format(postId, country, rankConfig[country]))
    return queryList

def queryListBuilder(queryList, dbData):
    if dbData[1] == 1:
        queryList = countryQueryListBuilder(queryList, dbData[0], 'ES')
    if dbData[2] == 1:
        queryList = countryQueryListBuilder(queryList, dbData[0], 'US')
    if dbData[3] == 1:
        queryList = countryQueryListBuilder(queryList, dbData[0], 'MX')
    if dbData[4] == 1:
        queryList = countryQueryListBuilder(queryList, dbData[0], 'CO')
    return queryList

def rankBuilder(limit, offset):
    queryList = []
    dbData = getPosts(limit, offset)
    for data in range(limit):
        queryList = queryListBuilder(queryList, dbData[data])
    dbQuery = rankQueryBuilder(queryList)
    cursor.execute(dbQuery)
    connection.commit()

def process():
    getCounts()
    offset = 0
    limit = config['totalPosts']
    if (config['totalPosts'] < config['batchSize']):
        rankBuilder(limit, offset)
    else:
        batches = config['totalPosts'] // config['batchSize']
        remainingPosts = config['totalPosts'] % config['batchSize']
        limit = config['batchSize']
        for count in range(batches):
            rankBuilder(limit, offset)
            offset = (count + 1) * config['batchSize']
        if remainingPosts > 0:
            limit = remainingPosts
            rankBuilder(limit, offset)
    print("Total ES Ranks Generated : {}".format(rankConfig['ES']))
    print("Total US Ranks Generated : {}".format(rankConfig['US']))
    print("Total MX Ranks Generated : {}".format(rankConfig['MX']))
    print("Total CO Ranks Generated : {}".format(rankConfig['CO']))

connection = pymysql.connect(**dbConfig)
cursor = connection.cursor()
process()
cursor.close()
connection.close()