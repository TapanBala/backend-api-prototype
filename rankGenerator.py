import pymysql.cursors
from random import randint
from config import dbConfig, rankGeneratorConfig as config

rankConfig = {
    'ES': 0,
    'US': 0,
    'MX': 0,
    'CO': 0
}

def getPostCount():
    query = "SELECT id FROM wp_posts WHERE site = '{}' ORDER BY id DESC LIMIT 1".format(site)
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]

def getPosts(limit, offset):
    query = ("SELECT id, ES, US, MX, CO FROM wp_posts WHERE site = '{}', id > {} ORDER BY id ASC LIMIT {}".format(site, offset, limit))
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def batchInsertRank(rankList):
    query = ",".join(rankList)
    query = (
        "INSERT INTO posts_queue ("
        "   `post_id`,"
        "   `country`,"
        "   `rank`,"
        "   `site`"
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def createCountryRankList(rankList, postId, country):
    rankConfig[country] += 1
    rankList.append("({}, '{}', {}, '{}')"
        .format(postId, country, rankConfig[country], site))
    return rankList

def createRankList(rankList, dbData):
    if dbData[1] == 1:
        rankList = createCountryRankList(rankList, dbData[0], 'ES')
    if dbData[2] == 1:
        rankList = createCountryRankList(rankList, dbData[0], 'US')
    if dbData[3] == 1:
        rankList = createCountryRankList(rankList, dbData[0], 'MX')
    if dbData[4] == 1:
        rankList = createCountryRankList(rankList, dbData[0], 'CO')
    return rankList

def insertRank(limit, offset):
    rankList = []
    dbData = getPosts(limit, offset)
    for data in range(limit):
        rankList = createRankList(rankList, dbData[data])
    batchInsertRank(rankList)

def rankGenerator():
    getPostCount()
    offset = 0
    limit = config['totalPosts']
    if (config['totalPosts'] < config['batchSize']):
        insertRank(limit, offset)
    else:
        batchCount = config['totalPosts'] // config['batchSize']
        remainderBatch = config['totalPosts'] % config['batchSize']
        limit = config['batchSize']
        for count in range(batchCount):
            insertRank(limit, offset)
            offset = (count + 1) * config['batchSize']
        if remainderBatch > 0:
            limit = remainderBatch
            insertRank(limit, offset)
    print("Total ES Ranks Generated : {}".format(rankConfig['ES']))
    print("Total US Ranks Generated : {}".format(rankConfig['US']))
    print("Total MX Ranks Generated : {}".format(rankConfig['MX']))
    print("Total CO Ranks Generated : {}".format(rankConfig['CO']))

def process(rankSite):
    global connection, cursor
    global site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    site = rankSite
    print("=====================================================")
    print("Generating ranks for ---> {}".format(site))
    rankGenerator()
    print("=====================================================")
    cursor.close()
    connection.close()