import pymysql.cursors
from random import randint
from config import dbConfig, rankGeneratorConfig

def getPostCount():
    query = "SELECT id FROM wp_posts WHERE site = '{}' ORDER BY id DESC LIMIT 1".format(site)
    cursor.execute(query)
    rankGeneratorConfig['totalPosts'] = cursor.fetchone()[0]

def getPosts(limit, offset):
    query = ("SELECT id, ES, US, MX, CO FROM wp_posts WHERE site = '{}' AND id > {} ORDER BY id ASC LIMIT {}".format(site, offset, limit))
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def batchInsertRank(ranks):
    query = ",".join(ranks)
    query = (
        "INSERT INTO posts_queue ("
        "   `post_id`,"
        "   `country`,"
        "   `rank`,"
        "   `site`"
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def createCountryRankList(ranks, postId, country):
    rankConfig[country] += 1
    ranks.append("({}, '{}', {}, '{}')"
        .format(postId, country, rankConfig[country], site))
    return ranks

def createRankList(ranks, dbData):
    if dbData[1] == 1:
        ranks = createCountryRankList(ranks, dbData[0], 'ES')
    if dbData[2] == 1:
        ranks = createCountryRankList(ranks, dbData[0], 'US')
    if dbData[3] == 1:
        ranks = createCountryRankList(ranks, dbData[0], 'MX')
    if dbData[4] == 1:
        ranks = createCountryRankList(ranks, dbData[0], 'CO')
    return ranks

def insertRank(limit, offset):
    ranks = []
    dbData = getPosts(limit, offset)
    for data in range(limit):
        ranks = createRankList(ranks, dbData[data])
    batchInsertRank(ranks)

def rankGenerator():
    print(rankConfig)
    getPostCount()
    offset = 0
    limit = rankGeneratorConfig['totalPosts']
    if (rankGeneratorConfig['totalPosts'] < rankGeneratorConfig['batchSize']):
        insertRank(limit, offset)
    else:
        batchCount = rankGeneratorConfig['totalPosts'] // rankGeneratorConfig['batchSize']
        remainderBatch = rankGeneratorConfig['totalPosts'] % rankGeneratorConfig['batchSize']
        limit = rankGeneratorConfig['batchSize']
        for count in range(batchCount):
            insertRank(limit, offset)
            offset = (count + 1) * rankGeneratorConfig['batchSize']
        if remainderBatch > 0:
            limit = remainderBatch
            insertRank(limit, offset)
    print("ES Ranks Generated : {}".format(rankConfig['ES']))
    print("US Ranks Generated : {}".format(rankConfig['US']))
    print("MX Ranks Generated : {}".format(rankConfig['MX']))
    print("CO Ranks Generated : {}".format(rankConfig['CO']))

def process(rankSite):
    global connection, cursor
    global site, rankConfig
    rankConfig = {
        'ES': 0,
        'US': 0,
        'MX': 0,
        'CO': 0
    }
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    site = rankSite
    print("=====================================================")
    print("Generating ranks for ---> {}".format(site))
    rankGenerator()
    print("=====================================================")
    cursor.close()
    connection.close()