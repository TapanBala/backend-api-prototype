import pymysql.cursors
from random import randint
from config import dbConfig, relationGeneratorConfig as config

totalRelations = 0

def getCounts():
    query = "SELECT id FROM wp_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]
    query = "SELECT id FROM wp_tags ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalTags'] = cursor.fetchone()[0]

def relationQueryBuilder(queryList):
    query = ",".join(queryList)
    query = (
        "INSERT INTO `post2tag` ("
        "   `post_id`,"
        "   `tag_id`"
        ")  VALUES  " + query)
    return query

def createTagRelations(startPostId, endPostId):
    queryList = []
    global totalRelations
    for postId in range(startPostId, (endPostId+1)):
        postTagCount = randint(3,5)
        totalRelations = totalRelations + postTagCount
        tagIdRange = config['totalTags'] // postTagCount
        startRange = 1
        endRange = tagIdRange
        for tagCounter in range(postTagCount):
            tagId = randint(startRange, endRange)
            queryList.append("({}, {})".format(postId, tagId))
            startRange = endRange + 1
            condition = endRange + (tagIdRange * 2)
            if (condition > config['totalTags']):
                endRange = config['totalTags']
            else:
                endRange = endRange + tagIdRange
    dbQuery = relationQueryBuilder(queryList)
    cursor.execute(dbQuery)
    connection.commit()
    print("Tag relations created for {} posts : {}".format(postId, totalRelations))

def createPostRelations():
    getCounts()
    startPostId = 1
    endPostId = config['totalPosts']
    if (config['totalPosts'] < config['batchSize']):
        createTagRelations(startPostId, endPostId)
    else:
        batches = config['totalPosts'] // config['batchSize']
        remainingPosts = config['totalPosts'] % config['batchSize']
        endPostId = config['batchSize']
        for count in range(batches):
            createTagRelations(startPostId, endPostId)
            startPostId = endPostId + 1
            endPostId = endPostId + config['batchSize']
        if remainingPosts > 0:
            endPostId = startPostId + remainingPosts - 1
            createTagRelations(startPostId, endPostId)
    print("Relations created for total of {} posts : {}".format(config['totalPosts'], totalRelations))

connection = pymysql.connect(**dbConfig)
cursor = connection.cursor()
createPostRelations()
cursor.close()
connection.close()