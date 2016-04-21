import pymysql.cursors
from random import randint
from config import dbConfig, relationGeneratorConfig as config

def getCounts():
    query = "SELECT id FROM wp_posts WHERE site = '{}' ORDER BY id DESC LIMIT 1".format(site)
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]
    query = "SELECT id FROM wp_tags ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalTags'] = cursor.fetchone()[0]

def batchInsertRelations(post2tagList):
    query = ",".join(post2tagList)
    query = (
        "INSERT INTO `post2tag` ("
        "   `post_id`,"
        "   `tag_id`,"
        "   `site`"
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def insertRelations(startPostId, endPostId):
    post2tagList = []
    global totalRelations
    for postId in range(startPostId, (endPostId+1)):
        postTagCount = randint(3,5)
        totalRelations = totalRelations + postTagCount
        tagIdRange = config['totalTags'] // postTagCount
        startRange = 1
        endRange = tagIdRange
        for tagCounter in range(postTagCount):
            tagId = randint(startRange, endRange)
            post2tagList.append("({}, {}, '{}')".format(postId, tagId, site))
            startRange = endRange + 1
            condition = endRange + (tagIdRange * 2)
            if (condition > config['totalTags']):
                endRange = config['totalTags']
            else:
                endRange = endRange + tagIdRange
    batchInsertRelations(post2tagList)
    print("Tag relations created for {} posts : {}".format(postId, totalRelations))

def createRelations():
    getCounts()
    startPostId = 1
    endPostId = config['totalPosts']
    if (config['totalPosts'] < config['batchSize']):
        insertRelations(startPostId, endPostId)
    else:
        batchCount = config['totalPosts'] // config['batchSize']
        remainderBatch = config['totalPosts'] % config['batchSize']
        endPostId = config['batchSize']
        for count in range(batchCount):
            insertRelations(startPostId, endPostId)
            startPostId = endPostId + 1
            endPostId = endPostId + config['batchSize']
        if remainderBatch > 0:
            endPostId = startPostId + remainderBatch - 1
            insertRelations(startPostId, endPostId)
    print("Relations created for total of {} posts : {}".format(config['totalPosts'], totalRelations))

def process(relationSite):
    global connection
    global cursor
    global totalRelations
    global site
    site = relationSite
    totalRelations = 0
    print("=====================================================")
    print("Generating Relations for ---> {}".format(site))
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    createRelations()
    print("=====================================================")
    cursor.close()
    connection.close()