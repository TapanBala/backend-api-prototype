import pymysql.cursors
from random import randint
from config import dbConfig, relationGeneratorConfig

def getCount():
    query = "SELECT id FROM wp_posts WHERE site = '{}' ORDER BY id DESC LIMIT 1".format(site)
    cursor.execute(query)
    relationGeneratorConfig['totalPosts'] = cursor.fetchone()[0]
    query = "SELECT id FROM wp_tags ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    relationGeneratorConfig['totalTags'] = cursor.fetchone()[0]

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
    for postId in range(startPostId, (endPostId + 1)):
        postTagCount = randint(3,5)
        totalRelations += postTagCount
        tagIdRange = relationGeneratorConfig['totalTags'] // postTagCount
        startRange = 1
        endRange = tagIdRange
        for tagCounter in range(postTagCount):
            tagId = randint(startRange, endRange)
            post2tagList.append("({}, {}, '{}')".format(postId, tagId, site))
            startRange = endRange + 1
            condition = endRange + (tagIdRange * 2)
            if (condition > relationGeneratorConfig['totalTags']):
                endRange = relationGeneratorConfig['totalTags']
            else:
                endRange += tagIdRange
    batchInsertRelations(post2tagList)
    print("Tag relations created for {} posts : {}".format(postId, totalRelations))

def createRelations():
    getCount()
    startPostId = 1
    endPostId = relationGeneratorConfig['totalPosts']
    if (relationGeneratorConfig['totalPosts'] < relationGeneratorConfig['batchSize']):
        insertRelations(startPostId, endPostId)
    else:
        batchCount = relationGeneratorConfig['totalPosts'] // relationGeneratorConfig['batchSize']
        remainderBatch = relationGeneratorConfig['totalPosts'] % relationGeneratorConfig['batchSize']
        endPostId = relationGeneratorConfig['batchSize']
        for count in range(batchCount):
            insertRelations(startPostId, endPostId)
            startPostId = endPostId + 1
            endPostId += relationGeneratorConfig['batchSize']
        if remainderBatch > 0:
            endPostId = startPostId + remainderBatch - 1
            insertRelations(startPostId, endPostId)
    print("Relations created for total of {} posts : {}".format(relationGeneratorConfig['totalPosts'], totalRelations))

def process(relationSite):
    global connection, cursor
    global totalRelations, site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    totalRelations = 0
    site = relationSite
    print("=====================================================")
    print("Generating Relations for ---> {}".format(site))
    createRelations()
    print("=====================================================")
    cursor.close()
    connection.close()