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

totalRelations = 0

def getCounts():

    query = "SELECT id FROM wp_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    config['totalPosts'] = cursor.fetchone()[0]

    query = "SELECT COUNT(*) FROM wp_tags"
    cursor.execute(query)
    config['totalTags'] = cursor.fetchone()[0]

def createLinkQuery(postId, tagId):

    query = (
        "INSERT INTO `post2tag` ("
        "   `post_id`,"
        "   `tag_id`"
        ")  VALUES  ({}, {})"
        .format(postId, tagId))
    return query
    
def appendLinkQuery(dbQuery, postId, tagId):
    
    query = dbQuery + (",({}, {})"
        .format(postId, tagId))
    return query

def relationQueryBuilder(dbQuery, postId, tagId):

    if dbQuery == '':
        query = (
            "INSERT INTO `post2tag` ("
            "   `post_id`,"
            "   `tag_id`"
            ")  VALUES  ({}, {})"
            .format(postId, tagId))
        return query
    else:
        query = dbQuery + (",({}, {})"
            .format(postId, tagId))
        return query

def createTagRelations(startPostId, endPostId):
    query = ''
    global totalRelations
    for postId in range(startPostId, (endPostId+1)):
        postTagCount = randint(3,5)
        totalRelations = totalRelations + postTagCount
        tagIdRange = config['totalTags']//postTagCount
        startRange = 1
        endRange = tagIdRange
        
        for tagCounter in range(postTagCount):
            
            tagId = randint(startRange, endRange)
            query = relationQueryBuilder(query, postId, tagId)
            startRange = endRange + 1
            condition = endRange + (tagIdRange*2)
            if (condition > config['totalTags']):
                endRange = config['totalTags']
            
            else:
                endRange = endRange + tagIdRange

    cursor.execute(query)

    connection.commit()
    print("Tag relations created for {} posts : {}".format(postId, totalRelations))

def createPostRelations():

    getCounts()
    startPostId = 1
    endPostId = config['totalPosts']

    if (config['totalPosts'] < config['batchSize']):
        createTagRelations(startPostId, endPostId)

    else:
        batches = config['totalPosts']//config['batchSize']
        remainingPosts = config['totalPosts']%config['batchSize']
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

