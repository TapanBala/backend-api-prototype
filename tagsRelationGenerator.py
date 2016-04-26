import pymysql.cursors
from random import randint
from config import dbConfig, relationGeneratorConfig

def getCount():
    query = "SELECT id FROM wp_posts ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    relationGeneratorConfig['totalPosts'] = cursor.fetchone()[0]
    query = "SELECT id FROM wp_tags ORDER BY id DESC LIMIT 1"
    cursor.execute(query)
    relationGeneratorConfig['totalTags'] = cursor.fetchone()[0]

def batchInsertRelations(relations):
    query = ",".join(relations)
    query = (
        "INSERT INTO `post2tag` ("
        "   `post_id`,"
        "   `tag_id`"
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def insertRelations():
    relations = []
    totalRelations = 0
    batchRelations = 0
    for postId in range(1, (relationGeneratorConfig['totalPosts'] + 1)):
        postTagCount = randint(3, 5)
        batchRelations += postTagCount
        tagIdRange = relationGeneratorConfig['totalTags'] // postTagCount
        startRange = 1
        endRange = tagIdRange
        for tag in range(postTagCount):
            tagId = randint(startRange, endRange + 1)
            relations.append("({}, {})".format(postId, tagId))
            startRange = endRange + 1
            condition = endRange + (tagIdRange * 2)
            if (condition > relationGeneratorConfig['totalTags']):
                endRange = relationGeneratorConfig['totalTags']
            else:
                endRange += tagIdRange
        if(postId % relationGeneratorConfig['batchSize']) == 0:
            batchInsertRelations(relations)
            print("Tag relations generated : {}".format(batchRelations))
            relations = []
            totalRelations += batchRelations
            batchRelations = 0
    if relations != []:
        totalRelations += batchRelations
        batchInsertRelations(relations)
    print("Tag relations created for {} posts : {}".format(postId, totalRelations))

def process():
    global connection, cursor
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    getCount()
    print("=====================================================")
    print("Generating Post to Tags Relations")
    insertRelations()
    print("=====================================================")
    cursor.close()
    connection.close()