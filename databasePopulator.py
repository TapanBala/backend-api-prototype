import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypes, populatorConfig as config

fake = Faker()

def batchInsertTags(tagList):
    query = ",".join(tagList)
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def batchInsertPosts(postList):
    query = ",".join(postList)
    query = (
        "INSERT INTO `wp_posts` ("
        "   `text`,"
        "   `published`,"
        "   `ES`,"
        "   `US`,"
        "   `MX`,"
        "   `CO`,"
        "   `type`,"
        "   `url`,"
        "   `special`"
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def insertPosts(postsCount):
    postList = []
    for numberOfPosts in range(postsCount):
        text      = fake.text(max_nb_chars = 100000)
        # text      = 'xxyyzz'
        published = fake.date_time_between(start_date = "-6y", end_date = "now")
        ES        = randint(0,1)
        US        = randint(0,1)
        MX        = randint(0,1)
        CO        = randint(0,1)
        postType  = postTypes[randint(0,9)]
        url       = fake.uri()
        special   = randint(0,1)
        postList.append("('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
            .format(
                text, 
                published, 
                ES, 
                US, 
                MX, 
                CO, 
                postType, 
                url, 
                special))
    batchInsertPosts(postList)
    print("Posts Query Execution completed for {} posts".format(postsCount))

def insertTags(tagsCount):
    tagList = []
    for numberOfTags in range(tagsCount):
        tagName   = fake.pystr(max_chars = 20)
        tagList.append("('{}')"
            .format(tagName))
    batchInsertTags(tagList)
    print("Tags Query Execution completed for {} tags".format(tagsCount))

def dataGenerator():
    if (config['postsCount'] < config['batchSize']) & (config['tagsCount'] < config['batchSize']):
        insertTags(config['tagsCount'])
        insertPosts(config['postsCount'])
    else:
        postBatchCount = config['postsCount'] // config['batchSize']
        remainderPostBatch = config['postsCount'] % config['batchSize']
        tagBatchCount = config['tagsCount'] // config['batchSize']
        remainderTagBatch = config['tagsCount'] % config['batchSize']
        for count in range(postBatchCount):
            insertPosts(config['batchSize'])
        if remainderPostBatch > 0:
            insertPosts(remainderPostBatch)
        for count in range(tagBatchCount):
            insertTags(config['batchSize'])
        if remainderTagBatch > 0:
            insertTags(remainderTagBatch)
    print("Data Populated")
    print("Total posts inserted = {}".format(config['postsCount']))
    print("Total tags inserted = {}".format(config['tagsCount']))

connection = pymysql.connect(**dbConfig)
cursor = connection.cursor()
dataGenerator()
cursor.close()
connection.close()