import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypes, populatorConfig as config

fake = Faker()

def tagsQueryBuilder(queryList):
    query = ",".join(queryList)
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  " + query)
    return query

def postsQueryBuilder(queryList):
    query = ",".join(queryList)
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
    return query

def populatePosts(postsCount):
    queryList = []
    for numberOfPosts in range(postsCount):
        # text      = fake.text(max_nb_chars = 100000)
        text      = 'xxyyzz'
        published = fake.date_time_between(start_date = "-6y", end_date = "now")
        ES        = randint(0,1)
        US        = randint(0,1)
        MX        = randint(0,1)
        CO        = randint(0,1)
        postType  = postTypes[randint(0,9)]
        url       = fake.uri()
        special   = randint(0,1)
        queryList.append("('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
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
    dbQuery = postsQueryBuilder(queryList)
    cursor.execute(dbQuery)
    connection.commit()
    print("Posts Query Execution completed for {} posts".format(postsCount))

def populateTags(tagsCount):
    queryList = []
    for numberOfTags in range(tagsCount):
        tagName   = fake.pystr(max_chars = 20)
        queryList.append("('{}')"
            .format(tagName))
    dbQuery = tagsQueryBuilder(queryList)
    cursor.execute(dbQuery)
    connection.commit()
    print("Tags Query Execution completed for {} tags".format(tagsCount))

def dataGenerator():
    if (config['postsCount'] < config['batchLimit']) & (config['tagsCount'] < config['batchLimit']):
        populateTags(config['tagsCount'])
        populatePosts(config['postsCount'])
    else:
        postsQueryCount = config['postsCount'] // config['batchLimit']
        postsQueryCountRem = config['postsCount'] % config['batchLimit']
        tagsQueryCount = config['tagsCount'] // config['batchLimit']
        tagsQueryCountRem = config['tagsCount']% config['batchLimit']
        for count in range(postsQueryCount):
            populatePosts(config['batchLimit'])
        if postsQueryCountRem > 0:
            populatePosts(postsQueryCountRem)
        for count in range(tagsQueryCount):
            populateTags(config['batchLimit'])
        if tagsQueryCountRem > 0:
            populateTags(tagsQueryCountRem)
    print("Data Populated")
    print("Total posts inserted = {}".format(config['postsCount']))
    print("Total tags inserted = {}".format(config['tagsCount']))

connection = pymysql.connect(**dbConfig)
cursor = connection.cursor()
dataGenerator()
cursor.close()
connection.close()