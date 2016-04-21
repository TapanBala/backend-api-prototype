import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypes, populatorConfig

def batchInsertTags(tags):
    query = ",".join(tags)
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def batchInsertPosts(posts):
    query = ",".join(posts)
    query = (
        "INSERT INTO `wp_posts` ("
        "   `id`,"
        "   `site`,"
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
    global postId
    posts = []
    for numberOfPosts in range(postsCount):
        text      = fake.text(max_nb_chars = 100000)
        published = fake.date_time_between(start_date = "-6y", end_date = "now")
        ES        = randint(0,1)
        US        = randint(0,1)
        MX        = randint(0,1)
        CO        = randint(0,1)
        postType  = postTypes[randint(0,9)]
        url       = fake.uri()
        special   = randint(0,1)
        posts.append("({}, '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
            .format(
                postId,
                site,
                text, 
                published, 
                ES, 
                US, 
                MX, 
                CO, 
                postType, 
                url, 
                special))
        postId = postId + 1
    batchInsertPosts(posts)
    print("Posts Insertion completed for {} posts".format(postsCount))

def insertTags(tagsCount):
    tags = []
    for numberOfTags in range(tagsCount):
        tagName   = fake.pystr(max_chars = 20)
        tags.append("('{}')"
            .format(tagName))
    batchInsertTags(tags)
    print("Tags Insertion completed for {} tags".format(tagsCount))

def generateData():
    if (populatorConfig['postsCount'] < populatorConfig['batchSize']) & (populatorConfig['tagsCount'] < populatorConfig['batchSize']):
        insertPosts(populatorConfig['postsCount'])
        insertTags(populatorConfig['tagsCount'])
    else:
        postBatchCount = populatorConfig['postsCount'] // populatorConfig['batchSize']
        remainderPostBatch = populatorConfig['postsCount'] % populatorConfig['batchSize']
        tagBatchCount = populatorConfig['tagsCount'] // populatorConfig['batchSize']
        remainderTagBatch = populatorConfig['tagsCount'] % populatorConfig['batchSize']
        for count in range(postBatchCount):
            insertPosts(populatorConfig['batchSize'])
        if remainderPostBatch > 0:
            insertPosts(remainderPostBatch)
        for count in range(tagBatchCount):
            insertTags(populatorConfig['batchSize'])
        if remainderTagBatch > 0:
            insertTags(remainderTagBatch)
    print("Total posts inserted = {}".format(populatorConfig['postsCount']))
    print("Total tags inserted = {}".format(populatorConfig['tagsCount']))

def process(postSite):
    global connection, cursor, fake
    global postId, site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    postId = 1
    site = postSite
    print("=====================================================")
    print("Generating data for ---> {}".format(site))
    generateData()
    print("=====================================================")
    cursor.close()
    connection.close()