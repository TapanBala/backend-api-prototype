import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypes, postsPopulatorConfig, fakeText

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
    fakeTextStart = randint(0,999000)
    fakeTextEnd   = fakeTextStart + 1000
    for numberOfPosts in range(postsCount):
        text      = fakeText[fakeTextStart:fakeTextEnd]
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

def generateData():
    if (postsPopulatorConfig['postsCount'] < postsPopulatorConfig['batchSize']):
        insertPosts(postsPopulatorConfig['postsCount'])
    else:
        postBatchCount = postsPopulatorConfig['postsCount'] // postsPopulatorConfig['batchSize']
        remainderPostBatch = postsPopulatorConfig['postsCount'] % postsPopulatorConfig['batchSize']
        for count in range(postBatchCount):
            insertPosts(postsPopulatorConfig['batchSize'])
        if remainderPostBatch > 0:
            insertPosts(remainderPostBatch)
    print("Total posts inserted = {}".format(postsPopulatorConfig['postsCount']))

def process(postSite):
    global connection, cursor, fake
    global postId, site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    postId = 1
    site = postSite
    print("=====================================================")
    print("Generating Post data for ---> {}".format(site))
    generateData()
    print("=====================================================")
    cursor.close()
    connection.close()