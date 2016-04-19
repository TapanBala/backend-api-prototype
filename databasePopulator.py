import pymysql.cursors
from random import randint
from faker import Faker

fake = Faker()

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

config = {
    'postsCount': 6000,
    'tagsCount' : 5540,
    'batchLimit': 500
}

postTypes = [
    'normal', 
    'ecommerce', 
    'slideshow', 
    'video', 
    'special', 
    'duplicate',  
    'branded_club', 
    'brand_article', 
    'longform', 
    'reposted_slideshow']

def createPostsQuery(text, ES, US, MX, CO, postType, url, special, published):

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
        ")  VALUES  ('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
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

    return query

def appendPostsQuery(dbQuery, text, ES, US, MX, CO, postType, url, special, published):

    query = dbQuery + (",('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
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
    return query

def createTagsQuery(tagName):
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  ('{}')"
        .format(tagName))

    return query

def appendTagsQuery(dbQuery, tagName):
    query = dbQuery + (",('{}')"
        .format(tagName))
    return query

connection = pymysql.connect(**dbConfig)

cursor = connection.cursor()

def populatePosts(postsCount):
    for numberOfPosts in range(postsCount):
        queryConfig = {
            'text'      : fake.text(max_nb_chars=100000),
            # 'text'      : 'xxyyzz',
            'published' : fake.date_time_between(start_date="-6y", end_date="now"),
            'ES'        : randint(0,1),
            'US'        : randint(0,1),
            'MX'        : randint(0,1),
            'CO'        : randint(0,1),
            'postType'  : postTypes[randint(0,9)],
            'url'       : fake.uri(),
            'special'   : randint(0,1)
        }

        if numberOfPosts == 0:
            dbQuery = createPostsQuery(**queryConfig)
        else:
            dbQuery = appendPostsQuery(dbQuery, **queryConfig)
    
    cursor.execute(dbQuery)
    connection.commit()
    print("Posts Query Execution completed for {} posts".format(postsCount))

def populateTags(tagsCount):
    for numberOfTags in range(tagsCount):
        queryConfig = {
            'tagName'   : fake.pystr(max_chars=20)
        }
        if numberOfTags == 0:
            dbQuery = createTagsQuery(**queryConfig)
        else:
            dbQuery = appendTagsQuery(dbQuery, **queryConfig)
    
    cursor.execute(dbQuery)
    connection.commit()
    print("Tags Query Execution completed for {} tags".format(tagsCount))

def dataGenerator():

    if (config['postsCount'] < config['batchLimit']) & (config['tagsCount'] < config['batchLimit']):
        populateTags(config['tagsCount'])
        populatePosts(config['postsCount'])
        
    else:
        postsQueryCount = config['postsCount']//config['batchLimit']
        postsQueryCountRem = config['postsCount']%config['batchLimit']
        tagsQueryCount = config['tagsCount']//config['batchLimit']
        tagsQueryCountRem = config['tagsCount']%config['batchLimit']

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

dataGenerator()
cursor.close()

connection.close()