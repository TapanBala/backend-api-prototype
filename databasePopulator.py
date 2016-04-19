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
    'postsCount': 100,
    'tagsCount' : 100
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

    QUERY = (
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

    return QUERY

def appendPostsQuery(DBQuery, text, ES, US, MX, CO, postType, url, special, published):

    QUERY = DBQuery + (",('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
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
    return QUERY

def createTagsQuery(tagName):
    QUERY = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  ('{}')"
        .format(tagName))

    return QUERY

def appendTagsQuery(DBQuery, tagName):
    QUERY = DBQuery + (",('{}')"
        .format(tagName))
    return QUERY

connection = pymysql.connect(**dbConfig)

cursor = connection.cursor()

def populateDatabase(postsCount, tagsCount):
    for numberOfPosts in range(postsCount):
        queryConfig = {
            # 'text'      : fake.text(max_nb_chars=100000),
            'text'      : 'xxyyzz',
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
            DBQuery = createPostsQuery(**queryConfig)
        else:
            DBQuery = appendPostsQuery(DBQuery, **queryConfig)
    
        cursor.execute(DBQuery)
    connection.commit()

    print("Posts Query Executed")

    for numberOfTags in range(tagsCount):
        queryConfig = {
            'tagName'   : fake.pystr(max_chars=20)
        }
        if numberOfTags == 0:
            DBQuery = createTagsQuery(**queryConfig)
        else:
            DBQuery = appendTagsQuery(DBQuery, **queryConfig)

        cursor.execute(DBQuery)
    connection.commit()
    
    print("Tags Query Executed")

    print("Data Populated")

populateDatabase()

cursor.close()

connection.close()