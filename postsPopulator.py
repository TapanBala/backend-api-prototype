import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypeChoice, postsPopulatorConfig, fakeText, countryChoice, specialChoice

def batchInsertPosts(posts):
    query = ",".join(posts)
    query = (
        "INSERT INTO `wp_posts` ("
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

def insertPosts():
    posts = []
    countriesWeighted = 0
    for numberOfPosts in range(postsPopulatorConfig['postsCount']):
        fakeTextStart = randint(0,997000)
        fakeTextEnd   = fakeTextStart + 3000
        text      = fakeText[fakeTextStart:fakeTextEnd]
        published = fake.date_time_between(start_date = "-6y", end_date = "now")
        countriesWeighted = countryChoice(countriesWeighted)
        ES        = countriesWeighted[0]
        US        = countriesWeighted[1]
        MX        = countriesWeighted[2]
        CO        = countriesWeighted[3]
        postType  = postTypeChoice()
        url       = fake.uri()
        special   = specialChoice()
        posts.append("('{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
            .format(
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
        if (numberOfPosts % postsPopulatorConfig['batchSize']) == 0:
            batchInsertPosts(posts)
            posts = []
            print("Posts created : {}".format(postsPopulatorConfig['batchSize']))
    if posts != []:
        batchInsertPosts(posts)
    print("Posts Insertion completed for {} posts".format(numberOfPosts + 1))

def process(postSite):
    global connection, cursor, fake
    global site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    site = postSite
    print("=====================================================")
    print("Generating Post data for ---> {}".format(site))
    insertPosts()
    print("=====================================================")
    cursor.close()
    connection.close()