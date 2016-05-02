import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypeChoice, postsPopulatorConfig, fakeText, countryChoice, specialChoice
# from timer import Timestamp

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
    for numberOfPosts in range(1, (postsPopulatorConfig['postsCount'] + 1)):
        fakeTextStart = randint(0,997000)
        fakeTextEnd   = fakeTextStart + 3000
        text      = fakeText[fakeTextStart:fakeTextEnd] + fake.pystr(max_chars = 20)
        # published = timestamp.random()
        published = fake.date_time_between(start_date = "-6y", end_date = "now")
        countriesWeighted = countryChoice(countriesWeighted)
        ES        = countriesWeighted[0]
        US        = countriesWeighted[1]
        MX        = countriesWeighted[2]
        CO        = countriesWeighted[3]
        postType  = postTypeChoice()
        url       = fake.uri() + fake.pystr(max_chars = 10)
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
        print("Posts created : {}".format(numberOfPosts % postsPopulatorConfig['batchSize']))
    print("Posts Insertion completed for {} posts".format(numberOfPosts))

def process(postSite):
    global connection, cursor, fake#, timestamp
    global site
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    # timestamp = Timestamp()
    site = postSite
    print("=====================================================")
    print("Generating Post data for ---> {}".format(site))
    insertPosts()
    print("=====================================================")
    cursor.close()
    connection.close()