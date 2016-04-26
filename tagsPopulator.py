import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, tagsPopulatorConfig

def batchInsertTags(tags):
    query = ",".join(tags)
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def insertTags():
    tags = []
    for numberOfTags in range(1, (tagsPopulatorConfig['tagsCount'] + 1)):
        tagName   = fake.pystr(max_chars = 20)
        tags.append("('{}')"
            .format(tagName))
        if (numberOfTags % tagsPopulatorConfig['batchSize']) == 0:
            batchInsertTags(tags)
            tags = []
            print("Tags created : {}".format(tagsPopulatorConfig['batchSize']))
    if tags != []:
        batchInsertTags(tags)
        print("Tags created : {}".format(numberOfTags % tagsPopulatorConfig['batchSize']))
    print("Tags Insertion completed for {} tags".format(numberOfTags))

def process():
    global connection, cursor, fake
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    print("=====================================================")
    print("Generating Tags data")
    insertTags()
    print("=====================================================")
    cursor.close()
    connection.close()