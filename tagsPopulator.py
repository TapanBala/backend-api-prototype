import pymysql.cursors
from random import randint
from faker import Faker
from config import dbConfig, postTypes, tagsPopulatorConfig

def batchInsertTags(tags):
    query = ",".join(tags)
    query = (
        "INSERT INTO `wp_tags` ("
        "   `name` "
        ")  VALUES  " + query)
    cursor.execute(query)
    connection.commit()

def insertTags(tagsCount):
    tags = []
    for numberOfTags in range(tagsCount):
        tagName   = fake.pystr(max_chars = 20)
        tags.append("('{}')"
            .format(tagName))
    batchInsertTags(tags)
    print("Tags Insertion completed for {} tags".format(tagsCount))

def generateData():
    if (tagsPopulatorConfig['tagsCount'] < tagsPopulatorConfig['batchSize']):
        insertTags(tagsPopulatorConfig['tagsCount'])
    else:
        tagBatchCount = tagsPopulatorConfig['tagsCount'] // tagsPopulatorConfig['batchSize']
        remainderTagBatch = tagsPopulatorConfig['tagsCount'] % tagsPopulatorConfig['batchSize']
        for count in range(tagBatchCount):
            insertTags(tagsPopulatorConfig['batchSize'])
        if remainderTagBatch > 0:
            insertTags(remainderTagBatch)
    print("Total tags inserted = {}".format(tagsPopulatorConfig['tagsCount']))

def process():
    global connection, cursor, fake
    connection = pymysql.connect(**dbConfig)
    cursor = connection.cursor()
    fake = Faker()
    print("=====================================================")
    print("Generating Tags data")
    generateData()
    print("=====================================================")
    cursor.close()
    connection.close()