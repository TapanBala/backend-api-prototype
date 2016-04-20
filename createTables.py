
from __future__ import print_function

import pymysql.cursors

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

try:
    connection = pymysql.connect(**dbConfig)
except Exception as err:
    print(err)
else:
    print("Connection to database successful")

cursor = connection.cursor()

def createTables():

    TABLES = {}

    TABLES['wp_posts'] = (
        "CREATE TABLE   `wp_posts` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `text`      longtext NOT NULL,"
        "   `published` datetime NOT NULL,"
        "   `ES`        tinyint(1) NOT NULL,"
        "   `US`        tinyint(1) NOT NULL,"
        "   `MX`        tinyint(1) NOT NULL,"
        "   `CO`        tinyint(1) NOT NULL,"
        "   `type`      varchar(100) NOT NULL,"
        "   `url`       varchar(255) NOT NULL,"
        # " `site`      varchar(20) NOT NULL,"
        "   `special`   tinyint(1) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB")

    TABLES['post2tag'] = (
        "CREATE TABLE   `post2tag` ("
        "   `post_id`   int(11) NOT NULL,"
        "   `tag_id`    int(11) NOT NULL,"
        "   CONSTRAINT compk_post2tag PRIMARY KEY (post_id,tag_id)"
        ")  ENGINE = InnoDB")

    TABLES['wp_tags'] = (
        "CREATE TABLE   `wp_tags` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `name`      varchar(255) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB")

    TABLES['posts_queue'] = (
        "CREATE TABLE   `posts_queue` ("
        "   `post_id`   int(11) NOT NULL,"
        "   `country`   varchar(3) ,"
        "   `rank`      int(11) "
        ")  ENGINE = InnoDB")


    for name, dbq in TABLES.items():
        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(dbq)
        except Exception as err:
            print(err)
        else:
            print("OK")

createTables()

cursor.close()

connection.close()