import pymysql.cursors
from config import dbConfig

def createTables():
    TABLES = {}
    TABLES['wp_posts'] = (
        "CREATE TABLE   `wp_posts` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `site`      varchar(100) NOT NULL,"
        "   `text`      longtext NOT NULL,"
        "   `published` timestamp NOT NULL,"
        # "   `published` int(11) NOT NULL,"
        "   `ES`        tinyint(1) NOT NULL,"
        "   `US`        tinyint(1) NOT NULL,"
        "   `MX`        tinyint(1) NOT NULL,"
        "   `CO`        tinyint(1) NOT NULL,"
        "   `type`      varchar(100) NOT NULL,"
        "   `url`       varchar(255) NOT NULL,"
        "   `special`   tinyint(1) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    TABLES['post2tag'] = (
        "CREATE TABLE   `post2tag` ("
        "   `post_id`   int(11) NOT NULL,"
        "   `tag_id`    int(11) NOT NULL,"
        "   PRIMARY KEY (`post_id`, `tag_id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    TABLES['wp_tags'] = (
        "CREATE TABLE   `wp_tags` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `name`      varchar(255) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    for name, dbq in TABLES.items():
        try:
            print("Creating table {}: ".format(name), end = '')
            cursor.execute(dbq)
        except Exception as err:
            print(err)
        else:
            print("OK")

def process():
    global connection, cursor
    try:
        connection = pymysql.connect(**dbConfig)
    except Exception as err:
        print("MySQL connection error : {}".format(err))
    else:
        print("Connection to database successful")
    cursor = connection.cursor()
    createTables()
    cursor.close()
    connection.close()