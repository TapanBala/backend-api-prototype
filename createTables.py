import pymysql.cursors
from config import dbConfig

def createTables():
    TABLES = {}
    TABLES['wp_posts'] = (
        "CREATE TABLE   `wp_posts` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `site`      varchar(20) NOT NULL,"
        "   `text`      longtext NOT NULL,"
        "   `published` timestamp NOT NULL,"
        "   `ES`        tinyint(1) NOT NULL,"
        "   `US`        tinyint(1) NOT NULL,"
        "   `MX`        tinyint(1) NOT NULL,"
        "   `CO`        tinyint(1) NOT NULL,"
        "   `type`      enum('normal', 'ecommerce', 'slideshow', 'video', 'duplicate', 'branded_club', 'brand_article', 'brand_article_video', 'longform', 'reposted_slideshow') NOT NULL DEFAULT 'normal',"
        "   `url`       varchar(255) NOT NULL,"
        "   `special`   tinyint(1) NOT NULL,"
        "   `rank`      int(11) NOT NULL,"
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
    TABLES['text'] = (
        "CREATE TABLE   `text` ("
        "   `id`        int(11) NOT NULL AUTO_INCREMENT,"
        "   `p_text`    longtext NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    TABLES['tag_query1'] = (
        "CREATE TABLE   `tag_query1` ("
        "   `text_id`   int(11) NOT NULL AUTO_INCREMENT,"
        "   `p_site`      varchar(20) NOT NULL,"
        "   `p_ES`        tinyint(1) NOT NULL,"
        "   `p_US`        tinyint(1) NOT NULL,"
        "   `p_MX`        tinyint(1) NOT NULL,"
        "   `p_CO`        tinyint(1) NOT NULL,"
        "   `p_published` timestamp NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    TABLES['tag_query2'] = (
        "CREATE TABLE   `tag_query2` ("
        "   `text_id`     int(11) NOT NULL AUTO_INCREMENT,"
        "   `p_site`      varchar(20) NOT NULL,"
        "   `p_ES`        tinyint(1) NOT NULL,"
        "   `p_US`        tinyint(1) NOT NULL,"
        "   `p_MX`        tinyint(1) NOT NULL,"
        "   `p_CO`        tinyint(1) NOT NULL,"
        "   `p_rank`      int(11) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ")  ENGINE = InnoDB DEFAULT CHARSET=latin1")
    TABLES['tag_query3'] = (
        "CREATE TABLE   `tag_query3` ("
        "   `text_id`   int(11) NOT NULL AUTO_INCREMENT,"
        "   `p_site`      varchar(20) NOT NULL,"
        "   `p_ES`        tinyint(1) NOT NULL,"
        "   `p_US`        tinyint(1) NOT NULL,"
        "   `p_MX`        tinyint(1) NOT NULL,"
        "   `p_CO`        tinyint(1) NOT NULL,"
        "   `p_rank`      int(11) NOT NULL,"
        "   `p_published` timestamp NOT NULL,"
        "   `p_type`      enum('normal', 'ecommerce', 'slideshow', 'video', 'duplicate', 'branded_club', 'brand_article', 'brand_article_video', 'longform', 'reposted_slideshow') NOT NULL DEFAULT 'normal',"
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