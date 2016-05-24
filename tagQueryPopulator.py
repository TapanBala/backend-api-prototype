import pymysql.cursors
import config

def tagQueryPopulate():
    query = "INSERT tag_query1 (p_site, p_ES, p_US, p_MX, p_CO, p_published) SELECT site, ES, US, MX, CO, published FROM wp_posts ORDER BY id ASC"
    cursor.execute(query)
    connection.commit()
    print("Table 1 Populated")
    query = "INSERT tag_query2 (p_site, p_ES, p_US, p_MX, p_CO, p_rank) SELECT site, ES, US, MX, CO, rank FROM wp_posts ORDER BY id ASC"
    cursor.execute(query)
    connection.commit()
    print("Table 2 Populated")
    query = "INSERT tag_query3 (p_site, p_ES, p_US, p_MX, p_CO, p_rank, p_published, p_type) SELECT site, ES, US, MX, CO, rank, published, type FROM wp_posts ORDER BY id ASC"
    cursor.execute(query)
    connection.commit() 
    print("Table 3 Populated")
    query = "INSERT text (p_text) SELECT text FROM wp_posts ORDER BY id ASC"
    cursor.execute(query)
    connection.commit() 
    print("Table 4 Populated")

def process():
    global connection, cursor
    connection = pymysql.connect(**config.dbConfig)
    cursor = connection.cursor()
    print("=====================================================")
    print("Populating Tag Query Tables")
    tagQueryPopulate()
    print("=====================================================")
    cursor.close()
    connection.close()