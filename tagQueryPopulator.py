import pymysql.cursors
import config

def tagQueryPopulate():
    query = "insert tag_query1 (p_site, p_ES, p_US, p_MX, p_CO, p_published) select site, ES, US, MX, CO, published from wp_posts"
    cursor.execute(query)
    connection.commit()
    print("Table 1 Populated")
    query = "insert tag_query2 (p_site, p_ES, p_US, p_MX, p_CO, p_rank) select site, ES, US, MX, CO, rank from wp_posts"
    cursor.execute(query)
    connection.commit()
    print("Table 2 Populated")
    query = "insert tag_query3 (p_site, p_ES, p_US, p_MX, p_CO, p_rank, p_published, p_type) select site, ES, US, MX, CO, rank, published, type from wp_posts"
    cursor.execute(query)
    connection.commit() 
    print("Table 3 Populated")

def process():
    global connection, cursor
    connection = pymysql.connect(**config.dbConfig)
    cursor = connection.cursor()
    print("=====================================================")
    print("Populating Tag Query Table")
    tagQueryPopulate()
    print("=====================================================")
    cursor.close()
    connection.close()