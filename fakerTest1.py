import pymysql.cursors
from random import randint
from faker import Faker

fake = Faker()

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

fakerConfig = {
	'postsCount': 1
}


def createQuery(text, ES, US, MX, CO, postType, url, special, published):

	QUERY = {}
	QUERY['wp_posts'] = (
		"INSERT INTO 	`wp_posts` ("
		"	`text`,"
		"	`ES`,"
		"	`US`,"
		"	`MX`,"
		"	`CO`,"
		"	`type`,"
		"	`url`,"
		"	`special`,"
		"	`published`"
		")	VALUES	('{}', {}, {}, {}, {}, '{}', '{}', {}, {})"
		.format(text, ES, US, MX, CO, postType, url, special, published))

	return QUERY['wp_posts']



postTypes = ['normal', 
			 'ecommerce', 
			 'slideshow', 
			 'video', 
			 'special', 
			 'duplicate',  
			 'branded_club', 
			 'brand_article', 
			 'longform', 
			 'reposted_slideshow']

with open('static100k.txt', 'r') as myfile:
	postText=myfile.read()

connection = pymysql.connect(**dbConfig)

cursor = connection.cursor()

for x in range(fakerConfig['postsCount']):
	queryConfig = {
	'text'		: postText,
	'ES' 		: randint(0,1),
	'US' 		: randint(0,1),
	'MX' 		: randint(0,1),
	'CO' 		: randint(0,1),
	'postType' 	: postTypes[randint(0,9)],
	'url' 		: fake.uri(),
	'special' 	: randint(0,1),
	'published' : fake.date_time_between(start_date="-6y", end_date="now"),
	}

	QUERIES = {}
	QUERIES = createQuery(**queryConfig)

	cursor.execute(QUERIES)

	print("Query executed")



print("Data Populated")



cursor.close()

connection.close()