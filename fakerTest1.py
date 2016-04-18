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
		"	`published`,"
		"	`ES`,"
		"	`US`,"
		"	`MX`,"
		"	`CO`,"
		"	`type`,"
		"	`url`,"
		"	`special`"
		")	VALUES	('{}', '{}', {}, {}, {}, {}, '{}', '{}', {})"
		.format(text, published, ES, US, MX, CO, postType, url, special))

	return QUERY



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
	'text'		: fake.text(max_nb_chars=100000),
	'published' : fake.date_time_between(start_date="-6y", end_date="now"),
	'ES' 		: randint(0,1),
	'US' 		: randint(0,1),
	'MX' 		: randint(0,1),
	'CO' 		: randint(0,1),
	'postType' 	: postTypes[randint(0,9)],
	'url' 		: fake.uri(),
	'special' 	: randint(0,1)
	}

	QUERIES = {}
	QUERIES = createQuery(**queryConfig)

	# print(QUERIES['wp_posts'])

	cursor.execute(QUERIES['wp_posts'])

	print("Query executed")

	connection.commit()

print("Data Populated")



cursor.close()

connection.close()