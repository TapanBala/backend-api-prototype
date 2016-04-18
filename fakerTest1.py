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
	'postsCount': 1,
	'tagsCount'	: 1
}


def createPostsQuery(text, ES, US, MX, CO, postType, url, special, published):

	QUERY = (
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

def createTagsQuery(tagName):
	QUERY = (
		"INSERT INTO `wp_tags` ("
		"	`name` "
		")	VALUES	('{}')"
		.format(tagName))

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

	DBQuery = {}
	DBQuery = createPostsQuery(**queryConfig)

	cursor.execute(DBQuery)
	connection.commit()
	print("Posts Query Executed")

for x in range(fakerConfig['tagsCount']):
	queryConfig = {
		'tagName'	: fake.pystr(max_chars=20)
	}

	DBQuery = {}
	DBQuery = createTagsQuery(**queryConfig)

	cursor.execute(DBQuery)
	connection.commit()
	print("Tags Query Executed")

print("Data Populated")



cursor.close()

connection.close()