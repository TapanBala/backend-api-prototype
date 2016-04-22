from faker import Faker
fake = Faker()
siteConfig = {
    'siteCount': 10
}

dbConfig = {
    'user': 'root',
    'host': 'localhost',
    'db'  : 'test'
}

postsPopulatorConfig = {
    'postsCount': 6000,
    'batchSize' : 500
}

tagsPopulatorConfig = {
    'tagsCount': 10000,
    'batchSize': 1000
}

postTypes = [
    'normal', 
    'ecommerce', 
    'slideshow', 
    'video', 
    'special', 
    'duplicate',  
    'branded_club', 
    'brand_article', 
    'longform', 
    'reposted_slideshow'
]

relationGeneratorConfig = {
    'batchSize': 500
}

rankGeneratorConfig = {
    'batchSize': 500
}

fakeText = ""

def process():
    global fakeText
    postsPopulatorConfig['postsCount'] //= siteConfig['siteCount'] 
    fakeText = fake.text(max_nb_chars = 1000000)

process()