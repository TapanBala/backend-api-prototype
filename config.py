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
    'postsCount': 60000,
    'batchSize' : 5000
}

tagsPopulatorConfig = {
    'tagsCount': 100000,
    'batchSize': 10000
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
    'batchSize': 5000
}

rankGeneratorConfig = {
    'batchSize': 5000
}

fakeText = ""

def process():
    global fakeText
    postsPopulatorConfig['postsCount'] //= siteConfig['siteCount'] 
    fakeText = fake.text(max_nb_chars = 1000000)

process()