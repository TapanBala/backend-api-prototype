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
    'batchSize' : 150
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
    'batchSize': 500
}

rankGeneratorConfig = {
    'batchSize': 500
}

def process():
    postsPopulatorConfig['postsCount'] //= siteConfig['siteCount'] 

process()