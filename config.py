from faker import Faker
import random
import numpy as np
from bisect import bisect
from itertools import accumulate

fake = Faker()
siteConfig = {
    'siteCount': 20
}

dbConfig = {
    'user'      : 'root',
    'host'      : 'localhost',
    'db'        : 'bench',
    'password'  : '12345',
    'charset'   : 'latin1'
}

postsPopulatorConfig = {
    'postsCount': 1000000,
    'batchSize' : 5000
}

tagsPopulatorConfig = {
    'tagsCount': 100000,
    'batchSize': 10000
}

relationGeneratorConfig = {
    'batchSize': 5000
}

fakeText = ""

def choice(weighted_choices):
    choices, weights = zip(*weighted_choices)
    cumdist = list(accumulate(weights))
    x = random.random() * cumdist[-1]
    weightedChoice = choices[bisect(cumdist, x)]
    return(weightedChoice)

def specialChoice():
    specialPostWeights = [
        (1,.1),
        (0,.9)
    ]
    return choice(specialPostWeights)

def postTypeChoice():
    postTypeWeights = [
        ('normal', .6), 
        ('ecommerce', .044), 
        ('slideshow', .044), 
        ('video', .044), 
        ('duplicate', .044),
        ('branded_club', .044), 
        ('brand_article', .044), 
        ('brand_article_video', .044), 
        ('longform', .044), 
        ('reposted_slideshow', .044)
    ]
    return choice(postTypeWeights)

def countryChoice(result):
    countryWeights = [('ES',.25),('US',.25),('MX',.25),('CO',.25),('NONE',3)]
    ES = 0
    US = 0
    MX = 0
    CO = 0
    for countries in range(4):
        countryWeightedChoice = choice(countryWeights)
        if (countryWeightedChoice == 'ES'):
            ES = 1
        elif (countryWeightedChoice == 'US'):
            US = 1
        elif (countryWeightedChoice == 'MX'):
            MX = 1
        elif (countryWeightedChoice == 'CO'):
            CO = 1
    result = (ES, US, MX, CO)
    if result == (0, 0, 0, 0):
        result = countryChoice(result) 
    return result

def randomRank():
    rankConfig = []
    for x in range(1, (postsPopulatorConfig['postsCount']) + 1):
        rankConfig.append(x)
    np.random.shuffle(rankConfig)
    return rankConfig

def process():
    global fakeText
    postsPopulatorConfig['postsCount'] //= siteConfig['siteCount'] 
    fakeText = fake.text(max_nb_chars = 1000000)

process()