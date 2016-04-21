from createDatabase import process as createDatabase
from createTables import process as createTables
from databasePopulator import process as databasePopulator
from tagsRelationGenerator import process as tagsRelationGenerator
from rankGenerator import process as rankGenerator
from config import siteConfig
from faker import Faker

def process():
    global fake
    fake = Faker()    
    createDatabase()
    createTables()
    for totalSites in range(siteConfig['siteCount']):
        site = fake.url()
        databasePopulator(site)
        tagsRelationGenerator(site)
        rankGenerator(site)
    print("=====================================================")
    print("Mock Database Created")
    print("=====================================================")

process()