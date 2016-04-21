from clearDatabase import process as clearDatabase
from createTables import process as createTables
from databasePopulator import process as databasePopulator
from tagsRelationGenerator import process as tagsRelationGenerator
from rankGenerator import process as rankGenerator
from config import siteConfig
from faker import Faker

fake = Faker()

def process():
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