from createDatabase import process as createDatabase
from createTables import process as createTables
from tagsPopulator import process as tagsPopulator
from postsPopulator import process as postsPopulator
from tagsRelationGenerator import process as tagsRelationGenerator
from rankGenerator import process as rankGenerator
from config import siteConfig
from faker import Faker
from timer import Timer, displayTimer

def process():
    fake = Faker()
    timer = Timer()
    print("=====================================================")
    print("Timer started at : " + timer.get_time_hhmmss())
    print("=====================================================")
    createDatabase()
    createTables()
    tagsPopulator()
    displayTimer(timer.get_time_hhmmss(), 'Elapsed')
    for totalSites in range(siteConfig['siteCount']):
        site = fake.url()
        postsPopulator(site)
        displayTimer(timer.get_time_hhmmss(), 'Elapsed')
    tagsRelationGenerator()
    displayTimer(timer.get_time_hhmmss(), 'Elapsed')
    print("=====================================================")
    print("Mock Database Created")
    print("=====================================================")
    executionTime = timer.get_time_hhmmss()
    displayTimer(timer.get_time_hhmmss(), 'Execution')

process()