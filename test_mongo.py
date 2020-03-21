import pymongo
import dns

from first_query import query_one
from second_query import query_two
from third_query import query_three
from fourth_query import query_four
from fifth_query import query_five

client = pymongo.MongoClient(
    "mongodb+srv://maximevgrafov:maximevgrafov@dmdassignmentspring-qvs0c.mongodb.net/test?retryWrites=true&w=majority")
mongo_db = client.test

