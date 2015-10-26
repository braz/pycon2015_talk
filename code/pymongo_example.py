from pymongo import MongoClient
from bson.son import SON

db = MongoClient().test
tmp_created_collection_per_day_name = 'page_per_day_hits_tmp';
pipeline = [{"$project":{'page': '$PAGE', 'time': { 'y': {'$year':'$DATE'} , 'm':{'$month':'$DATE'}, 'day':{'$dayOfMonth':'$DATE'}}}}, {'$group':{'_id':{'p':'$page','y':'$time.y','m':'$time.m','d':'$time.day'}, 'daily':{'$sum':1}}},{'$out': tmp_created_collection_per_day_name}]
results = db.logs.aggregate(pipeline)