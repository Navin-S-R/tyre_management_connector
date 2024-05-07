import frappe
import requests
import json
from pymongo import MongoClient,ASCENDING,DESCENDING

def mongo_db_connect():
	mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
	mongo_uri = mongo_uri
	client = MongoClient(mongo_uri)
	db = client.get_database()
	return client, db

def create_records(client,db,data):
	my_collection = db["toll_distance_data"]
	data_to_insert = data
	my_collection.insert_one(data_to_insert)
	client.close()

def get_toll_distance():
	client, db = mongo_db_connect()
	toll_list=[
		{
			"from_lng" : 80.1997,
			"from_lat" : 13.0741,
			"to_lng" : 76.9696,
			"to_lat" : 11.0167
		},
		{
			"from_lng" : 80.1997,
			"from_lat" : 13.0741,
			"to_lng" : 76.9696,
			"to_lat" : 11.0167
		}
	]
	for row in toll_list:
		print(row)
		if res := get_location_distance(from_lng=row.get('from_lng'),from_lat=row.get('from_lat'),to_lng=row.get('to_lng'),to_lat=row.get('to_lat')):
			res['processed_lat_lng']={
				"from":{
					"lat" : row.get('from_lat'),
					"lng" : row.get('from_lng')
				},
				"to":{
					"lat" : row.get('to_lat'),
					"lng" : row.get('to_lng')
				}
			}
			create_records(client=client, db=db, data=json.dumps(res))

def get_location_distance(from_lng,from_lat,to_lng,to_lat):
	url = f"http://router.project-osrm.org/route/v1/driving/{from_lng},{from_lat};{to_lng},{to_lat}?alternatives=true&steps=false&overview=simplified&annotations=false"
	response = requests.request("GET", url).json()
	if response.get('code')==200:
		return response

#Get Toll Results
@frappe.whitelist()
def get_toll_distance_bulk(filters=None):
	client, db = mongo_db_connect()
	collection = db['toll_distance_data']
	cursor = collection.aggregate()
	results = list(cursor)
	print(results)
	client.close()
