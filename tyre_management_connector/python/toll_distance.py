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

def create_records(data):
	client, db = mongo_db_connect()
	my_collection = db["toll_distance_data"]
	my_collection.insert_one(data)
	client.close()

def get_toll_distance():
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
			res['erp_time_stamp']=frappe.utils.now()
			create_records(data=res)

def get_location_distance(from_lng,from_lat,to_lng,to_lat):
	url = f"http://router.project-osrm.org/route/v1/driving/{from_lng},{from_lat};{to_lng},{to_lat}?alternatives=true&steps=false&overview=simplified&annotations=false"
	response = requests.request("GET", url).json()
	if response.get('code') in ["200",200,"ok","OK","Ok"]:
		return response

#Get Toll Results
@frappe.whitelist()
def get_toll_distance_bulk(filters=None):
	client, db = mongo_db_connect()
	collection = db['toll_distance_data']
	# Use find() to retrieve all documents
	cursor = collection.find({})
	results = list(cursor)
	print(results)
	client.close()
