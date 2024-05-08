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

def toll_details_from_file():
	import pandas as pd
	df = pd.read_excel(f"/home/navin/Downloads/firefox_downloads/Toll-Details updated.xlsx")
	for _1, row_a in df.iterrows():
		print(_1)
		if row_a.get('actual_latitude') and row_a.get('actual_longitude'):
			for _2, row_b in df.iterrows():
				if row_b.get('actual_latitude') and row_b.get('actual_longitude'):
					if row_a.get('tollplaza_id') !=  row_b.get('tollplaza_id'):
						get_toll_distance(
							from_lng=row_a.get('actual_longitude'),
							from_lat=row_a.get('actual_latitude'),
							to_lng=row_b.get('actual_longitude'),
							to_lat=row_b.get('actual_latitude')
						)

def get_toll_distance(from_lng,from_lat,to_lng,to_lat):
	if res := get_location_distance(
			from_lng=from_lng,
			from_lat=from_lat,
			to_lng=to_lng,
			to_lat=to_lat
		):
		res['processed_lat_lng']={
			"from":{
				"lat" : from_lat,
				"lng" : from_lng
			},
			"to":{
				"lat" : to_lat,
				"lng" : to_lng
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
