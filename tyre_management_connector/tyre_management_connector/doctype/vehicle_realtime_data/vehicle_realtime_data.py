# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document
from datetime import datetime, timedelta
from pymongo import MongoClient,ASCENDING,DESCENDING
import requests
class VehicleRealtimeData(Document):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
		self.mongo_uri = mongo_uri
		self.client = MongoClient(self.mongo_uri)
		self.db = self.client.get_database()

	def db_insert(self, *args, **kwargs):
		my_collection = self.db["intangles_vehicle_data"]
		data_to_insert = self.get_valid_dict(convert_dates_to_str=True)
		my_collection.insert_one(data_to_insert)
		self.client.close()

	def load_from_db(self):
		pass

	def db_update(self, *args, **kwargs):
		pass

	@staticmethod
	def get_list(args):
		pass

	@staticmethod
	def get_count(args):
		pass

	@staticmethod
	def get_stats(args):
		pass

import frappe
@frappe.whitelist()
def pull_realtime_data(**args):
	if isinstance(args, str):
		args = json.loads(args)
	erp_time_stamp = frappe.utils.now()
	args.pop('cmd')
	args['erp_time_stamp']=erp_time_stamp
	frappe.get_doc({
				"doctype" : "Vehicle Realtime Data",
				"device_id" : args.get('device_id'),
				"vehicle_no" : args.get('vehicle_id'),
				"erp_time_stamp" : erp_time_stamp,
				"overall_response" : json.dumps(args,indent=4)
	}).insert(ignore_permissions=True)
	return {"response" : "Success"}

#Get Bulk
@frappe.whitelist()
def get_intangles_vehicle_data_bulk(filters=None):
	mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
	client_server = MongoClient(mongo_uri)
	db = client_server.get_database()
	collection = db['intangles_vehicle_data']
	query = {}
	sort = DESCENDING
	if filters:
		if 'vehicle_no' in filters:
			query['vehicle_no'] = {"$in": filters['vehicle_no']}
		if 'device_id' in filters:
			query['device_id'] = {"$in": filters['device_id']}
		if 'from_date' in filters and 'to_date' in filters:
			query['modified'] = {
				"$gte": filters['from_date'],
				"$lte": filters['to_date']
			}
		if 'sort' in filters and filters['sort'].upper() == 'ASC':
			sort = ASCENDING
	pipeline = [
		{"$match": query},
		{"$sort": {"vehicle_no": 1, "modified": sort}},
		{"$group": {
			"_id": "$vehicle_no",
			"latest_data": {"$first": "$$ROOT"}
		}}
	]
	cursor = collection.aggregate(pipeline)
	results = list(cursor)
	client_server.close()
	final_data={}
	for result in results:
		final_data[result.get('_id')] = json.loads(result.get('latest_data').get('overall_response'))
		url = f"https://geocode.maps.co/reverse?lat={json.loads(result.get('latest_data').get('overall_response')).get('geo').get('lat')}&lon={json.loads(result.get('latest_data').get('overall_response')).get('geo').get('lng')}"

		payload = {}
		headers = {}
		response = requests.request("GET", url, headers=headers, data=payload)
		if response.ok:
			response=response.json()
			final_data[result.get('_id')]["location_details"] = response
	return final_data

#Delete data
def delete_old_intangles_vehicle_data():
	mongo_uri = frappe.db.get_single_value("MongoDB Connector", "url")
	client = MongoClient(mongo_uri)
	db = client.get_database()
	collection = db['intangles_vehicle_data']
	threshold_date = datetime.now() - timedelta(days=1)
	query = {
		"modified": {"$lt": str(threshold_date)}
	}
	result = collection.delete_many(query)
	collection.drop_indexes()
	db.command("compact", collection.name)
	client.close()
	return result.deleted_count


#Get long standing vehicle status
def find_stopped_vehicles(threshold_minutes=20):
	mongo_uri = frappe.db.get_single_value("MongoDB Connector", "url")
	client_server = MongoClient(mongo_uri)
	db = client_server.get_database()
	collection = db['intangles_vehicle_data']

	current_time = frappe.utils.now()
	threshold_time = frappe.utils.add_days(current_time, days=-1 * threshold_minutes / (24 * 60))

	# Convert threshold_time to a datetime object
	threshold_time = datetime.strptime(str(threshold_time), "%Y-%m-%d %H:%M:%S.%f")

	# Add debugging print statements
	print(f"Debug: Current Time: {current_time}")
	print(f"Debug: Threshold Time: {threshold_time}")

	pipeline = [
		{"$match": {
			"erp_time_stamp": {
				"$gte": str(threshold_time),
				"$lt": str(current_time)
			}
		}},
		{"$sort": {"vehicle_no": 1, "erp_time_stamp": 1}},
		{"$group": {
			"_id": "$vehicle_no",
			"data": {"$push": "$$ROOT"}
		}}
	]

	cursor = collection.aggregate(pipeline)
	results = list(cursor)

	# Add more print statements for debugging
	print(f"Debug: Number of Results: {len(results)}")

	client_server.close()

	stopped_vehicles = []

	for result in results:
		vehicle_data_list = result.get('data')
		if len(vehicle_data_list) > 1:
			# Get the first set of data (before threshold)
			before_threshold_data = vehicle_data_list[0]

			# Get the latest set of data
			latest_data = vehicle_data_list[-1]

			before_threshold_geocode = json.loads(before_threshold_data.get('overall_response', '{}')).get('geocode', {})
			latest_geocode = json.loads(latest_data.get('overall_response', '{}')).get('geocode', {})

			# Compare geocodes for both sets
			if (
				before_threshold_geocode.get('lat') == latest_geocode.get('lat') and
				before_threshold_geocode.get('lng') == latest_geocode.get('lng')
			):
				try:
					url = f"https://geocode.maps.co/reverse?lat={latest_geocode.get('lat')}&lon={latest_geocode.get('lng')}"
					payload = {}
					headers = {}
					response = requests.request("GET", url, headers=headers, data=payload)
					if response.ok:
						response=response.json()
						response.pop('licence')
						response.pop('powered_by')
						response.pop('osm_type')
						response.pop('osm_id')
						last_location=response
				except:
					last_location=None
				stopped_vehicles.append({
					"vehicle_no": result.get('_id'),
					"last_geocode": latest_geocode,
					"last_location": last_location, 
					"last_update_time": latest_data.get('erp_time_stamp')
				})

	return stopped_vehicles
