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
		final_data[result.get('_id')]["location_details"] = get_location_for_lat_lng(lat=json.loads(result.get('latest_data').get('overall_response')).get('geocode').get('lat'),lng=json.loads(result.get('latest_data').get('overall_response')).get('geocode').get('lng'))
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
@frappe.whitelist()
def find_stopped_vehicles(threshold_minutes=20,get_location=False):
	mongo_uri = frappe.db.get_single_value("MongoDB Connector", "url")
	client_server = MongoClient(mongo_uri)
	db = client_server.get_database()
	collection = db['intangles_vehicle_data']

	current_time = datetime.strptime(frappe.utils.now(), "%Y-%m-%d %H:%M:%S.%f")
	time_change=timedelta(minutes=threshold_minutes)
	threshold_time=(current_time-time_change)

	# Get records for each vehicle before the threshold
	before_threshold_pipeline = [
		{"$match": {
			"erp_time_stamp": {
				"$lte": str(threshold_time)			}
		}},
		{"$sort": {"vehicle_no": 1, "erp_time_stamp": -1}},
		{"$group": {
			"_id": "$vehicle_no",
			"latest_before_threshold": {"$first": "$$ROOT"}
		}}
	]

	before_threshold_cursor = collection.aggregate(before_threshold_pipeline)
	before_threshold_results = list(before_threshold_cursor)
	# Get the current latest record for each vehicle
	current_latest_pipeline = [
		{"$sort": {"vehicle_no": 1, "erp_time_stamp": -1}},
		{"$group": {
			"_id": "$vehicle_no",
			"latest": {"$first": "$$ROOT"}
		}}
	]

	current_latest_cursor = collection.aggregate(current_latest_pipeline)
	current_latest_results = list(current_latest_cursor)

	client_server.close()

	stopped_vehicles = []

	for result in before_threshold_results:
		vehicle_no = result.get('_id')
		latest_before_threshold_data = result.get('latest_before_threshold')

		# Find the corresponding latest record for the current vehicle
		current_latest_data = next((item.get('latest') for item in current_latest_results if item.get('_id') == vehicle_no), None)

		if current_latest_data:
			latest_before_threshold_geocode = json.loads(latest_before_threshold_data.get('overall_response', '{}')).get('geocode', {})
			latest_geocode = json.loads(current_latest_data.get('overall_response', '{}')).get('geocode', {})

			if (
				latest_before_threshold_geocode.get('lat') == latest_geocode.get('lat') and
				latest_before_threshold_geocode.get('lng') == latest_geocode.get('lng')
			):
				# Convert string timestamps to datetime objects
				latest_before_threshold_timestamp = datetime.strptime(latest_before_threshold_data['erp_time_stamp'], "%Y-%m-%d %H:%M:%S.%f")
				latest_timestamp = datetime.strptime(current_latest_data['erp_time_stamp'], "%Y-%m-%d %H:%M:%S.%f")

				standing_duration = (latest_timestamp - latest_before_threshold_timestamp).total_seconds()

				if standing_duration >= (threshold_minutes * 60):
					data = {
						"vehicle_no": vehicle_no,
						"last_update_time": current_latest_data['erp_time_stamp']
					}
					if get_location:
						data["last_location"]=get_location_for_lat_lng(lat=json.loads(current_latest_data.get('overall_response', '{}')).get('geocode').get('lat'),lng=json.loads(current_latest_data.get('overall_response', '{}')).get('geocode').get('lng'))
					stopped_vehicles.append(data)
	return stopped_vehicles

def get_location_for_lat_lng(lat, lng):
	url = f"https://geocode.maps.co/reverse?lat={lat}&lon={lng}"
	response = requests.request("GET", url, headers={}, data={})
	if response.ok:
		response=response.json()
		response.pop('licence')
		response.pop('powered_by')
		response.pop('osm_type')
		response.pop('osm_id')
		return response
