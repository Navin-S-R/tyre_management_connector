# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document
from datetime import datetime
from pymongo import MongoClient,ASCENDING,DESCENDING

class SmartTyreRealtimeData(Document):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
		self.mongo_uri = mongo_uri
		self.client = MongoClient(self.mongo_uri)
		self.db = self.client.get_database()
	
	def db_insert(self, *args, **kwargs):
		my_collection = self.db["smart_tyre_data"]
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

@frappe.whitelist()
def pull_realtime_data(**args):
	frappe.log_error(message = args, title = "JK Realtime data")
	if isinstance(args, str):
		args = json.loads(args)
	args={'DeviceID': '356078113946756', 'DeviceDateTime': '2023-10-09 11:46:03', 'messageCount': '6105', 'Latitude': '16.47912', 'Longitude': '80.61800', 'Speed': '0', 'VehicleBattery': '24.6', 'MGR_Value': '32031089', 'Event': '01', 'Nooftreeltags': '7', 'vehicleNo': 'NL01B1288', 'alertTyrePosition': '-', 'Fule_Value': '', 'Asset_Value': '', 'Pres_0': '133', 'Temp_0': '35', 'Bat_0': '100', 'Event_0': '0', 'Pres_1': '135', 'Temp_1': '35', 'Bat_1': '100', 'Event_1': '0', 'Pres_2': '0', 'Temp_2': '36', 'Bat_2': '100', 'Event_2': '1', 'Pres_3': '131', 'Temp_3': '40', 'Bat_3': '100', 'Event_3': '0', 'Pres_4': '0', 'Temp_4': '36', 'Bat_4': '100', 'Event_4': '32', 'Pres_5': '118', 'Temp_5': '35', 'Bat_5': '100', 'Event_5': '0', 'Pres_6': '-', 'Temp_6': '-', 'Bat_6': '-', 'Event_6': '2', 'cmd': 'tyre_management.python.tyre_details_api.pull_realtime_data'}
	frappe.get_doc({
		"doctype" : "Smart Tyre Realtime Data",
		"device_id" : args.get('DeviceId'),
		"device_date_time" : args.get('DeviceDateTime'),
		"vehicle_no" : args.get('vehicleNo'),
		"erp_time_stamp" : frappe.utils.now(),
		"overall_response" : json.dumps(args,indent=4)
	}).insert(ignore_permissions=True)
	return {"response" : "Success"}

def get_smart_tyre_data(
		vehicle_no=None,
		device_id=None,
		from_date=None,
		to_date=None,
		limit=None,
		sort=None
	):
	
	# from_date = str(datetime(2023, 10, 6, 00, 00, 00, 00))
	# to_date = str(datetime(2023, 10, 10, 23, 59, 59, 00))

	#filters
	filters ={}
	if from_date and to_date:
		filters["modified"]={
							"$gte": from_date,
							"$lte": to_date
							}
	if vehicle_no:
		filters["vehicle_no"]=vehicle_no
	if device_id:
		filters["device_id"]=device_id
	
	limit = int(limit) if limit and int(limit) else 0
	sort = 1 if sort=="ASC" else -1
	
	#Create MongoDB connection
	mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
	client_server = MongoClient(mongo_uri)
	client_server_db = client_server.get_database()
	client_server_collection = client_server_db["smart_tyre_data"]
	results = client_server_collection.find(filters).sort([("modified", sort)]).limit(limit)
	data=[]
	for result in results:
		data.append(result)
	client_server.close()

	return data

#Get Bulk 
def get_smart_tyre_data_bulk(filters=None):
	mongo_uri = frappe.db.get_single_value("MongoDB Connector","url")
	client_server = MongoClient(mongo_uri)
	db = client_server.get_database()
	collection = db['smart_tyre_data']
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
	return results