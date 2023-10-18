# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt
import copy
import requests
import frappe
import json
from frappe.model.document import Document
from datetime import datetime
from pymongo import MongoClient,ASCENDING,DESCENDING
from tyre_management_connector.python.intangles_api import get_intangles_odometer_data

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
	if isinstance(args, str):
		args = json.loads(args)
	args.pop('cmd')
	erp_time_stamp = frappe.utils.now()
	args['erp_time_stamp']=erp_time_stamp
	frappe.get_doc({
		"doctype" : "Smart Tyre Realtime Data",
		"device_id" : args.get('DeviceId'),
		"device_date_time" : args.get('DeviceDateTime'),
		"vehicle_no" : args.get('vehicleNo'),
		"erp_time_stamp" : erp_time_stamp,
		"overall_response" : json.dumps(args,indent=4)
	}).insert(ignore_permissions=True)
	return {"response" : "Success"}

@frappe.whitelist()
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
@frappe.whitelist()
def get_smart_tyre_data_bulk(filters=None,odometer_value=None):
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
	vehicles = [d.get("_id") for d in results if "_id" in d]
	if vehicles:
		final_data={}
		url = "https://desk.lnder.in/api/method/tyre_management.tyre_management.doctype.vehicle_tire_position.vehicle_tire_position.get_vehicle_tyre_positions"

		payload = json.dumps({"vehicles": [vehicles]})
		headers = {
			'Authorization': 'token 5d86d079564a18a:80e46996b1b9eaf',
			'Content-Type': 'application/json'
		}
		response = requests.request("GET", url, headers=headers, data=payload)
		if response.ok:
			response=response.json().get('message')
			if odometer_value:
				odometer_details = get_intangles_odometer_data(vehicle_no=vehicles)
			for result in results:
				if response.get(result.get('_id')):
					smart_tyre_data = json.loads(result.get('latest_data').get('overall_response'))
					for idx,key in enumerate(response.get(result.get('_id'))):
						if (final_data.get(result.get('_id'))):
							final_data[result.get('_id')].append({
								"tyre_position": key,
								"tyre_serial_no": response.get(result.get('_id')).get(key),
								"Pres":smart_tyre_data.get("Pres_"+str(idx)),
								"Temp":smart_tyre_data.get("Temp_"+str(idx)),
								"Bat":smart_tyre_data.get("Bat_"+str(idx)),
								"Event":smart_tyre_data.get("Event_"+str(idx)),
								"erp_time_stamp":smart_tyre_data.get("erp_time_stamp")
							})
						else:
							for data in odometer_details:
								if data.get('plate') == result.get('_id'):
									final_data[result.get('_id')]=[{
										"current_odometer_value" : data.get('end').get('odo_km'),
										"current_engine_hours" : data.get('end').get('engine_hours'),
										"total_distance_today" : data.get('end').get('total_distance'),
										"total_engine_hours_today" : data.get('end').get('total_engine_hours')
									}]
							if (final_data.get(result.get('_id'))):
								final_data[result.get('_id')].append({
									"tyre_position": key,
									"tyre_serial_no": response.get(result.get('_id')).get(key),
									"Pres":smart_tyre_data.get("Pres_"+str(idx)),
									"Temp":smart_tyre_data.get("Temp_"+str(idx)),
									"Bat":smart_tyre_data.get("Bat_"+str(idx)),
									"Event":smart_tyre_data.get("Event_"+str(idx)),
									"erp_time_stamp":smart_tyre_data.get("erp_time_stamp"),
								})
							else:
								final_data[result.get('_id')]=[{
									"tyre_position": key,
									"tyre_serial_no": response.get(result.get('_id')).get(key),
									"Pres":smart_tyre_data.get("Pres_"+str(idx)),
									"Temp":smart_tyre_data.get("Temp_"+str(idx)),
									"Bat":smart_tyre_data.get("Bat_"+str(idx)),
									"Event":smart_tyre_data.get("Event_"+str(idx)),
									"erp_time_stamp":smart_tyre_data.get("erp_time_stamp"),
								}]
			return final_data
		else:
			return response.raise_for_status()
	else:
		return []
