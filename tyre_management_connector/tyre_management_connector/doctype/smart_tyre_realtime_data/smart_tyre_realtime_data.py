# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document
from datetime import datetime
from pymongo import MongoClient

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
	args = json.loads(args)
	frappe.new_get({
		"doctype" : "Smart Tyre Realtime Data",
		"device_id" : args.get('DeviceId'),
		"device_date_time" : args.get('DeviceDateTime'),
		"ref_doctype" : "Vehicle Registration Certificate",
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