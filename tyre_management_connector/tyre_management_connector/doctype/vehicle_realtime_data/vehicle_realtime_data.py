# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document
from datetime import datetime
from pymongo import MongoClient,ASCENDING,DESCENDING

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
#	frappe.log_error(message = args, title = "Intangles Realtime Data")
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
	return final_data