# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document

class VehicleRealtimeData(Document):
	
	def db_insert(self, *args, **kwargs):
		pass

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
	frappe.log_error(message = args, title = "Intangles Realtime Data")
	if isinstance(args, str):
		args = json.loads(args)
	frappe.get_doc({
                "doctype" : "Vehicle Realtime Data",
                "device_id" : args.get('device_id'),
                "vehicle_no" : args.get('vehicle_id'),
                "erp_time_stamp" : frappe.utils.now(),
                "overall_response" : json.dumps(args,indent=4)
	}).insert(ignore_permissions=True)
	return {"response" : "Success"}
