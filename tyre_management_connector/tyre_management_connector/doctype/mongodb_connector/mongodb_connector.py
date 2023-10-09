# Copyright (c) 2023, Aerele and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from pymongo import MongoClient

class MongoDBConnector(Document):
	def validate(self):
		try:
			client = MongoClient(self.url)
			if client.server_info():
				frappe.throw("Connection successful. MongoDB is running.")
			else:
				frappe.throw("Connection failed. MongoDB may not be running or the connection string is incorrect.")
		except Exception as e:
			frappe.throw(f"Connection failed with error: {str(e)}")
		finally:
			client.close()