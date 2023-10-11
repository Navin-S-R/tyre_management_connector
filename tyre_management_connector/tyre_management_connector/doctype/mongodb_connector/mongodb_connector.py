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
				frappe.msgprint("Connection successful. MongoDB is running.")
			else:
				frappe.msgprint("Connection failed. MongoDB may not be running or the connection string is incorrect.")
		except Exception as e:
			frappe.throw(f"Connection failed with error: {str(e)}")
		finally:
			client.close()


def delete_data_in_collection(collection_name):
	from pymongo import MongoClient
	mongo_uri = frappe.db.get_single_value("MongoDB Connector", "url")
	client = MongoClient(mongo_uri)
	db = client.get_database()
	collection = db[collection_name]
	collection.delete_many({})
	client.close()
