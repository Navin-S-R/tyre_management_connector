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
	df = pd.read_excel(f"/home/frappe/Toll-Details-updated.xlsx")
	for _1, row_a in df.iterrows():
		print(_1)
		if row_a.get('actual_latitude') and row_a.get('actual_longitude'):
			for _2, row_b in df.iterrows():
				if row_b.get('actual_latitude') and row_b.get('actual_longitude'):
					if row_a.get('tollplaza_id') !=  row_b.get('tollplaza_id'):
						if not check_for_record_existence(row_a.get('tollplaza_id'),row_b.get('tollplaza_id')):
							get_toll_distance(
								from_lng=row_a.get('actual_longitude'),
								from_lat=row_a.get('actual_latitude'),
								to_lng=row_b.get('actual_longitude'),
								to_lat=row_b.get('actual_latitude'),
								from_toll_id= str(row_a.get('tollplaza_id')) if row_a.get('tollplaza_id') else None,
								from_tollplaza_name=row_a.get('tollplaza_name'),
								from_city=row_a.get('gle_city'),
								from_state=row_a.get('gle_state'),
								to_toll_id=str(row_b.get('tollplaza_id')) if row_b.get('tollplaza_id') else None,
	                                                        to_tollplaza_name=row_b.get('tollplaza_name'),
	                                                        to_city=row_b.get('gle_city'),
	                                                        to_state=row_b.get('gle_state')
							)

def get_toll_distance(from_lng,from_lat,to_lng,to_lat,from_toll_id=None,from_tollplaza_name=None,from_city=None,from_state=None,to_toll_id=None,to_tollplaza_name=None,to_city=None,to_state=None):
	if res := get_location_distance(
			from_lng=from_lng,
			from_lat=from_lat,
			to_lng=to_lng,
			to_lat=to_lat
		):
		res['processed_lat_lng']={
			"from":{
				"lat" : from_lat,
				"lng" : from_lng,
				"toll_id" : from_toll_id,
				"tollplaza_name" : from_tollplaza_name,
				"city" : from_city,
				"state" : from_state
			},
			"to":{
				"lat" : to_lat,
				"lng" : to_lng,
				"toll_id" : to_toll_id,
				"tollplaza_name" : to_tollplaza_name,
                                "city" : to_city,
                                "state" : to_state
			}
		}
		res['erp_time_stamp']=frappe.utils.now()
		create_records(data=res)

def get_location_distance(from_lng,from_lat,to_lng,to_lat):
	try:
		url = f"http://router.project-osrm.org/route/v1/driving/{from_lng},{from_lat};{to_lng},{to_lat}?alternatives=true&steps=false&overview=simplified&annotations=false"
		response = requests.request("GET", url).json()
		if response.get('code') in ["200",200,"ok","OK","Ok"]:
			return response
	except Exception as e:
		print(e)

def check_for_record_existence(from_toll_id, to_toll_id):
	try:
		# Connect to MongoDB and access collection
		client, db = mongo_db_connect()
		collection = db['toll_distance_data']
		# Check if record exists
		query = {"processed_lat_lng.from.toll_id": str(from_toll_id), "processed_lat_lng.to.toll_id": str(to_toll_id)}
		result = collection.find_one(query)
		if result:
			return True
		else:
			return False
	except Exception as e:
		print("An error occurred:", e)
		return False
	finally:
		if client:
			client.close()
#Get Toll Results
@frappe.whitelist()
def get_toll_distance_bulk(filters=None):
	from frappe.desk.query_report import build_xlsx_data
	from frappe.utils.xlsxutils import make_xlsx
	from frappe.utils import get_site_path

	client, db = mongo_db_connect()
	collection = db['toll_distance_data']
	# Use find() to retrieve all documents
	cursor = collection.find({})
	results = list(cursor)
	print(results)
	final_data=[]
	columns = ["from_toll_id","from_tollplaza_name","from_city","from_state","from_lat","from_lng","to_toll_id","to_tollplaza_name","to_city","to_state","to_lat","to_lat","geometry","duration","distance"]
	xl_columns=[]
	for col_name in columns:
		xl_columns.append({'fieldname': col_name.lower().replace(" ","_"), 'label': col_name, 'fieldtype': 'Data', 'width': 300})
	for p_row in results:
		processed_lat_lng=p_row.get('processed_lat_lng')
		p_row_dict={
			"from_toll_id" : processed_lat_lng.get('from',{}).get('toll_id'),
			"from_tollplaza_name" : processed_lat_lng.get('from',{}).get('tollplaza_name'),
			"from_city" : processed_lat_lng.get('from',{}).get('city'),
			"from_state" : processed_lat_lng.get('from',{}).get('state'),
			"from_lat" : processed_lat_lng.get('from',{}).get('lat'),
			"from_lng" : processed_lat_lng.get('from',{}).get('lng'),
			"to_toll_id" : processed_lat_lng.get('to',{}).get('toll_id'),
                        "to_tollplaza_name" : processed_lat_lng.get('to',{}).get('tollplaza_name'),
                        "to_city" : processed_lat_lng.get('to',{}).get('city'),
                        "to_state" : processed_lat_lng.get('to',{}).get('state'),
			"to_lat" : processed_lat_lng.get('to',{}).get('lat'),
			"to_lnt" : processed_lat_lng.get('to',{}).get('lng')
		}
		for routes in p_row.get('routes'):
			if not "geometry" in p_row_dict:
				p_row_dict['geometry'] = routes.get('geometry')
				p_row_dict['duration'] = float(routes.get('duration'))/60 if routes.get('duration') else None
				p_row_dict['distance'] = float(routes.get('distance'))/1000 if routes.get('distance') else None
				final_data.append(p_row_dict)
			else:
				p_row_dict={
					"from_toll_id" : None,
					"from_tollplaza_name" : None,
					"from_city" : None,
					"from_state" : None,
					"from_lat" : None,
					"from_lng" : None,
					"to_toll_id" : None,
					"to_tollplaza_name" : None,
					"to_city" : None,
					"to_state" : None,
					"to_lat" : None,
					"to_lnt" : None
				}
				p_row_dict['geometry'] = routes.get('geometry')
				p_row_dict['duration'] = float(routes.get('duration'))/60 if routes.get('duration') else None
				p_row_dict['distance'] = float(routes.get('distance'))/1000 if routes.get('distance') else None
				final_data.append(p_row_dict)
	report_data = frappe._dict()
	report_data["result"] = final_data
	report_data["columns"] = xl_columns
	xlsx_data, column_widths = build_xlsx_data(report_data, [], 1, ignore_visible_idx=True)
	xlsx_file = make_xlsx(xlsx_data, f"Toll Details", column_widths=column_widths).getvalue()
	file_path = f"{get_site_path()}/public/files/toll.xlsx"
	with open(file_path, "wb") as file:
		file.write(xlsx_file)
	if client:
		client.close()
