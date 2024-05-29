import frappe
import json
from datetime import datetime
import requests
from frappe import _

@frappe.whitelist()
def get_intangles_odometer_data(start_time=None,end_time=None,vehicle_no=None):
	connector_doc=frappe.get_single("Intangles Connector")
	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	epoch = datetime(1970, 1, 1)
	start_time_obj = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
	start_time_difference = start_time_obj - epoch
	end_time_obj = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
	end_time_difference = end_time_obj - epoch

	url = f"{connector_doc.url}/api/v1/vendor/report/odoreport?start_time={int(start_time_difference.total_seconds() * 1000)}&end_time={int(end_time_difference.total_seconds() * 1000)}&account_id={connector_doc.account_id}"
	payload = {}
	headers = {
		'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}
	response = requests.request("GET", url, headers=headers, data=payload)
	if response.ok:
		response=response.json().get('result')
		reqd_data=[]
		if vehicle_no:
			for row in response:
				if row.get('plate') in vehicle_no:
					reqd_data.append(row)
		else:
			reqd_data = response
		return reqd_data
	else:
		response.raise_for_status()
#Update ODOMETER Value
def update_odometer_value():
	odometer_value=get_intangles_odometer_data()
	url = "https://desk.lnder.in/api/method/tyre_management.tyre_management.doctype.tyre_serial_no.tyre_serial_no.update_odometer_value"
	payload = json.dumps({"args":[odometer_value]})
	headers = {
		'Authorization': 'token 5d86d079564a18a:80e46996b1b9eaf',
		'Content-Type': 'application/json'
	}
	response = requests.request("POST", url, headers=headers, data=payload)
	if response.ok:
		response=response.json()
	else:
		frappe.log_error(response.raise_for_status())

#get epoch datetime
@frappe.whitelist()
def get_epoch_datetime(date):
	epoch = datetime(1970, 1, 1)
	if isinstance(date, datetime):
		date_str = date.strftime("%Y-%m-%d %H:%M:%S")
	else:
		date_str = date
	date_object=datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
	time_difference = date_object - epoch
	return int(time_difference.total_seconds() * 1000)
#Get vehicle idling Log
def get_vehicle_idling_log(start_time=None,end_time=None,vehicle_no=None):
	"""
		:param start_time: The start time parameter should be in the format "YYYY-MM-DD HH:MM:SS"
		:param end_time: The `end_time` parameter should be in the format "YYYY-MM-DD HH:MM:SS
		:param vehicle_no: The parameter "vehicle_no" is used to specify the vehicle number
	"""
	connector_doc=frappe.get_single("Intangles Connector")
	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	epoch = datetime(1970, 1, 1)
	start_time_obj = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
	start_time_difference = start_time_obj - epoch

	end_time_obj = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
	end_time_difference = end_time_obj - epoch

	url = f"{connector_doc.url}/api/v1/vendor/alert_logs/{connector_doc.account_id}/list/{int(start_time_difference.total_seconds() * 1000)}/{int(end_time_difference.total_seconds() * 1000)}?types=idling"
	headers = {
	  'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}

	response = requests.request("GET", url, headers=headers)
	if response.ok:
		response=response.json().get('result')
		reqd_data=[]
		if vehicle_no:
			for row in response.get('logs'):
				if row.get('vehicle_plate') in vehicle_no:
					reqd_data.append(row)
		else:
			reqd_data = response.get('logs')
		return reqd_data
	else:
		response.raise_for_status()

#Get vehicle idling Log
def get_vehicle_stoppage_log(start_time=None,end_time=None,vehicle_no=None,stoppage_duration=None):
	"""
		:param start_time: The start time parameter should be in the format "YYYY-MM-DD HH:MM:SS"
		:param end_time: The `end_time` parameter should be in the format "YYYY-MM-DD HH:MM:SS
		:param vehicle_no: The parameter "vehicle_no" is used to specify the vehicle number
		:param stoppage_duration: The parameter "stoppage_duration" is used to specify the minimum stopped duration
	"""
	connector_doc=frappe.get_single("Intangles Connector")
	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	epoch = datetime(1970, 1, 1)
	start_time_obj = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
	start_time_difference = start_time_obj - epoch

	end_time_obj = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
	end_time_difference = end_time_obj - epoch

	url = f"{connector_doc.url}/api/v1/vendor/alert_logs/{connector_doc.account_id}/list/{int(start_time_difference.total_seconds() * 1000)}/{int(end_time_difference.total_seconds() * 1000)}?types=stoppage"
	headers = {
	  'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}
	response = requests.request("GET", url, headers=headers)
	if response.ok:
		response=response.json().get('result')
		reqd_data=[]
		if vehicle_no:
			for row in response.get('logs'):
				if row.get('vehicle_plate') in vehicle_no:
					if stoppage_duration:
						if row.get('duration')/60 > stoppage_duration:
							reqd_data.append(row)
					else:
						reqd_data.append(row)
		else:
			for row in response.get('logs'):
				if stoppage_duration:
					if row.get('duration')/60 > stoppage_duration:
						reqd_data.append(row)
				else:
					reqd_data.append(row)
		return reqd_data
	else:
		response.raise_for_status()

@frappe.whitelist()
def get_intangles_fuel_log(start_time=None,end_time=None):

	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	result=fuel_alert_log(
		start_time=start_time,
		end_time=end_time
	)
	while result.get('last_evaluated_timestamp'):
		print(result.get('last_evaluated_timestamp'))
		result=fuel_alert_log(
			start_time=start_time,
			end_time=end_time,
			last_evaluated_timestamp=result.get('last_evaluated_timestamp')
		)

def pull_fuel_alert_logs():
	from datetime import timedelta ,datetime
	to_date_obj = datetime.now()
	from_date_obj = datetime.now()- timedelta(days=7)
	get_intangles_fuel_log(start_time=from_date_obj,end_time=to_date_obj)

def fuel_alert_log(start_time=None, end_time=None, last_evaluated_timestamp=None):
	connector_doc=frappe.get_single("Intangles Connector")
	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")
	
	start_time = get_epoch_datetime(start_time)
	end_time = get_epoch_datetime(end_time)
	if last_evaluated_timestamp:
		evaluated_timestamp = f"?last_evaluated_timestamp={last_evaluated_timestamp}"
	else:
		evaluated_timestamp=""
	url = f"{connector_doc.url}/api/v1/vendor/fuel_alert_logs/{connector_doc.account_id}/list/{start_time}/{end_time}{evaluated_timestamp}"
	headers = {
	  'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}
	response = requests.request("GET", url, headers=headers)
	result={}
	if response.ok and response.status_code==200:
		response=response.json().get('result')
		if response.get('logs'):
			post_fuel_logs(logs=response.get('logs'))
		if response.get('paging',{}).get('isLastPage') == False:
			result['last_evaluated_timestamp']=response.get('paging',{}).get('lastEvaluatedTimestamp')
	return result

def post_fuel_logs(logs):
	url = "https://desk.lnder.in/api/resource/Intangle Fuel Logs"
	for log in logs:
		try:
			payload = json.dumps({
			  "intangles_id": log.get('id'),
			  "latitude": log.get('location',{}).get('latitude'),
			  "longitude": log.get('location',{}).get('longitude'),
			  "vehicle_id": log.get('vehicle_id'),
			  "account_id": log.get('account_id'),
			  "vehicle_plate": log.get('vehicle_plate'),
			  "address": log.get('address'),
			  "type": log.get('type'),
			  "volume": log.get('amount')  ,
			  "start_level_mileage": log.get('start_level_mileage'),
			  "start_odo": log.get('odo'),
			  "end_odo": log.get('end_odo'),
			  "start_level": log.get('start_level'),
			  "end_level": log.get('end_level'),
			  "intangles_timestamp_epoch":log.get('timestamp'),
			  "overall_response": json.dumps(log)
			})
			headers = {
			  'Authorization': 'token 5d86d079564a18a:80e46996b1b9eaf',
			  'Content-Type': 'application/json'
			}
			response = requests.request("POST", url, headers=headers, data=payload)
			print(response)
		except:
			pass


@frappe.whitelist()
def get_vehicle_details_intangles(vehicle_no=None):
	connector_doc=frappe.get_single("Intangles Connector")
	url = f"{connector_doc.url}/api/v1/vendor/vehicle/list/?account_id={connector_doc.account_id}&no_deleted=true"

	payload = {}
	headers = {
	  'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}

	response = requests.request("GET", url, headers=headers, data=payload)
	if response.ok:
		response=response.json().get('result')
		reqd_data=[]
		if vehicle_no and isinstance(vehicle_no,list):
			for row in response.get('vehicles'):
				if row.get('plate') in vehicle_no:
					reqd_data.append(row)
		elif vehicle_no:
			return "Vehicle parameter must be a list"
		else:
			for row in response.get('vehicles'):
				reqd_data.append(row)
		return reqd_data
	else:
		response.raise_for_status()

@frappe.whitelist()
def get_intangles_alert_log(start_time=None,end_time=None):

	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	result=get_alertlogs(
		start_time=start_time,
		end_time=end_time
	)
	print(result)
	while result.get('last_evaluated_timestamp'):
		result=get_alertlogs(
			start_time=start_time,
			end_time=end_time,
			last_evaluated_timestamp=result.get('last_evaluated_timestamp')
		)

@frappe.whitelist()
def get_alertlogs(start_time=None,end_time=None,vehicle_no=None,alert_type=None,last_evaluated_timestamp=None):
	"""
		alert_type = [
			over_speed,
			idling,
			hard_brake,
			stoppage,
			freerun,
			unscheduled_driving,
			over_acc,
			fuel_bhara,
			fuel_chori,
			def_bhara,
			def_chori,
			device_disconnected,
			device_connected
		]
	"""
	if not alert_type:
		alert_type = [
			"over_speed",
			"idling",
			"hard_brake",
			"stoppage",
			"freerun",
			"unscheduled_driving",
			"over_acc",
			"fuel_bhara",
			"fuel_chori",
			"def_bhara",
			"def_chori",
			"device_disconnected",
			"device_connected"
		]
	type_list=",".join(list(set(alert_type)))
	connector_doc=frappe.get_single("Intangles Connector")
	if not start_time and not end_time:
		time_obj = datetime.now()
		start_time = time_obj.strftime("%Y-%m-%d 00:00:00")
		end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")

	start_time = get_epoch_datetime(start_time)
	end_time = get_epoch_datetime(end_time)

	if last_evaluated_timestamp:
		evaluated_timestamp = f"&last_evaluated_timestamp={last_evaluated_timestamp}"
	else:
		evaluated_timestamp=""
	url = f"{connector_doc.url}/api/v1/vendor/alert_logs/{connector_doc.account_id}/list/{start_time}/{end_time}?types={type_list}{evaluated_timestamp}"
	headers = {
	  'vendor-access-token': connector_doc.get_password("vendor_access_token")
	}
	response = requests.request("GET", url, headers=headers)
	result={}
	if response.ok:
		response=response.json().get('result')
		if vehicle_no and isinstance(vehicle_no,list):
			for row in response.get('logs'):
				if row.get('vehicle_plate') in vehicle_no:
					post_alert_logs(logs=row)
		elif not vehicle_no:
			for row in response.get('logs'):
				post_alert_logs(logs=row)
		if response.get('paging',{}).get('isLastPage') == False:
			result['last_evaluated_timestamp']=response.get('paging',{}).get('lastEvaluatedTimestamp')
			return result
	else:
		response.raise_for_status()
		return result

def post_alert_logs(logs):
	url = "https://desk.lnder.in/api/resource/Intangles Vehicle Alert Log"
	for log in logs:
		try:
			payload = json.dumps({
			  "id": log.get('id'),
			  "latitude": log.get('location',{}).get('latitude'),
			  "longitude": log.get('location',{}).get('longitude'),
			  "vehicle_id": log.get('vehicle_id'),
			  "account_id": log.get('account_id'),
			  "vehicle_plate": log.get('vehicle_plate'),
			  "address": log.get('address'),
			  "type": log.get('type'),
			  "overall_response": json.dumps(log)
			})
			headers = {
			  'Authorization': 'token 5d86d079564a18a:80e46996b1b9eaf',
			  'Content-Type': 'application/json'
			}
			response = requests.request("POST", url, headers=headers, data=payload)
			print(response)
		except:
			pass
