import frappe
import json
from datetime import datetime
import requests

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