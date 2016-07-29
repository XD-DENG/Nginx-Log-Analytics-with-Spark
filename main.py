
import sys   # read argument from command line
import requests   # get info from API
import json  # decode json data
from prettytable import PrettyTable


# the first argument will be file name by default, so even if we only enter 3 arguments, there will be four.
# this is also why we start index from 1 instead of 0.

raw_data_directory = str(sys.argv[1])       # "/"var/log/nginx/access.*"

print("Building the Spark Context--------------------------")
from pyspark import SparkContext
sc = SparkContext(appName="Nginx Log Analytics - IP Address Analytics")

print("Loading the data------------------------------------")
raw_content = sc.textFile(raw_data_directory)


print("Cleaning the data---------------------------------")
content = raw_content.map(lambda x: (x.split(' - ')[0], x))  # Get all the IP address records in the log files


print(str(content.count()) + " rows of log records are loaded.")

print("Preparing for IP Information---------------------------------")

unique_IP_count = content.countByKey()


# Get the location and other detailed information from http://ipinfo.io API.
ip_to_check = unique_IP_count.keys()
for ip in ip_to_check:
    temp = requests.get('http://ip-api.com/json/' + ip)
    ip_info = json.loads(temp.text)
    unique_IP_count[ip] = {'count':unique_IP_count[ip], 'country':ip_info['country'], 'region':ip_info['region'], 'city':ip_info['city'], 'org':ip_info['org']}


# Only show the IP visisted for more than n times
threshold_to_display = 10

to_display = {}
for ip in ip_to_check:
	if unique_IP_count[ip]['count'] > threshold_to_display:
		to_display[ip] = unique_IP_count[ip]


# Print Out important information.
field_names = ["IP", "Count", "Country", "Region", "City", "Org"]

t = PrettyTable(field_names)

for k in to_display.keys():
    t.add_row([k, to_display[k]['count'], to_display[k]['country'], to_display[k]['region'], to_display[k]['city'], to_display[k]["org"]])

print t
print "(Only IP addresses visited more than " + str(threshold_to_display) + " times are displayed.)"
