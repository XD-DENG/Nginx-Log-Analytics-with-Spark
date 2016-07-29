
import sys   # read argument from command line
import requests   # get info from API
import json  # decode json data
from prettytable import PrettyTable
from time import sleep


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


threshold_to_display = 5
# Only show the request the Geo info for IP visited for more than n times

# Get the location and other detailed information from API.
ip_to_check = unique_IP_count.keys()
for ip in ip_to_check:
    if unique_IP_count[ip] > threshold_to_display:
        print "Getting IP Geo info for " + ip
        temp = requests.get('http://ip-api.com/json/' + ip)
        sleep(0.5) # this is to prevent being banned since ip-api has restriction to request no more than 150 times per minuete
        ip_info = json.loads(temp.text)
        unique_IP_count[ip] = {'count':unique_IP_count[ip], 'country':ip_info['country'], 'region':ip_info['region'], 'city':ip_info['city'], 'org':ip_info['org']}
    else:
        unique_IP_count[ip] = {'count':unique_IP_count[ip]}


# Only show the IP visisted for more than n times
to_display = {}
for ip in ip_to_check:
    if unique_IP_count[ip]['count'] > threshold_to_display:
        to_display[ip] = unique_IP_count[ip]


# Print Out important information. ------------------

# preparation - 1
# Extract the request count information
# This is for printing important information by the order of times of requests
request_count_of_IP_to_display = []
for k in to_display.keys():
    request_count_of_IP_to_display.insert(0, to_display[k]['count'])
sorted_request_count_of_IP_to_display = sorted(request_count_of_IP_to_display)
    

# preparation - 2
# a dict of IP and request counts
# it also helps print the important information by the order ot the request count
dict_of_IP_and_COUNT = {}
for k in to_display.keys():
    dict_of_IP_and_COUNT[k] = to_display[k]['count']


# start to print out the table of important information
field_names = ["IP", "Count", "Country", "Region", "City", "Org"]
t = PrettyTable(field_names)

for k in range(len(sorted_request_count_of_IP_to_display)):
    max_count = sorted_request_count_of_IP_to_display.pop()
    IP_to_print_at_this_round = dict_of_IP_and_COUNT.keys()[dict_of_IP_and_COUNT.values().index(max_count)]
    del dict_of_IP_and_COUNT[IP_to_print_at_this_round]
    temp = to_display[IP_to_print_at_this_round]
    t.add_row([IP_to_print_at_this_round, temp['count'], temp['country'], temp['region'], temp['city'], temp["org"]])

print t
print "(Only IP addresses visited more than " + str(threshold_to_display) + " times are displayed.)"
