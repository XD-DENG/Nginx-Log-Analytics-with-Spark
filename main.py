
import sys   # read argument from command line
import requests   # get info from API
import json  # decode json data
from prettytable import PrettyTable
from time import sleep
import re # to detect specific string from the log. This is for detecting failed traffic

# the first argument will be file name by default, so even if we only enter 3 arguments, there will be four.
# this is also why we start index from 1 instead of 0.

raw_data_directory = str(sys.argv[1])       # "/"var/log/nginx/access.*"

print("Building the Spark Context--------------------------")
from pyspark import SparkContext
sc = SparkContext(appName="Nginx Log Analytics - IP Address Analytics")

print("Loading the data------------------------------------")
raw_content = sc.textFile(raw_data_directory)


print("Cleaning the data---------------------------------")
# help detect failed traffic by matching '403' or '404' in the log
re_detect_failed_traffic = re.compile('.+ 404 .+')
re_detect_rejected_traffic = re.compile('.+ 403 .+')
content = raw_content.map(lambda x: (x.split(' - ')[0], bool(re_detect_failed_traffic.match(x)), bool(re_detect_rejected_traffic.match(x)), x.split(' - ')[0].split('.')[0] in ['10', '172', '192']))  # Get all the IP address records in the log files, and check if the traffic failed

print(str(content.count()) + " rows of log records are loaded.")

print("Preparing for IP Information---------------------------------")

unique_IP_count = content.countByKey()


threshold_to_display = 3
# Only show the request the Geo info for IP visited for more than n times

# Get the location and other detailed information from API.
ip_to_check = unique_IP_count.keys()
for ip in ip_to_check:
    
    bool_private_ip = content.filter(lambda x:x[0] == ip).map(lambda x:x[3]).take(1)[0]
    
    if unique_IP_count[ip] > threshold_to_display:
        print "Getting IP Geo info & Failure Ratio for " + ip
        temp = requests.get('http://ip-api.com/json/' + ip)
        sleep(0.5) # this is to prevent being banned since ip-api has restriction to request no more than 150 times per minuete
        ip_info = json.loads(temp.text)
        
        failure_times = sum(content.filter(lambda x:x[0] == ip).map(lambda x:x[1]).collect())
        failure_ratio = float(failure_times)/unique_IP_count[ip]
        failure_ratio = str(round(failure_ratio, 4) * 100) + "%"
        
        rejected_times = sum(content.filter(lambda x:x[0] == ip).map(lambda x:x[2]).collect())
        rejected_ratio = float(rejected_times)/unique_IP_count[ip]
        rejected_ratio = str(round(rejected_ratio, 4) * 100) + "%"
        
        if bool_private_ip == False:
            unique_IP_count[ip] = {'count':unique_IP_count[ip], 'private_ip':'-', 'ratio.404':failure_ratio, 'ratio.403':rejected_ratio, 'country':ip_info['country'], 'region':ip_info['region'], 'city':ip_info['city'], 'org':ip_info['org']}
        else:
            unique_IP_count[ip] = {'count':unique_IP_count[ip], 'private_ip':'Y', 'ratio.404':failure_ratio, 'ratio.403':rejected_ratio, 'country':'NA', 'region':'NA', 'city':'NA', 'org':'NA'}
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
field_names = ["IP", "Count", "Private IP", "404 - Ratio", "403 - Ratio", "Country", "Region", "City", "Org"]
t = PrettyTable(field_names)

for k in range(len(sorted_request_count_of_IP_to_display)):
    max_count = sorted_request_count_of_IP_to_display.pop()
    IP_to_print_at_this_round = dict_of_IP_and_COUNT.keys()[dict_of_IP_and_COUNT.values().index(max_count)]
    del dict_of_IP_and_COUNT[IP_to_print_at_this_round]
    temp = to_display[IP_to_print_at_this_round]
    t.add_row([IP_to_print_at_this_round, temp['count'], temp['private_ip'], temp['ratio.404'], temp['ratio.403'], temp['country'], temp['region'], temp['city'], temp["org"]])

print t
print "(Only IP addresses visited more than " + str(threshold_to_display) + " times are displayed.)"
