# Analyze Access Log of Nginx with Apache Spark (PySpark)

## Introduction

**Nginx** is a great web server, it's easy to use while providing rich features, like logging. As a data analyst, I'm more interested in finding some useful information from the logs.

The log files of Nginx can be found under path */var/log/nginx/access.log*. The standard format looks like

```
171.18.**.*** - - [19/Aug/2016:08:00:26 +0000] "GET /index.html" 200 1137450 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
```

In order, it can provide the IP address of visitor, the visiting time, contents visited, HTTP status code, size, header details. Quite rich information. I'm mainly interested in the IP, how many times each IP visited, if the traffic is failed or rejected, and more detailed information of each IP (external data will be needed for this).

## How to Use

```{bash}
./bin/spark-submit main.py "/var/log/nginx/access.*"
```
