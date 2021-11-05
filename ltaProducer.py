import configparser
import csv
import json
import httplib2
from kafka import KafkaProducer
import time
import os

def read_config(home_folder):
	config = configparser.ConfigParser()
	config.read("{}tasks/config.ini".format(home_folder))

	config_all = {}
	config_all['API_KEY'] = config['API']['API_KEY']
	config_all['BOOTSTRAP_SERVER'] = config['KAFKA']['BOOTSTRAP_SERVER']
	config_all['TOPIC_NAME'] = config['KAFKA']['TOPIC_NAME']

	return config_all


def fetch_bus_codes(home_folder):
	bus_stop_codes = []
	with open('{}tasks/bus_interchange_code.csv'.format(home_folder),'r') as csv_file:
		csv_reader = csv.reader(csv_file)
		next(csv_reader)
		for row in csv_reader:
			bus_stop_codes.append(row[1])
	return bus_stop_codes


def generate_stream(**kwargs):
	bus_stop_codes = fetch_bus_codes(home_folder)
	config_all = read_config(home_folder)
	headers = {'AccountKey': config_all['API_KEY'], 'accept': 'application/json'}
	method = 'GET'
	body = 'abc'

	producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

	for i in bus_stop_codes:
		url = 'http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2?BusStopCode={}'.format(i)
		h = httplib2.Http()
		response, content = h.request(url,method,body,headers)
		jsonObj = json.loads(content)
		json_str = json.dumps(jsonObj, indent=4)
		print(i)
		print(json_str)
		producer.send(config_all['TOPIC_NAME'],json_str)

	producer.close()

if __name__ == "__main__":
	home_folder = <LOCATION_OF_YOUR_DAG>
	time.sleep(2)
	generate_stream()