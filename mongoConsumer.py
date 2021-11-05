import configparser
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import math
from pymongo import MongoClient

def read_config(home_folder):
	config = configparser.ConfigParser()
	config.read("{}/tasks/config.ini".format(home_folder))

	config_all = {}
	config_all['HOST'] = config['MONGODB']['HOST']
	config_all['PORT'] = config['MONGODB']['PORT']
	config_all['DATABASE'] = config['MONGODB']['DATABASE']
	config_all['COLLECTION1'] = config['MONGODB']['COLLECTION1']
	config_all['COLLECTION2'] = config['MONGODB']['COLLECTION2']

	return config_all

def read_messages():
	consumer = KafkaConsumer('lta_producer',
							bootstrap_servers='localhost:9092',
							value_deserializer=lambda x: json.loads(x.decode('utf-8')),
							auto_offset_reset='latest',
							consumer_timeout_ms=1000)

	for message in consumer:
		get_values(message)


def get_values(message):
	output = {}
	values = message[6]
	message_json = json.loads(values)
	bus_stop = message_json['BusStopCode']
	output[bus_stop] = {}
	b={}
	for service in message_json['Services']:
		c = []
		bus_service = service['ServiceNo']

		arrival = service['NextBus']['EstimatedArrival'].split("+")[0]
		if arrival != "":
			arrival_time = datetime.strptime(arrival, '%Y-%m-%dT%H:%M:%S')
			time_diff = math.ceil((arrival_time-datetime.now()).total_seconds()/60)
			if time_diff < 0:
				time_diff = 0
			capacity = service['NextBus']['Load']
			c.append([time_diff,capacity])

		arrival2 = service['NextBus2']['EstimatedArrival'].split("+")[0]
		if arrival2 != "":
			arrival_time2 = datetime.strptime(arrival2, '%Y-%m-%dT%H:%M:%S')
			time_diff2 = math.ceil((arrival_time2-datetime.now()).total_seconds()/60)
			if time_diff2 < 0:
				time_diff2 = 0
			capacity2 = service['NextBus2']['Load']
			c.append([time_diff2,capacity2])

		arrival3 = service['NextBus3']['EstimatedArrival'].split("+")[0]
		if arrival3 != "":
			arrival_time3 = datetime.strptime(arrival3, '%Y-%m-%dT%H:%M:%S')
			time_diff3 =  math.ceil((arrival_time3-datetime.now()).total_seconds()/60)
			if time_diff3 < 0:
				time_diff3 = 0
			capacity3 = service['NextBus3']['Load']
			c.append([time_diff3,capacity3])

		b[bus_service] = c

	output[bus_stop] = b
	for key,value in output.items():
		mongodb_dump[key] = value

def update_mongo(mongodb_dump):

	#config_all = read_config()
	##host = config_all['HOST']
	#port = int(config_all['PORT'])
	#database = config_all['DATABASE']
	#collection1 = config_all['COLLECTION1']

	try:
		conn = MongoClient()
		print('Connected to MongoDB')
	except:
		print('Could not connect to MongoDB')

	db = conn.lta_bus_arrival
	print('Connected to Database')
	collection = db.arrival_time
	print('Connected to Collection')

	for key,value in mongodb_dump.items():
		document = {}
		document[key] = value
		doc = collection.find_one({key : { "$exists" : True }})
		if doc != None:
			collection.update_one({key:doc[key]},{"$set": {key:value}})
		else:
			collection.insert_one({key:value})
	print('Update complete')

if __name__ == "__main__":
	home_folder = <LOCATION_OF_YOUR_DAG>
	mongodb_dump = {}
	print('Starting Consumer')
	timeout = time.time() + 10
	while True:
		if time.time() > timeout:
			print('Closing Consumer')
			break
		else:
			read_messages()

	update_mongo(mongodb_dump)

