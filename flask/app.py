from flask import Flask, render_template, request, redirect
from pymongo import MongoClient

app = Flask(__name__)

def connect_mongo(bus_stop_code):
	try:
		conn = MongoClient('localhost',27017)
		print('Connected to MongoDB')
	except:
		print('Could not connect to MongoDB')

	db = conn.lta_bus_arrival
	collection = db.arrival_time

	cursor_list = list(collection.find({bus_stop_code: {"$exists" : True}}))
	if len(list(cursor_list)) != 0:
		output = cursor_list[0][bus_stop_code]
	else:
		output = {}

	return output

@app.route("/index", methods=['GET','POST'])
def hello():
	if request.method == "POST":
		bus_stop_code = request.form['bus_stop']
		data = connect_mongo(bus_stop_code)
		return render_template('index.html',data=data, bus_stop_code = bus_stop_code)
	return render_template('index.html')

if __name__ == "__main__":
  	app.run()