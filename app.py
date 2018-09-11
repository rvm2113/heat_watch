from flask import Flask
from flask import jsonify
import psycopg2
import pandas as pd
import requests
from configparser import ConfigParser
from kafka import KafkaProducer
from consumer_service import HazardService
import copy
import sys

# The Google Geocoding API
	# is used to convert the given address to 
	# a list of longitude of latitude.
def geocode(address):
	api_key = 'AIzaSyBPP5HOiHWYZxK39bI6PK43tNPlaGYzlQ4'
	query_list = []
	query = {} 
	query['address'] = address
	query['key'] = api_key

	try:
		url = "https://maps.googleapis.com/maps/api/geocode/json"
		response = requests.get(url, query)
	except requests.exceptions.RequestException as error: 
		print(error)
		sys.exit(1)

	location = None
	print("RESP: " + str(response))
	if(len(response.json()['results']) == 0):
		print("Unable to geocode this address..")
	else:
		latitude = response.json()['results'][0]['geometry']['location']['lat']
		longitude = response.json()['results'][0]['geometry']['location']['lng']
		location = [longitude, latitude]
	return location




# consumer = KafkaConsumer('my-topic', group_id='view' bootstrap_servers=['0.0.0.0:9092'])
kafka_producer = None
try:
	kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
except Exception as ex:
	print('Unable to connect to Kafka broker ' + str(ex))



s = HazardService()
app = Flask(__name__)

# @app.route("/")
# def hello():
#     return "Hello World!"





#!flask/bin/python


def publish_req_message(name_of_topic, key, val):
	try:
		value_in_bytes = bytes(val, encoding = 'utf-8')
		key_in_bytes = bytes(key, encoding = 'utf-8')
		kafka_producer.send(name_of_topic, key = key_in_bytes, value = value_in_bytes)
		kafka_producer.flush()	
		print('Message published to the Kafka broker')

	except Exception as ex:
		print('Unable to publish message: ' + str(ex))



def config(filename = 'database_params.ini', section = 'postgresql'):
	parser = ConfigParser()
	parser.read(filename)
	params_db = {}

	if(parser.has_section(section)):
		params = parser.items(section)
		for param in params:
			params_db[param[0]] = param[1]

	return params_db


@app.route('/global_surface_temperatures/<int:temperature_min>', methods=['GET'])
@app.route('/global_surface_temperatures/<int:temperature_min>/<int:temperature_max>', methods=['GET'])
@app.route('/global_surface_temperatures/<int:temperature_min>/<int:temperature_max>/<int:num_vertices>', methods=['GET'])
def get_temperatures(num_vertices = -1, temperature_min = -1, temperature_max = -1):



	# publish_req_message('hazardrequest', )
	conn = None
	try:
		params = config()
		print("CONFIG SUCCESSFUL..")
		print(params)
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		print("CONN: "  + str(conn))
		cur = conn.cursor()
		full_query = 'SELECT temperature FROM global_surface_temperatures '
		selection = False
		first_clause = True
		if(num_vertices != -1):
			if(not(selection)):
				full_query += ' WHERE '
				selection = True


			if(not(first_clause)):
				full_query += 'AND'
			else:
				first_clause = False
			full_query += (' number_of_vertices = ' + str(num_vertices))
		

		if(temperature_min != -1):
			if(not(selection)):
				full_query += ' WHERE '
				selection = True


			if(not(first_clause)):
				full_query += 'AND'
			else:
				first_clause = False
			full_query += (' temperature >= ' + str(temperature_min))

		if(temperature_max != -1):
			if(not(selection)):
				full_query += ' WHERE '
				selection = True


			if(not(first_clause)):
				full_query += 'AND'
			else:
				first_clause = False
			full_query += (' temperature <= ' + str(temperature_max))

		full_query +=';'

		print("FULL QUERY: " + full_query)
		cur.execute(full_query)
		temperature_all = cur.fetchall()
		print("RESULT: " + str(temperature_all))
		return jsonify({'temperatures': temperature_all})

		# cur.execute('DROP TABLE IF EXISTS dairy ;')
		# cur.execute('CREATE TABLE dairy(year integer NOT NULL, retail_cost integer NOT NULL, farm_value integer NOT NULL, farm_to_retail_spread integer NOT NULL, farm_value_share integer NOT NULL);')
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')

    # task = [task for task in tasks if task['id'] == task_id]
    # if len(task) == 0:
    #     abort(404)

	empty_arr = [{}]
	return jsonify({'temperatures': empty_arr})
	


@app.route("/international_hazards/<string:address>", methods=['GET'])
		



@app.route("/international_hazards/<string:address>", methods=['GET'])
def get_hazards(address = None):
	dynamic_id = None
	try:
		params = config()
		print("CONFIG SUCCESSFUL..")
		print(params)
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		print("CONN: "  + str(conn))
		cur = conn.cursor()

		find_addr = 'SELECT address_id FROM address_hazard WHERE address = '
		find_addr += '\'' + str(address) + '\';'
		cur.execute(find_addr)
		init_result = cur.fetchall()
		if(len(init_result) == 0):
			full_insert = 'INSERT INTO address_hazard VALUES(DEFAULT, '
			full_insert += '\'' + address + '\''+ ' , '  + 'NULL);'
			cur.execute(full_insert)
			cur.execute(find_addr)
			addr_result = cur.fetchall()
			dynamic_id = addr_result[0][0]
		else:
			dynamic_id = init_result[0][0]
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')

	print('DYNAMIC ID: ' + str(dynamic_id))

	publish_req_message('hazardrequest', 'address' + str(dynamic_id) , address)
	# loc = geocode(address)
	return s.get_hazards(dynamic_id)








	


if __name__ == '__main__':
	loc = geocode('614+Concord+Road%2C+Ridgewood%2C+NJ')
	print('Attempt: ' + str(loc))
	# init_flask()
	# kafka_producer_connect()

	app.run(debug=True)
