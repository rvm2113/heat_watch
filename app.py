from flask import Flask
from flask import jsonify
from flask import request, abort
import psycopg2
from psycopg2 import sql
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
	if(len(response.json()['results']) == 0):
		print("Unable to geocode this address..")
	else:
		latitude = response.json()['results'][0]['geometry']['location']['lat']
		longitude = response.json()['results'][0]['geometry']['location']['lng']
		location = [longitude, latitude]
	return location




#The Kafka producer publishes streams of records/address requests to the topic
#'hazardrequest'
kafka_producer = None
try:
	kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
except Exception as ex:
	print('Unable to connect to Kafka broker ' + str(ex))



s = HazardService()
app = Flask(__name__)






def publish_req_message(name_of_topic, key, val):
	try:
		value_in_bytes = bytes(val, encoding = 'utf-8')
		key_in_bytes = bytes(key, encoding = 'utf-8')
		kafka_producer.send(name_of_topic, key = key_in_bytes, value = value_in_bytes)
		kafka_producer.flush()	
		print('Message published to the Kafka broker')

	except Exception as ex:
		print('Unable to publish message: ' + str(ex))




#Configurations/authentication details for the 
#database/back end are specified
def config(filename = 'database_params.ini', section = 'postgresql'):
	parser = ConfigParser()
	parser.read(filename)
	params_db = {}

	if(parser.has_section(section)):
		params = parser.items(section)
		for param in params:
			params_db[param[0]] = param[1]

	return params_db



#Each address is  displayed with any droughts
#occurring within the surrounding region of that address
@app.route('/addresses/get_by_drought', methods = ['GET'])
def get_addresses_by_drought():
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()

		
		find_addr = 'SELECT coalesce(address_id, 0), coalesce(address, \'no hazard\'), coalesce(improvement, -1), '
		find_addr += 'coalesce(persistent, -1), coalesce(development, -1) FROM address_hazard a1 LEFT JOIN drought d ON ST_COVERS(d.geom, a1.geom);'
		cur.execute(sql.SQL(find_addr))
		init_result = cur.fetchall()
		return jsonify({'addresses:': init_result})
		
		
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')
	empty_arr = [{}]
	return jsonify({'addresses': empty_arr})



#Each address is  displayed with any hazards
#occurring within the surrounding region of that address
@app.route('/addresses/get_by_hazard', methods = ['GET'])
def get_addresses_by_hazard():
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()

		
		find_addr = 'SELECT coalesce(address_id, 0), coalesce(address, \'no hazard\'), coalesce(region, \'no hazard\'), '
		find_addr += 'coalesce(date_hazard, \'September 25, 2018\'), coalesce(type, -1) FROM address_hazard a1 LEFT JOIN international_hazards ih ON ST_COVERS(ih.geom, a1.geom);'
		
		cur.execute(sql.SQL(find_addr))
		init_result = cur.fetchall()
		return jsonify({'addresses:': init_result})
		
		
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')
	empty_arr = [{}]
	return jsonify({'addresses': empty_arr})


#The specified address
#is inserted into the existing relation
#if not already present.
@app.route('/addresses', methods = ['POST'])
def add_address():
	if not request.json or not 'address' in request.json:
		abort(400)
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()


		print(str(request.json))
		address = request.json['address']
		address = address.replace('+', ' ')
		curr_loc = geocode(address)
		

		find_addr = 'SELECT address_id FROM address_hazard WHERE address = '
		find_addr += '\'' + str(request.json['address']) + '\';'
		cur.execute(sql.SQL(find_addr))
		init_result = cur.fetchall()
		if(len(init_result) == 0):
			full_insert = 'INSERT INTO address_hazard VALUES(DEFAULT, '
			full_insert += '\'' + address + '\''+ ' , '
			full_insert += 'ST_GeomFromText(\'POINT'
			full_insert += ('(' + str(curr_loc[0]) + ' ' + str(curr_loc[1]) + ')\'));')
			cur.execute(sql.SQL(full_insert))
			cur.execute(sql.SQL(find_addr))
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
	empty_arr = [{}]
	return jsonify({'addresses': empty_arr})



#The centroid is returned
#for all geopolygons with the specified temperature range
#and/or the number of vertices.
@app.route('/global_surface_temperatures/<int:temperature_min>', methods=['GET'])
@app.route('/global_surface_temperatures/<int:temperature_min>/<int:temperature_max>', methods=['GET'])
@app.route('/global_surface_temperatures/<int:temperature_min>/<int:temperature_max>/<int:num_vertices>', methods=['GET'])
def get_temperatures(num_vertices = -1, temperature_min = -1, temperature_max = -1):
	conn = None
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()
		full_query = 'SELECT ST_AsText(ST_Centroid(geom)) FROM global_surface_temperatures '
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

		cur.execute(sql.SQL(full_query))
		temperature_all = cur.fetchall()
		return jsonify({'temperatures': temperature_all})
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')
	empty_arr = [{}]
	return jsonify({'temperatures': empty_arr})
	



#All droughts within the surrrounding region
#are returned for the specified address
@app.route("/droughts/<string:address>", methods=['GET'])
def get_drought_stats(address = None):
	dynamic_id = None
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()


		address = address.replace('+', ' ')
		curr_loc = geocode(address)


		find_addr = 'SELECT address_id FROM address_hazard WHERE address = '
		find_addr += '\'' + str(address) + '\';'
		cur.execute(sql.SQL(find_addr))
		init_result = cur.fetchall()
		if(len(init_result) == 0):
			full_insert = 'INSERT INTO address_hazard VALUES(DEFAULT, '
			full_insert += '\'' + address + '\''
			full_insert += ',ST_GeomFromText(\'POINT'
			full_insert += ('(' + str(curr_loc[0]) + ' ' + str(curr_loc[1]) + ')\'));')

			cur.execute(sql.SQL(full_insert))
			cur.execute(sql.SQL(find_addr))
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
	publish_req_message('hazardrequest', 'address' + str(dynamic_id) , address)
	return s.get_drought_stats(dynamic_id)




#All international hazards within the surrounding region
#of a specified address are returned.
@app.route("/international_hazards/<string:address>", methods=['GET'])
def get_hazards(address = None):
	dynamic_id = None
	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()

		address = address.replace('+', ' ')
		curr_loc = geocode(address)


		find_addr = 'SELECT address_id FROM address_hazard WHERE address = '
		find_addr += '\'' + str(address) + '\';'
		cur.execute(sql.SQL(find_addr))
		init_result = cur.fetchall()
		if(len(init_result) == 0):
			full_insert = 'INSERT INTO address_hazard VALUES(DEFAULT, '
			full_insert += '\'' + address + '\''
			full_insert += ',ST_GeomFromText(\'POINT'
			full_insert += ('(' + str(curr_loc[0]) + ' ' + str(curr_loc[1]) + ')\'));')
			cur.execute(sql.SQL(full_insert))
			cur.execute(sql.SQL(find_addr))
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

	publish_req_message('hazardrequest', 'address' + str(dynamic_id) , address)
	return s.get_hazards(dynamic_id)








	


if __name__ == '__main__':
	app.run(debug=True)
