from flask import Flask
from flask import jsonify
import psycopg2
import pandas as pd
import requests
from configparser import ConfigParser
from kafka import KafkaProducer, KafkaConsumer
import copy
import sys



#
#curl -XGET 'http://127.0.0.1:5000/international_hazards/11+Th+Street%2C+Central+Business+Dis%2C+Abuja%2C+Nigeria'
#bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic hazardrequest --config retention.ms=1000
#./kafka-topics.sh --zookeeper localhost:2181 --alter --topic hazardrequest --delete-config retention.ms
#curl -XGET 'http://127.0.0.1:5000/droughts/3199+Juniper+St+San+Diego%2C+CA+92104'
#./kafka-server-start.sh ../config/server.properties
#/usr/local
#
#



#The Kafka consumer subscribes to streams of records/address requests from the topic
#'hazardrequest'
consumer = KafkaConsumer('hazardrequest', group_id='view',bootstrap_servers=['0.0.0.0:9092'])

class HazardService(object):

	def __init__(self):
		pass


	def config(self, filename = 'database_params.ini', section = 'postgresql'):
		parser = ConfigParser()
		parser.read(filename)
		params_db = {}

		if(parser.has_section(section)):
			params = parser.items(section)
			for param in params:
				params_db[param[0]] = param[1]

		return params_db

	# The Google Geocoding API
	# is used to convert the given address to 
	# a list of longitude of latitude.
	def geocode(self, address):
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



	#Once the address/request is obtained
	#through the Kafka consumer, information
	#for each drought that contains the provided
	#address is returned.
	def get_drought_stats(self, id_tmp):
		for address in consumer:
			address_id = 'address' + str(id_tmp)
			address_key = address.key.decode('UTF-8')
			address_value = address.value.decode('UTF-8')
			if(address_key == address_id):
				address_key = address_key.replace('+', ' ')
				curr_loc = self.geocode(address_value)
				conn = None
				try:
					params = self.config()
					conn = psycopg2.connect(**params)
					conn.autocommit = True
					cur = conn.cursor()
					full_query = 'SELECT  improvement, persistent, development FROM drought WHERE ST_COVERS(geom, '
					full_query += 'ST_GeomFromText(\'POINT'
					full_query += ('(' + str(curr_loc[0]) + ' ' + str(curr_loc[1]) + ')\'));')
					cur.execute(sql.SQL(full_query))
					hazards_all = cur.fetchall()
					return jsonify({'hazards': hazards_all})
				except(Exception, psycopg2.DatabaseError) as error:
					print("ERROR: " + str(error))
				finally:
					if conn is not None:
						conn.close()
						print('Database connection closed.')
				break
		empty_arr = [{}]
		return jsonify({'drought': empty_arr})


	#Once the address/request is obtained
	#through the Kafka consumer, information
	#for each hazard that contains the provided
	#address is returned.
	def get_hazards(self, id_tmp):

		for address in consumer:
			address_id = 'address' + str(id_tmp)
			address_key = address.key.decode('UTF-8')
			address_value = address.value.decode('UTF-8')
			if(address_key == address_id):
				address_key = address_key.replace('+', ' ')
				curr_loc = self.geocode(address_value)
				conn = None
				try:
					params = self.config()
					conn = psycopg2.connect(**params)
					conn.autocommit = True
					cur = conn.cursor()
					full_query = 'SELECT  region, date_hazard, type FROM international_hazards WHERE ST_COVERS(geom, '
					full_query += 'ST_GeomFromText(\'POINT'
					full_query += ('(' + str(curr_loc[0]) + ' ' + str(curr_loc[1]) + ')\'));')
					cur.execute(sql.SQL(full_query))
					hazards_all = cur.fetchall()
					return jsonify({'hazards': hazards_all})
				except(Exception, psycopg2.DatabaseError) as error:
					print("ERROR: " + str(error))
				finally:
					if conn is not None:
						conn.close()
						print('Database connection closed.')
				break
			

		empty_arr = [{}]
		return jsonify({'hazards': empty_arr})