from flask import Flask
from flask import jsonify
import psycopg2
import pandas as pd

from configparser import ConfigParser
import copy

# The Google Geocoding API
	# is used to convert the given address to 
	# a list of longitude of latitude.
def geocode(address):
	api_key = 'AIzaSyBxQz8JecvI5q2RyaDLArP-WfitxwQFo8k'
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


app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"





#!flask/bin/python




def config(filename = 'database_params.ini', section = 'postgresql'):
	parser = ConfigParser()
	parser.read(filename)
	params_db = {}

	if(parser.has_section(section)):
		params = parser.items(section)
		for param in params:
			params_db[param[0]] = param[1]

	return params_db




	



@app.route("/international_hazards/<string:address>", methods=['GET'])
def get_hazards(address = None):
	print('FUNCTION HAZARDS CALLED..')
	curr_loc = geocode(address)
	print("CURR LOC: " + str(curr_loc))
	conn = None
	try:
		params = config()
		print("CONFIG SUCCESSFUL..")
		print(params)
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()
		full_query = 'SELECT  Region, Date_, Type FROM international_hazards WHERE ST_COVERS(geom, '
		full_query += 'ST_GeogFromText(SRID=4326;POINT'
		full_query += ('(' + curr_loc[x] + ',' + curr_loc[y] + '));')












	


if __name__ == '__main__':
    app.run(debug=True)
