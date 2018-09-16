import psycopg2
import pandas as pd
import json
import shapefile
import shapely
from psycopg2 import sql
from shapely.geometry import shape
from configparser import ConfigParser
from bs4 import BeautifulSoup
import zipfile
import requests
import io
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO




#All authentication credentials
#for the database are specified
def config(filename = 'database_params.ini', section = 'postgresql'):
	parser = ConfigParser()
	parser.read(filename)
	params_db = {}

	if(parser.has_section(section)):
		params = parser.items(section)
		for param in params:
			params_db[param[0]] = param[1]

	return params_db


#A cron job is scheduled in order to populate
#the backend of the web service with the latest spatial data
#regarding hazards, droughts, and global surface temperatures.
def read_shp_file(number_of_previous_days, url , purpose):

	
	page = requests.get(url).text
	soup = BeautifulSoup(page, 'html.parser')
	ext = '.shp.zip'
	ext_second = '.zip'
	ext_forbidden = '.tif.zip'
	all_files = [node.get('href') for node in soup.find_all('a') if node.get('href') is not None and (node.get('href').endswith(ext) or node.get('href').endswith(ext_second)
		and not(node.get('href').endswith(ext_forbidden)))]
	print(str(all_files))
	if(purpose == 'global_hazards'):	
		all_files = [(url + curr_file[0:]) for curr_file in all_files if 'hazards' in curr_file]
	else:
		all_files = [('http' + curr_file[3:]) for curr_file in all_files]

	


	

	try:
		params = config()
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		cur = conn.cursor()
		if(purpose == 'global_hazards'):
			cur.execute(sql.SQL("DROP TABLE international_hazards;"))
			cur.execute(sql.SQL("CREATE TABLE international_hazards( region varchar(20) NOT NULL, date_hazard date NOT NULL, type integer NOT NULL, geom geometry);"))
		elif(purpose == 'global_surface_temperatures'):
			cur.execute(sql.SQL("DROP TABLE global_surface_temperatures;"))
			cur.execute(sql.SQL("CREATE TABLE global_surface_temperatures(temperature integer NOT NULL, number_of_vertices integer NOT NULL, unit varchar(20) NOT NULL, geom geometry(Polygon));"))
		elif(purpose == 'drought'):
			cur.execute(sql.SQL("DROP TABLE drought;"))
			cur.execute(sql.SQL("CREATE TABLE drought(improvement integer NOT NULL, persistent integer NOT NULL, development integer NOT NULL, date_drought date NOT NULL, removal integer NOT NULL, geom geometry);"))

		
	



		for file in all_files:
			

			
			result = requests.get(file)
			z = zipfile.ZipFile(io.BytesIO(result.content))

			all_types = [y for y in sorted(z.namelist()) for ending in ['dbf', 'prj', 'shp', 'shx'] if y.endswith(ending)]  
			
			dbf, prj, shp, shx = [io.BytesIO(z.read(types)) for types in all_types]
			
			
			try:
				r = shapefile.Reader(shp=shp, shx=shx, prj = prj, dbf=dbf)
			

				print(r.numRecords)
				geom = []
				field_names = [properties[0] for properties in r.fields[1:]]  

				

				for curr_row in r.shapeRecords():  
					geom.append(shape(curr_row.shape.__geo_interface__))

					if(purpose == 'global_hazards'):
						
						region = curr_row.record[1]
						date = curr_row.record[2]
						type_category = curr_row.record[3]
						geometric_shape = curr_row.shape.__geo_interface__	
						cur.execute(sql.SQL("INSERT INTO international_hazards VALUES (%s, %s, %s, ST_GeomFromText(%s));"),
							(region, date, type_category, shape(curr_row.shape.__geo_interface__).wkt))

					elif(purpose == 'global_surface_temperatures'):
						
						temp = curr_row.record[0]
						num_vertices = curr_row.record[1]
						unit_c = "Celsius"
						geometric_shape = curr_row.shape.__geo_interface__
						cur.execute(sql.SQL("INSERT INTO global_surface_temperatures VALUES (%s, %s, %s, ST_GeomFromText(%s));"),
							(temp, num_vertices, unit_c, shape(curr_row.shape.__geo_interface__).wkt))
					elif(purpose == 'drought'):
						improvement = curr_row.record[0]
						persistent = curr_row.record[1]
						development = curr_row.record[2]
						date_drought = curr_row.record[3]
						removal = curr_row.record[5]

						geometric_shape = curr_row.shape.__geo_interface__

						cur.execute(sql.SQL("INSERT INTO drought VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s));"),
							(improvement, persistent, development, date_drought, removal, shape(geometric_shape).wkt))



			except Exception as e:
				print("ERROR: " + str(e))
				continue

	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')



if __name__=='__main__':
	url = "http://www.cpc.ncep.noaa.gov/products/GIS/GIS_DATA/sst_oiv2/"
	url_second = "http://ftp.cpc.ncep.noaa.gov/GIS/int_hazards/"
	url_third = "http://www.cpc.ncep.noaa.gov/products/GIS/GIS_DATA/droughtlook/"

	read_shp_file(3, url, purpose = 'global_surface_temperatures')
	read_shp_file(3, url_second, purpose = 'global_hazards')
	read_shp_file(3, url_third, purpose = 'drought')
	