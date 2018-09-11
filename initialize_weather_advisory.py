import psycopg2
import pandas as pd
import json
import shapefile
import shapely
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


def config(filename = 'database_params.ini', section = 'postgresql'):
	parser = ConfigParser()
	parser.read(filename)
	params_db = {}

	if(parser.has_section(section)):
		params = parser.items(section)
		for param in params:
			params_db[param[0]] = param[1]

	return params_db



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

	


	# u = urllib2.urlopen(url)
	# meta = u.info()

	try:
		params = config()
		print("CONFIG SUCCESSFUL..")
		print(params)
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		print("CONN: "  + str(conn))
		cur = conn.cursor()
		if(purpose == 'global_hazards'):
			cur.execute("DROP TABLE international_hazards;")
			cur.execute("CREATE TABLE international_hazards( region varchar(20) NOT NULL, date_hazard date NOT NULL, type integer NOT NULL, geom geometry);")
		elif(purpose == 'global_surface_temperatures'):
			cur.execute("DROP TABLE global_surface_temperatures;")
			cur.execute("CREATE TABLE global_surface_temperatures(temperature integer NOT NULL, number_of_vertices integer NOT NULL, unit varchar(20) NOT NULL, geom geometry(Polygon));")
		elif(purpose == 'drought'):
			cur.execute("DROP TABLE drought;")
			cur.execute("CREATE TABLE drought(improvement integer NOT NULL, persistent integer NOT NULL, development integer NOT NULL, date_drought date NOT NULL, removal integer NOT NULL, geom geometry);")

		print('ALL FILES: ' + str(all_files))
	



		for file in all_files:
			

			print("FILE: " + str(file))
			result = requests.get(file)
			z = zipfile.ZipFile(io.BytesIO(result.content))

			all_types = [y for y in sorted(z.namelist()) for ending in ['dbf', 'prj', 'shp', 'shx'] if y.endswith(ending)]  
			print("ALL TYPES: " + str(all_types))
			dbf, prj, shp, shx = [io.BytesIO(z.read(types)) for types in all_types]
			
			# r = shapefile.Reader(z.)
			try:
				r = shapefile.Reader(shp=shp, shx=shx, prj = prj, dbf=dbf)
			

				print(r.numRecords)
				geom = []
				field_names = [properties[0] for properties in r.fields[1:]]  

				print('FIELDS..')

				for curr_row in r.shapeRecords():  
					geom.append(shape(curr_row.shape.__geo_interface__))

					if(purpose == 'global_hazards'):
						print("CURR ROW: " + str(curr_row.record))
						region = curr_row.record[1]
						date = curr_row.record[2]
						type_category = curr_row.record[3]
						geometric_shape = curr_row.shape.__geo_interface__
						print("GEOMETRIC: " + str(geometric_shape))
						print("CURR WELL KNOWN TEXT:"  + str(shape(curr_row.shape.__geo_interface__).wkt))
					#ST_GeomFromGeoJSON(%s)
						
						cur.execute("INSERT INTO international_hazards VALUES (%s, %s, %s, ST_GeomFromText(%s));",
							(region, date, type_category, shape(curr_row.shape.__geo_interface__).wkt))

					elif(purpose == 'global_surface_temperatures'):
						print("CURR ROW: " + str(curr_row.record))
						temp = curr_row.record[0]
						num_vertices = curr_row.record[1]
						unit_c = "Celsius"
						geometric_shape = curr_row.shape.__geo_interface__
						print("GEOMETRIC: " + str(geometric_shape))
						print("CURR WELL KNOWN TEXT:"  + str(shape(curr_row.shape.__geo_interface__).wkt))
						#ST_GeomFromGeoJSON(%s)
						
						
						cur.execute("INSERT INTO global_surface_temperatures VALUES (%s, %s, %s, ST_GeomFromText(%s));",
							(temp, num_vertices, unit_c, shape(curr_row.shape.__geo_interface__).wkt))
					elif(purpose == 'drought'):
						print('CURR ROW: ' + str(curr_row.record))
						improvement = curr_row.record[0]
						persistent = curr_row.record[1]
						development = curr_row.record[2]
						date_drought = curr_row.record[3]
						# target_date = curr_row.record[4]
						removal = curr_row.record[5]

						geometric_shape = curr_row.shape.__geo_interface__

						cur.execute("INSERT INTO drought VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s));",
							(improvement, persistent, development, date_drought, removal, shape(geometric_shape).wkt))



			except Exception as e:
				print("ERROR: " + str(e))
				continue


				
			# records = r.records()

			# for curr_record in records:
			# 	print("CURR RECORD: " + str(curr_record))
	except(Exception, psycopg2.DatabaseError) as error:
		print("ERROR: " + str(error))
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')


		


	# 	poly = Polygon(shell=[(0,0),(0,10),(10,10),(10,0),(0,0)], holes=[[(2,2),(2,6),(7,7),(2,2)]])
	# poly_wkb = hexlify(poly.wkb).upper() # convert to ASCII version of WKB



	# print(zipshape.namelist())
	# dbfname, _, shpname, _, shxname = zipshape.namelist()
	# r = shapefile.Reader(shp=StringIO.StringIO(zipshape.read(shpname)), shx=StringIO.StringIO(zipshape.read(shxname)), dbf=StringIO.StringIO(zipshape.read(dbfname)))


def connect():
	conn = None
	try:
		params = config()
		print("CONFIG SUCCESSFUL..")
		print(params)
		conn = psycopg2.connect(**params)
		conn.autocommit = True
		print("CONN: "  + str(conn))
		cur = conn.cursor()

		cur.execute('DROP TABLE IF EXISTS dairy ;')
		cur.execute('CREATE TABLE dairy(year integer NOT NULL, retail_cost integer NOT NULL, farm_value integer NOT NULL, farm_to_retail_spread integer NOT NULL, farm_value_share integer NOT NULL);')



		df = pd.read_csv('dairy_basket.csv', dtype={
            "year" : str,
            "retail_cost" : str,
            "farm_value" : str,
            "farm_to_retail_spread" : str,
            "farm_value_share" : str,
           
        })

		# out = df.to_json(orient = 'index')
		# print(str(out))

		# with open('dairy.json', 'w') as outfile:
		# 	json.dump(out, outfile)

		# with open('dairy.json', 'r') as infile:


		for index, row in df.iterrows():

			year = row['Year']
			retail_cost = row['Retail cost']
			farm_value = row['Farm value']
			farm_to_retail_spread = row['Farm-to-retail spread']
			farm_value_share = row['Farm value share']




			cur.execute('INSERT INTO dairy VALUES(%s, %s, %s, %s, %s)', (year, retail_cost, farm_value, farm_to_retail_spread, farm_value_share))


		


		# db_version = cur.fetchone()
		# print(db_version)
		# cur.commit()
		cur.close()

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
	# connect()