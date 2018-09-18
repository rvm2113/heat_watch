# Heat Watch
<br/>
Traveler's Guide allows users to specify locations in the world where they would prefer to travel, the address of each person(in case of a group rendez-vous), preferred radius/distance to nearby airports, and preferred weather for the destination. Traveler's Guide then customizes and outputs all cities that 
match the given criteria.
<br/>
<br/>
Heat Watch, a Flask web service, allows users to specify addresses and desired temperatures in order to determine where they would prefer to travel.
Heat Watch offers three major endpoints for acquiring information: /global_surface_temperatures, /droughts, /international_hazards.
For each respective endpoint, Heat Watch returns metrics for nearby droughts, hazards, and addresses of global surface temperatures
within the specified range.
<br/>
In addition, Heat Watch offers the option to create entries for addresses and obtain all stored
addresses by nearby hazards and nearby droughts.
<br/>

# System Requirements/Technologies Used:
<br />
Java Version 10.0.2 2018-07-17(or higher) <br />
Apache Kafka 2.12-2.0.0(or higher) <br />
Zookeeper 3.4.12(or higher)<br />
PostgreSQL 10.1(or higher)<br/>
Flask <br/>
Python 3<br/>
PostGIS extension for PostgreSQL<br/>

# Usage: 

(For setup):
Within the path folder for the Zookeeper installation:
Complete Path/usr/local/zookeeper-3.4.12/bin
sudo ./zkServer.sh start

Within the path folder for the Kafka installation:
sudo ./kafka-server-start.sh ../config/server.properties


<br />
(To execute the Flask web service): 

<br />
python3 app.py<br />


Sample Requests
<br/>

curl -XGET 'http://127.0.0.1:5000/international_hazards/11+Th+Street%2C+Central+Business+Dis%2C+Abuja%2C+Nigeria'
curl -XGET 'http://127.0.0.1:5000/droughts/3199+Juniper+St+San+Diego%2C+CA+92104'
curl -XGET 'http://127.0.0.1:5000/global_surface_temperatures/30/60'

<br/>





# Functionalities:

<br />



<br />
<br />
<br />


<br />
<br />
<br />






