from flask import Flask, render_template, request, Response
from flask.json import jsonify
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import time

Base = declarative_base()

class Reading(Base):
    __tablename__ = 'readings'

    device_uuid = Column(String, primary_key=True)
    type = Column(String)
    value = Column(Integer)
    date_created = Column(Integer)

HTTP_UNOROCESSABLE_ENTITY = 422
SENSOR_MIN = 0
SENSOR_MAX = 100
VALID_SENSOR_RANGE = range(SENSOR_MIN,SENSOR_MAX+1)
VALID_SENSOR_TYPE = ['temperature',
                      'humidity']

app = Flask(__name__)

# Setup the SQLite DB
engine = create_engine("sqlite:///database.db")
session = sessionmaker(bind=engine)
conn = sqlite3.connect('database.db')
conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
conn.close()

@app.route('/devices/<string:device_uuid>/readings/', methods = ['POST', 'GET'])
def request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """

    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    post_data = json.loads(request.data)
    if request.method == 'POST':
        # Grab the post parameters
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created', int(time.time()))

        if sensor_type not in VALID_SENSOR_TYPE:
            return HTTP_UNOROCESSABLE_ENTITY, ('Invalid sensor type. Needs to be one of: '
                                              f'{VALID_SENSOR_TYPE}')
        if value not in VALID_SENSOR_RANGE:
            return HTTP_UNOROCESSABLE_ENTITY, ('Invalid sensor range. Needs to be in: '
                                              f'{VALID_SENSOR_RANGE}')
        # Insert data into db
        reading = Reading(device_uuid=device_uuid,
                          type=sensor_type,
                          value=value,
                          date_created=date_created)
        session.add(reading)
        session.commit()
        
        # Return success
        return 'success', 201
    else:
        # Execute the query
        sensor_type = post_data.get('type')
        start = post_data.get('start', 0)
        end = post_data.get('end', int(time.time()))
        session.query(Reading).filter_by(type=sensor_type, start=start, end=end).all()
        cur.execute('select * from readings where device_uuid="{}"'.format(device_uuid))
        rows = cur.fetchall()

        # Return the JSON
        return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200

@app.route('/devices/<string:device_uuid>/readings/max/', methods = ['GET'])
def request_device_readings_max(device_uuid):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

@app.route('/devices/<string:device_uuid>/readings/median/', methods = ['GET'])
def request_device_readings_median(device_uuid):
    """
    This endpoint allows clients to GET the median sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

@app.route('/devices/<string:device_uuid>/readings/mean/', methods = ['GET'])
def request_device_readings_mean(device_uuid):
    """
    This endpoint allows clients to GET the mean sensor readings for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods = ['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

#@app.route('<fill-this-in>', methods = ['GET'])
def request_readings_summary():
    """
    This endpoint allows clients to GET a full summary
    of all sensor data in the database per device.

    Optional Query Parameters
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """

    return 'Endpoint is not implemented', 501

if __name__ == '__main__':
    app.run()
