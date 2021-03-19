from flask import Flask, render_template, request, Response
from flask.json import jsonify
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, func
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import time

Base = declarative_base()

class Reading(Base):
    __tablename__ = 'readings'
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_uuid = Column(String)
    type = Column(String)
    value = Column(Integer)
    date_created = Column(Integer)

HTTP_UNPROCESSABLE_ENTITY = 422
SENSOR_MIN = 0
SENSOR_MAX = 100
VALID_SENSOR_RANGE = range(SENSOR_MIN,SENSOR_MAX+1)
VALID_SENSOR_TYPE = ['temperature',
                      'humidity']

app = Flask(__name__)

# Setup the SQLite DB
session = None

def get_db_session():
    if app.config['TESTING']:
        engine = create_engine("sqlite:///test_database.db")
    else:
        engine = create_engine("sqlite:///database.db")
    return sessionmaker(bind=engine)()

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
    with app.app_context():
        session = get_db_session()

    data = {}
    if len(request.data) > 0:
        data = json.loads(request.data)
    
    if request.method == 'POST':
        if len(data) == 0:
            return 'Missing Payload', HTTP_UNPROCESSABLE_ENTITY
        # Grab the post parameters
        sensor_type = data.get('type')
        value = data.get('value')
        date_created = data.get('date_created', int(time.time()))
        if sensor_type is None or sensor_type not in VALID_SENSOR_TYPE:
            return ('Invalid sensor type. Needs to be one of: '
                    f'{VALID_SENSOR_TYPE}'), HTTP_UNPROCESSABLE_ENTITY
        if value is None or value not in VALID_SENSOR_RANGE:
            return ('Invalid sensor range. Needs to be in: '
                    f'{VALID_SENSOR_RANGE}'), HTTP_UNPROCESSABLE_ENTITY
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
        sensor_type = data.get('type')
        start = data.get('start')
        end = data.get('end')
        query = session.query(Reading).filter(Reading.device_uuid==device_uuid)
        if sensor_type is not None:
            query = query.filter(Reading.type==sensor_type)
        if start is not None:
            query = query.filter(Reading.date_created >= start)
        if end is not None:
            query = query.filter(Reading.date_created <= end)

        # Return the JSON
        return jsonify([{'device_uuid': row.device_uuid,
                         'type': row.type,
                         'value': row.value,
                         'date_created': row.date_created} for row in query.all()]), 200

@app.route('/devices/<string:device_uuid>/readings/min/', methods = ['GET'])
def request_device_readings_min(device_uuid):
    """
    This endpoint allows clients to GET the min sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    data = {}
    if len(request.data) > 0:
        data = json.loads(request.data)
    sensor_type = data.get('type')
    if sensor_type is None:
        return 'Missing Payload', HTTP_UNPROCESSABLE_ENTITY

    with app.app_context():
        session = get_db_session()

    query = session.query(func.min(Reading.value), Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if data.get('start') is not None:
        query = query.filter(Reading.date_created >= data.get('start'))
    if data.get('end') is not None:
        query = query.filter(Reading.date_created <= data.get('end'))
    
    return jsonify([{'device_uuid': device_uuid,
                     'type': sensor_type,
                     'value': query.one()[0],
                     'date_created': query.one()[1]} for row in query.all()]), 200

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
    data = {}
    if len(request.data) > 0:
        data = json.loads(request.data)
    sensor_type = data.get('type')
    if sensor_type is None:
        return 'Missing Payload', HTTP_UNPROCESSABLE_ENTITY

    with app.app_context():
        session = get_db_session()

    query = session.query(func.max(Reading.value), Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if data.get('start') is not None:
        query = query.filter(Reading.date_created >= data.get('start'))
    if data.get('end') is not None:
        query = query.filter(Reading.date_created <= data.get('end'))
    return jsonify([{'device_uuid': device_uuid,
                     'type': sensor_type,
                     'value': query.one()[0],
                     'date_created': query.one()[1]} for row in query.all()]), 200

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
