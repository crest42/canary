import json
import time
import numpy
from flask import Flask, request
from flask.json import jsonify
from jsonschema import validate, ValidationError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, func
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

HTTP_UNPROCESSABLE_ENTITY = 422
DATE_MIN = 0
SENSOR_MIN = 0
SENSOR_MAX = 100
VALID_SENSOR_TYPES = ['temperature', 'humidity']

request_device_readings_schema_post = {
   'type': 'object',
   'properties': {
       'type': {
           "enum": VALID_SENSOR_TYPES,
       },
       'value': {
           'type': 'number',
           'minimum': SENSOR_MIN,
           'maximum': SENSOR_MAX,
       },
       'date_created': {
           'type': 'number',
       },
   },
   'required': ['type','value']
}

request_device_readings_schema_get = {
   'type': 'object',
   'properties': {
       'type': {
           "enum": VALID_SENSOR_TYPES,
       },
       'start': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
       'end': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
   },
   'required': []
}

class Reading(Base):
    __tablename__ = 'readings'
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_uuid = Column(String)
    type = Column(String)
    value = Column(Integer)
    date_created = Column(Integer)

app = Flask(__name__)

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
    session = None
    with app.app_context():
        session = get_db_session()
    data = {}
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    if request.method == 'POST':
        try:
            validate(instance=data, schema=request_device_readings_schema_post)
        except ValidationError as validation_error:
            return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY
         # Grab the post parameters
        sensor_type = data.get('type')
        value = data.get('value')
        date_created = data.get('date_created', int(time.time()))
        # Insert data into db
        reading = Reading(device_uuid=device_uuid,
                          type=sensor_type,
                          value=value,
                          date_created=date_created)
        session.add(reading)
        session.commit()

        # Return success
        return 'success', 201
    elif request.method == 'GET':
        try:
            validate(instance=data, schema=request_device_readings_schema_get)
        except ValidationError as validation_error:
            return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY
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

    return (f'Invalid request method {request.method}'), HTTP_UNPROCESSABLE_ENTITY

request_device_readings_metric_schema = {
   'type': 'object',
   'properties': {
       'type': {
            "enum": VALID_SENSOR_TYPES,
       },
       'start': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
       'end': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
   },
   'required': ['type']
}

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
    session = None
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_metric_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY

    sensor_type = data.get('type')
    start_date = data.get('start')
    end_date = data.get('end')

    with app.app_context():
        session = get_db_session()
    query = session.query(func.min(Reading.value), Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
    if end_date is not None:
        query = query.filter(Reading.date_created <= end_date)

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
    session = None
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_metric_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY

    sensor_type = data.get('type')
    start_date = data.get('start')
    end_date = data.get('end')

    with app.app_context():
        session = get_db_session()

    query = session.query(func.max(Reading.value), Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
    if end_date is not None:
        query = query.filter(Reading.date_created <= end_date)
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
    data = {}
    session = None
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_metric_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY

    sensor_type = data.get('type')
    start_date = data.get('start')
    end_date = data.get('end')

    with app.app_context():
        session = get_db_session()

    query = session.query(Reading.value, Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
    if end_date is not None:
        query = query.filter(Reading.date_created <= end_date)
    a = query.all()
    i = list(zip(*a))[0].index(numpy.quantile(list(zip(*a))[0], q=0.5, interpolation='nearest'))
    return jsonify([{'device_uuid': device_uuid,
                     'type': sensor_type,
                     'value': a[i][0],
                     'date_created': a[i][1]}]), 200

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

    data = {}
    session = None
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_metric_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY

    sensor_type = data.get('type')
    start_date = data.get('start')
    end_date = data.get('end')

    with app.app_context():
        session = get_db_session()

    query = session.query(func.avg(Reading.value)) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
    if end_date is not None:
        query = query.filter(Reading.date_created <= end_date)
    return jsonify([{'value': query.one()[0]} for row in query.all()]), 200

request_device_readings_quartiles_schema = {
   'type': 'object',
   'properties': {
       'type': {
            "enum": VALID_SENSOR_TYPES,
       },
       'start': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
       'end': {
           'type': 'number',
           'minimum': DATE_MIN,
       },
   },
   'required': ['type','start','end']
}

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
    data = {}
    session = None
    if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_quartiles_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY

    sensor_type = data.get('type')
    start_date = data.get('start')
    end_date = data.get('end')

    with app.app_context():
        session = get_db_session()

    query = session.query(Reading.value, Reading.date_created) \
                   .filter(Reading.device_uuid==device_uuid) \
                   .filter(Reading.type==sensor_type)
    if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
    if end_date is not None:
        query = query.filter(Reading.date_created <= end_date)
    result = query.all()
    lower = list(zip(*result))[0].index(numpy.quantile(list(zip(*result))[0], q=0.25, interpolation='nearest'))
    upper = list(zip(*result))[0].index(numpy.quantile(list(zip(*result))[0], q=0.75, interpolation='nearest'))
    return jsonify([{'quartile_1': result[lower][0],
                     'quartile_3': result[upper][0]}]), 200

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
