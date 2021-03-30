import json
import pytest
import sqlite3
import time
import unittest

from app import app

class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (id INTEGER, device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')

        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (0, self.device_uuid, 'temperature', 22, 5))
        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (1, self.device_uuid, 'temperature', 50, 10))
        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (2, self.device_uuid, 'temperature', 100, 20))
        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (3, self.device_uuid, 'temperature', 10, 25))

        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (4,'other_uuid', 'temperature', 22, 30))

        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (5, self.device_uuid, 'humidity', 42, 40))

        cur.execute('insert into readings (id, device_uuid,type,value,date_created) VALUES (?,?,?,?,?)',
                    (6, self.device_uuid, 'humidity', 23, 50))

        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have six sensor readings
        self.assertEqual(len(json.loads(request.data)), 6)

        #When we make a request with empty request data
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                    data=json.dumps({
                                    }))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have six sensor readings
        self.assertEqual(len(json.loads(request.data)), 6)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100
            }))

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have five
        self.assertEqual(len(rows), 7)

    def test_device_readings_post_invalid_parameters(self):
        #If we make a request with empty POST data
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=json.dumps(dict()))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with non-JSON POST data
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data="No data")

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #When we make a request with a missing 'value' parameter
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'type': 'temperature',
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #When we make a request with a missing 'type' parameter
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'value': 23,
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'type': 'false',
                                        'value': 22
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'value' parameter
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'type': 'temperature',
                                        'value': -1
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

    def test_device_readings_get_invalid_parameters(self):
        #If we make a request with an invalid 'type' parameter
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'type': 'false',
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'start': -1,
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'end': -1,
                                     }))

        #Then we should receive a 422
        self.assertEqual(request.status_code, 422)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """

        #If we make a valid request
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid),
                                    data=json.dumps({
                                        'type': 'temperature',
                                    }))

        #Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive 4 values
        self.assertEqual(len(json.loads(request.data)), 4)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """

        #If we make a valid request
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'type': 'humidity',
                                    }))

        #Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive 2 values
        self.assertEqual(len(json.loads(request.data)), 2)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """

        #If we make valid request with a startdate > 10
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'start': 10,
                                    }))

        #Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive five values
        self.assertEqual(len(json.loads(request.data)), 5)

        #If we make valid request with a startdate < 40
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'end': 40,
                                    }))

        #Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive five values
        self.assertEqual(len(json.loads(request.data)), 5)

        #If we make valid request with startdate >= 10 and enddate <= 20
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'start': 10,
                                        'end': 20
                                    }))

        #Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive two values
        self.assertEqual(len(json.loads(request.data)), 2)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        metric = 'min'

        #If we make an empty request
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'false'}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'start': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'end': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a valid request for temperature values
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 10,
                              'date_created': 25})

        #If we make a valid request for humidity values
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'humidity',
                              'value': 23,
                              'date_created': 50})

        #If we make a valid request for temperature values in a certain range
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 50,
                              'date_created': 10})

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        metric = 'max'

        #If we make an empty request
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'false'}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'start': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'end': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a valid request for temperature
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 100,
                              'date_created': 20})

        #If we make a valid request for humidity
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'humidity',
                              'value': 42,
                              'date_created': 40})

        #If we make a valid request for temperature values in a certain range
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))

        #We should receive a 200
        self.assertEqual(request.status_code, 200)

        #And receive the following dict as a result
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 100,
                              'date_created': 20})

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        metric = 'median'

        #If we make an empty request
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'false'}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'start': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'end': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 50,
                              'date_created': 10})

        #Test humidity metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'humidity',
                              'value': 23,
                              'date_created': 50})

        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 25}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 50,
                              'date_created': 10})

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        metric = 'mean'

#If we make an empty request
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'false'}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'start': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'end': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'value': 45.5})

        #Test humidity metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'value': 32.5})


        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'value': 75})

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        self.assertTrue(False)

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        metric = 'quartiles'

        #If we make an empty request
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'type' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'false'}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'start' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'start': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with an invalid 'end' parameter
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'end': -1}))

        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #If we make a request with missing date parameters
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        #We should receive a 422
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'quartile_1': 50,
                              'quartile_3': 100})
