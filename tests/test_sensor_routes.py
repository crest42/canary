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

        # And the response data should have three sensor readings
        self.assertEqual(len(json.loads(request.data)), 6)

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading

        #Test empty payload
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=json.dumps(dict()))

        self.assertEqual(request.status_code, 422)
        #Test missing parameter 'value'
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'type': 'temperature',
                                     }))
        self.assertEqual(request.status_code, 422)

        #Test missing parameter 'type'
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid),
                                     data=json.dumps({
                                        'value': 22,
                                     }))
        self.assertEqual(request.status_code, 422)

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

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """

        #Test for working case
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'type': 'temperature',
                                    }))
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 4)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'type': 'humidity',
                                    }))
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 2)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """

        #One-Sided-Test if all values >= 10 are returned
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'start': 10,
                                    }))
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 5)

        #One-Sided-Test if all values <= 40 are returned
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'end': 40,
                                    }))
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 5)

        #Two-Sided-Test if all values >= 10 and <= 20 are returned
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid), data=
                                    json.dumps({
                                        'start': 10,
                                        'end': 20
                                    }))
        self.assertEqual(request.status_code, 200)
        self.assertEqual(len(json.loads(request.data)), 2)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        metric = 'min'

        #Test empty payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))
        self.assertEqual(request.status_code, 422)

        #Test with missing type
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 10,
                              'date_created': 25})

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
                                                     'end': 20}))
        self.assertEqual(request.status_code, 200)
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

        #Test empty payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))
        self.assertEqual(request.status_code, 422)

        #Test with missing type
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 100,
                              'date_created': 20})

        #Test humidity metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'humidity',
                              'value': 42,
                              'date_created': 40})

        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))
        self.assertEqual(request.status_code, 200)
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

        #Test empty payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))
        self.assertEqual(request.status_code, 422)

        #Test with missing type
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 36.0,
                              'date_created': 15})

        #Test humidity metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'humidity',
                              'value': 32.5,
                              'date_created': 45})

        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 25}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'device_uuid': self.device_uuid,
                              'type': 'temperature',
                              'value': 50.0,
                              'date_created': 10.0})

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        metric = 'mean'

        #Test empty payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))
        self.assertEqual(request.status_code, 422)

        #Test with missing type
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')
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
                             {'value': 53.33})

        self.assertTrue(False)

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

        #Test empty payload
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/', data=json.dumps({}))
        self.assertEqual(request.status_code, 422)

        #Test with missing type
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/')
        self.assertEqual(request.status_code, 422)

        #Test temperatur metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'quartile_1': 19.0,
                              'quartile_3': 62.5})

        #Test humidity metric read
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'humidity'}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'quartile_1': 27.75,
                              'quartile_3': 37.25})

        #Test temperatur metric read with date
        request = self.client().get(f'/devices/{self.device_uuid}/readings/{metric}/',
                                    data=json.dumps({'type': 'temperature',
                                                     'start': 10,
                                                     'end': 20}))
        self.assertEqual(request.status_code, 200)
        self.assertDictEqual(json.loads(request.data)[0],
                             {'quartile_1': 62.5,
                              'quartile_3': 87.5})