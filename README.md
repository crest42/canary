# Canary Platform Homework

## Introduction
Imagine a system where hundreds of thousands of Canary like hardware devices are concurrently uploading temperature and humidty sensor data.

The API to facilitate this system accepts creation of sensor records, in addition to retrieval.

These `GET` and `POST` requests can be made at `/devices/<uuid>/readings/`.

Retrieval of sensor data should return a list of sensor values such as:

```
    [{
        'date_created': <int>,
        'device_uuid': <uuid>,
        'type': <string>,
        'value': <int>
    }]
```

The API supports optionally querying by sensor type, in addition to a date range.

A client can also access metrics such as the max, median and mean over a time range.

These metric requests can be made by a `GET` request to `/devices/<uuid>/readings/<metric>/`

When requesting max or median, a single sensor reading dictionary should be returned as seen above.

When requesting the mean, the response should be:

```
    {
        'value': <mean>
    }
```

The API also supports the retrieval of the 1st and 3rd quartile over a specific date range.

This request can be made via a `GET` to `/devices/<uuid>/readings/quartiles/` and should return

```
    {
        'quartile_1': <int>,
        'quartile_3': <int>
    }
```

Finally, the API supports a summary endpoint for all devices and readings. When making a `GET` request to this endpoint, we should receive a list of summaries as defined below, where each summary is sorted in descending order by number of readings per device.

```
    [
        {
            'device_uuid':<uuid>,
            'number_of_readings': <int>,
            'max_reading_value': <int>,
            'median_reading_value': <int>,
            'mean_reading_value': <int>,
            'quartile_1_value': <int>,
            'quartile_3_value': <int>
        },

        ... additional device summaries
    ]
```

The API is backed by a SQLite database.

## Getting Started
This service requires Python3. To get started, create a virtual environment using Python3.

Then, install the requirements using `pip install -r requirements.txt`.

Finally, run the API via `python app.py`.

## Testing
Tests can be run via `pytest -v`.

## Tasks
Your task is to fork this repo and complete the following:

- [x] Add field validation. Only *temperature* and *humidity* sensors are allowed with values between *0* and *100*.

Design rational:

I used jsonschema for input data validation. the benefits are quite obvious. The request data are assumed to be in JSON anyways. By using jsonschema its easy to describe the fields with their limitations in a human readable dataformat and offload the validation to the library.
This way the parsing on every endpoint can be implemented like:

```
if request.data:
        try:
            data = json.loads(request.data)
        except json.JSONDecodeError:
            return ('Request contains no valid JSON in POST data'), HTTP_UNPROCESSABLE_ENTITY
    try:
        validate(instance=data, schema=request_device_readings_metric_schema)
    except ValidationError as validation_error:
        return (f'Validation Error: {validation_error}'), HTTP_UNPROCESSABLE_ENTITY
```

I use HTTP_UNPROCESSABLE_ENTITY (422) as an error for syntactical correct request with flawed data. Its defined as a extension for WebDAV[1]. While it is not standard HTTP a sane client should at least fallback to an 4XX error message and a _more_ sane client could use this for better error handling.

- [x] Add logic for query parameters for *type* and *start/end* dates.

I decided to switch the database backend from the sqlite3 python module to sqlalchemy. Using the ORM model of sqlalchemy we can access our database in an object-like fashion. This way something like:

```
if start_date is not None:
        query = query.filter(Reading.date_created >= start_date)
```

allows us to easily extend the query with the optional parameters, without messing around with manual SQL statements. It was necessary to include a primary key in the table for sqlalchemy's backend.

- [x] Implementation
  - [x] The max, median and mean endpoints.

This also benefits from sqlalchemy. Through using sqlalchemy.func.(max|avg|min) the endpoint implementation is almost identical.
Regarding the median it is a little bit more complicated:

Since I would expect the database to grow largely over time calculating the median from a set of data, returned by the database, is not feasible and instead the calculation should be offloaded to the database backend. The downside with this approach is that sqlite3 does not include an aggregate function for the median directly. But since we need to include the quartiles in a different endpoint, we could use the quartiles and the definition of the median as the .5 quartile.

By using the following statement:
```
WITH p AS (SELECT value, NTILE(4) OVER ( ORDER BY value) AS percentile
           FROM readings)
SELECT percentile, MAX(value) as value
FROM p
GROUP by percentile;
```

We divide the dataset into 4 equal-sized bins and select the maximum for each bin. It is important to notice that this approach only selects the correct value approximately. Imagine the following example:

[2,4,6,8]

The Median in this example is not well-defined, since the number of values is even. Usually the mean between 4 and 6 = 5 would be used as the median. In our example we define the median in this case to be floor(a,b) = 4. The R language defines not less then 6 different methods of calculating quartiles in such cases. I would argue that this is a valid implementation of quartiles is correct, and gives a good approximation, while being very efficient, since database-level optimization can be used to reduce the overhead of the query to O(1).

  - [x] The quartiles endpoint with start/end parameters

As a combination of the quartile calculation above and the primitives used in the other endpoints, this endpoint is trivially implemented.

  - [x] Add the path for the summary endpoint
  - [x] Complete the logic for the summary endpoint

In the implementation I used two different queries to retrieve the results.

1. An aggregation query using group_by and min, max, avg etc. to calculate the aggregates grouped by device_uuid
2. The quartile calculation function as described above to calculate the quartiles, grouped by device_uuid

It could have been possible to reduce code duplication by sharing re-using utility function to calculate the aggregates in this function and the corresponding aggregate function by the cost of sending more queries to the database backend. I decided to reduce the amount of queries to avoid performance bottlenecks in the future.

- [x] Tests
  - [x] Wrap up the stubbed out unit tests with your changes

I took the freedom to add additional rows to the test database to make the tests a bit more meaningful.

  - [x] Add tests for the new summary endpoint
  - [x] Add unit tests for any missing error cases
- [x] README
  - [x] Explain any design decisions you made and why.
  - [x] Imagine you're building the roadmap for this project over the next quarter. What features or updates would you suggest that we prioritize?

When you're finished, send your git repo link to Michael Klein at michael@canary.is. If you have any questions, please do not hesitate to reach out!

Open Issues (minor):

- The data is assumed to be in JSON. We could offload the JSON parsing to flask by limiting the requests to Content-Type JSON. This way we could use 'request.json' directly
- Currently a lot of the unit tests are duplicates. This can be reduce and lead to more readable tests by accumulating common test-cases.
- The endpoint implementations code is also quiet redundant. There are multiple opportunities to reduce boilerplate code by using utility functions.
- Currently the validations exception from jsonschema is returned in the HTTP response. I cleaner error message may be desireable

Roadmap:

There a 3 major features I would suggest for the near future:

1. The database backend is very simple and may lead to performance issues in the future. When sticking to the relational database model, I would advice different optimizations:

- Normalize the database. e. g.:
 - Adding a device table and using key constrains instead of a single large table
- Calculating aggregates like the max directly when new data is added. This could reduce query load.
- Adding index structures to optimize aggregate queries.

 Indeed, a thoughtful evaluation of the database design and the access pattern of the data would be needed to make more educated suggestions.

2. The current backend may not scale. For a more distributed approach a data-streaming design may be a better fit. Services like Apache Kafka or Elastic can be used to build a easy-to-scale data backend with an efficient support for most of the aggregate queries.

3. Currently the access to the API is not authenticated in any way. While the data is not very sensitive a authentication may be beneficial to reduce the risk of DoS type of attacks and to reduce the risk of manipulation of data.


[1]https://tools.ietf.org/html/rfc4918#section-11.2