# REST API for Snowpipe Streaming
This repo creates a REST API for ingesting data into Snowflake via
Snowpipe Streaming.

There is one endpoint:
* `snowpipe/insert` - this will load the data into the
    specified table. This accepts the `PUT` verb.

You specify the table that you want to insert data into via application
properties. You can either modify the `application.properties` file, or
specify the environment variables (see below).

The data is sent in the body of the `PUT` request. The data is a JSON array
of JSON objects. For example:

```json
[{"some_int": 1, "some_string": "one"}, {"some_int": 2, "some_string": "two"}]
```

If the database user running the service does not have permissions to 
write to the specified table, a `404` error is returned. If the data is
incorrectly formatted, a `400` error is returned.

# Instructions
Before starting, you will need a Snowflake user with access to a warehouse
and permissions on the table(s) that you want to write to. You will also 
need an SSH Key for your Snowflake user (see [here](https://docs.snowflake.com/en/user-guide/key-pair-auth.html))

This example is driven from the Makefile. The Makefile has variables at the top
that can be overriden by either editing the Makefile or setting the variable(s) in
the Linux environment.

To build the application, run `make build`. At this point you can run the 
application locally.

## Running Locally
To run the application locally, you will need to run with Java. 
You need to specify a few parameters to run:
* `snowflake.url` - the HTTPS URL for your Snowflake account (e.g., `https://myacct.snowflakecomputing.com`)
* `snowflake.user` - the Snowflake user that the application should use
* `snowflake.role` - the role for the Snowflake user that the application should use
* `snowflake.private_key` - the SSH private key for the Snowflake user; this should be the private PEM file minus the header and footer and on one line (CR/LF removed).
* `snowpipe.name` - a name for the client. This will allow identifying this client from others. It should be unique.
* `snowpipe.database` - the name of the database to insert data into
* `snowpipe.schema` - the name of the schema in the database to insert data into
* `snowpipe.table` - the name of the table in the schema to insert data into

You can set these by environment variable, as well:
* `SNOWFLAKE_URL` for `snowflake.url`
* `SNOWFLAKE_USER` for `snowflake.user`
* `SNOWFLAKE_ROLE` for `snowflake.role`
* `SNOWFLAKE_PRIVATE_KEY` for `snowflake.private_key`
* `SNOWPIPE_NAME` for `snowpipe.name`
* `SNOWPIPE_DATABASE` for `snowpipe.database`
* `SNOWPIPE_SCHEMA` for `snowpipe.schema`
* `SNOWPIPE_TABLE` for `snowpipe.table`

From the commandline run:
```bash
java -jar target/SnowpipeRest-0.0.1-SNAPSHOT.jar \
  --snowflake.url="<SNOWFLAKE URL>" \
  --snowflake.user="<SNOWFLAKE USER>" \
  --snowflake.role="<SNOWFLAKE ROLE>" \
  --snowflake.private_key="<SNOWFLAKE PRIVATE KEY (as a single line)>" \
  --snowpipe.name="<UNIQUE NAME>" \
  --snowpipe.database="<DATABASE NAME>" \
  --snowpipe.schema="<SCHEMA NAME>" \
  --snowpipe.table="<TABLE NAME>"
```

Alternatively, you can edit the `src/main/resources/application.properties` and add
your parameters there. Then you can just run `java -jar target/SnowpipeRest-0.0.1-SNAPSHOT.jar`.

Additionally, set the proper environment variables and run:
```bash
java -jar target/SnowpipeRest-0.0.1-SNAPSHOT.jar
```

## Running with Docker
If you want to build a Docker container for this application, you can run
`make docker` which builds for the local platform.
If you want to make the Docker image specifically for the `linux/amd64` platform, 
run `make docker_native`.

To run the Docker image (here named `snowpiperest`) locally, you can run:
```bash
docker run -p 8080:8080 snowpiperest \
  --snowflake.url="<SNOWFLAKE URL>" \
  --snowflake.user="<SNOWFLAKE USER>" \
  --snowflake.role="<SNOWFLAKE ROLE>" \
  --snowflake.private_key="<SNOWFLAKE PRIVATE KEY (as a single line)>" \
  --snowpipe.name="<UNIQUE NAME>" \
  --snowpipe.database="<DATABASE NAME>" \
  --snowpipe.schema="<SCHEMA NAME>" \
  --snowpipe.table="<TABLE NAME>"
```

Note, see above for the parameters.

If you set the environment variables, you can also run
```bash
docker run -p 8080:8080 --env-file env.list snowpiperest
```

Alternatively, if you set the envrionment variables, you can 
also run the Docker image using Docker Compose:
```bash
docker compose up
```

Or use the `run` target in the Makefile (which uses Docker Compose):
```bash
make run
```

## Test the API

### Setup
1. Create a simple table to test:
```
CREATE TABLE mydb.myschema.mytbl (a INT, b TEXT, c DOUBLE);
```

2. Grant permission to read/write to the table to the Snowpipe Streaming user
```
GRANT ALL ON mydb.myschema.mytbl TO myapprole;
```

### Tests
1. Insert one record:
```
curl -X PUT -H "Content-Type: application/json" -d '[{"a": 1, "b": "one"}]' "http://localhost:8080/snowpipe/insert"
```

Expected response:
```
{
  "inserts_attempted": 1,
  "inserts_succeeded": 1,
  "insert_errors": 0,
  "error_rows":
    [
    ]
}
```

Check the contents of the table:
```
SELECT * FROM mydb.myschema.mytbl;
```

2. Insert one record:
```
curl -X PUT -H "Content-Type: application/json" -d '[{"a": 2, "b": "two"}, {"a": 3, "b": "three", "c": 3.0}]' "http://localhost:8080/snowpipe/insert"
```

Expected response:
```
{
  "inserts_attempted": 2,
  "inserts_succeeded": 2,
  "insert_errors": 0,
  "error_rows":
    [
    ]
}
```

Check the contents of the table:
```
SELECT * FROM mydb.myschema.mytbl;
```

3. Try to insert malformed data:
```
curl -X PUT -H "Content-Type: application/json" -d '[{"a": 1, "b": "one"]' "http://localhost:8080/snowpipe/insert"
```

Expected response:
```
400 BAD_REQUEST "Unable to parse body as list of JSON strings."
```
