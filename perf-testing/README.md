This is performance testing script for the project. It's based on [goose
framework](https://book.goose.rs/).

# How to use

Before using the performance test, follow [parent README](../README.md) and have
the project running in docker. Assuming the default settings are used and
`ex_snowpipe_rest` is listening on `http://localhost:8080`.

Set up a file which will represent a sequence of messages sent to the endpoint.

For example, this file:

```
[ {"col1": 1, "col2": "A"} ]
[ {"col1": 1 }, {"col1": 1} ]
```

Will effectively translate to the following per-client scenario:

1. Connect to the `ex_snowpipe_rest` endpoint
2. Perform a `PUT` with the first line as the body (array of one object)
3. Perform a `PUT` with the second line as the body (array of two objects)
4. GOTO 1

Thus, one scenario should result in 3 lines being added to the table.

Use one of the following methods to run the test:

## Using docker

Run following commands (replacing the placeholders in `,>`):

```
docker build -t "ex_snowpipe_rest_perftest" .
docker run -v <path_to_directory_with_test_file>:/app/data\
    -e FIXTURE_PATH=/app/data/<test_file_name>
    --network=host\
    ex_snowpipe_rest_perftest\
    /app/bin/perftest --host http://localhost:8080 --run-time 1m --no-reset-metrics
```

Notes:

* `--network=host` is needed if running independent docker containers and is an
  easy way to get `perftest` container access to `ex_snowpipe_rest` container.
* Additional options are available from by passing `--help` flag

## Using nix

1. `nix build .`
2. `FIXTURE_PATH=<test_file_name> ./result/bin/perftest --host http://localhost:8080 --run-time 1m
   --no-reset-metrics`

## Using Rust

If you have Rust toolchain and don't want to use docker or nix, you can run:

```
FIXTURE_PATH=<test_file_name> cargo run --release -- --host http://localhost:8080 --run-time 1m --no-reset-metrics
```

## Debugging the test run

Spawns a user, waits a second and completes:
```
FIXTURE_PATH=$(realpath ./test.json) cargo run --release -- --host http://localhost:8080 --run-time 1s --no-reset-metrics -u 1
```
