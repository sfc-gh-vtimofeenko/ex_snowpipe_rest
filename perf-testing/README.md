This is performance testing script for the project. It's based on [goose
framework](https://book.goose.rs/).

# How to use

## High-level overview

The performance tests are packaged in a separate container. The perftest
container only needs access to the container from the main project. Test results
are printed to the console.

## Step by step guide

1. Set up snowpiperest container (follow the [parent README](../README.md)),
   have it listen on `http://localhost:8080`.

2. Set up a fixture file called `test.json` in `perf-testing` directory with the
   following content:

    ```
    [ {"col1": 1, "col2": "A"} ]
    [ {"col1": 1 }, {"col1": 1} ]
    ```

    Tweak the file to match the schema you had established when setting up
    snowpipe.

3. Run the performance test using one of the methods below.

### Using docker

Run following commands (replacing the placeholders in `,>`):

```
cd perf-testing
docker build -t "ex_snowpipe_rest_perftest" .
docker run -v <path_to_directory_with_test.json_file>:/app/data\
    -e FIXTURE_PATH=/app/data/test.json\
    --network=host\
    ex_snowpipe_rest_perftest\
    /app/bin/perftest --host http://localhost:8080 --run-time 1m --no-reset-metrics
```

Notes:

* Make sure you're in the `perf-testing` directory when running docker build, or
  pass the Dockerfile path to the build command
* `--network=host` is needed if running independent docker containers and is an
  easy way to get `perftest` container access to `ex_snowpipe_rest` container.
* Additional options are available from by passing `--help` flag

### Using nix

1. `nix build .`
2. `FIXTURE_PATH=<test_file_name> ./result/bin/perftest --host http://localhost:8080 --run-time 1m
   --no-reset-metrics`

### Using Rust

If you have Rust toolchain installed and don't want to use docker or nix, you can run:

```
FIXTURE_PATH=<test_file_name> cargo run --release -- --host http://localhost:8080 --run-time 1m --no-reset-metrics
```
# Fixture file format

This file:

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

# Debugging the test run

Spawns a user, waits a second and completes:
```
FIXTURE_PATH=$(realpath ./test.json) cargo run --release -- --host http://localhost:8080 --run-time 1s --no-reset-metrics -u 1
```
