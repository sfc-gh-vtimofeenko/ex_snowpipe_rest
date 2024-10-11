This is performance testing script for the project. It's based on [goose
framework](https://book.goose.rs/).

# How to use

Before using the performance test, follow [parent README](../README.md) and have
the project running in docker. Assuming the default settings are used and
`ex_snowpipe_rest` is listening on `http://localhost:8080`.

Use one of the following methods to run the test:

## Using docker

Run following commands:
```
docker build -t "ex_snowpipe_rest_perftest" .
docker run --network=host ex_snowpipe_rest_perftest /app/bin/perftest --host http://localhost:8080 --run-time 1m --no-reset-metrics
```

Notes:

* `--network=host` is needed if running independent docker containers and is an
  easy way to get `perftest` container access to `ex_snowpipe_rest` container.
* Additional options are available from by passing `--help` flag

## Using nix

1. `nix build .`
2. `./result/bin/perftest --host http://localhost:8080 --run-time 1m
   --no-reset-metrics`

## Using Rust

If you have Rust toolchain and don't want to use docker or nix, you can run:

```
cargo run --release -- --host http://localhost:8080 --run-time 1m --no-reset-metrics
```
