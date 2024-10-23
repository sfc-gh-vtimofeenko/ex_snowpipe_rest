{
  description = "Description for the project";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    devshell.url = "github:numtide/devshell";
  };

  outputs =
    inputs@{ flake-parts, self, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [ inputs.devshell.flakeModule ];
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      perSystem =
        {
          config,
          self',
          inputs',
          pkgs,
          system,
          ...
        }:
        {
          packages.default = pkgs.rustPlatform.buildRustPackage {
            name = "perf-testing";

            src = ./.;

            buildInputs =
              [ pkgs.openssl ]
              ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
                pkgs.libiconv
                pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
              ];
            nativeBuildInputs = [
              pkgs.openssl
              pkgs.pkg-config
            ];
            cargoHash = "sha256-7Zdhv0NZ5Q2M6rw3xMsFD9DLBODPIBOmM+5hOndLxDk=";
          };
          devshells.default = {
            commands = [
              {
                help = "build the main package";
                name = "main-build";
                command = ''
                  pushd $PRJ_ROOT/..
                  make build
                  popd
                '';
              }
              {
                help = "Build the docker image for the main package";
                name = "main-docker-build";
                command = ''
                  pushd $PRJ_ROOT/..
                  make docker
                  popd
                '';
              }
              {
                help = "Run the docker image locally";
                name = "main-docker-run";
                # Differences:
                # 1. Add --rm to kill container when done
                # 2. Add spacer for visual run separation
                command = ''
                  set -euo pipefail

                  docker run --rm -p 8080:8080 snowpiperest \
                    --snowflake.url="$SNOWFLAKE_URL" \
                    --snowflake.user="$SNOWFLAKE_USER" \
                    --snowflake.role="$SNOWFLAKE_ROLE" \
                    --snowflake.private_key="$SNOWFLAKE_PRIVATE_KEY" \
                    --snowpipe.name="$UNIQUE_NAME" \
                    --snowpipe.database="$DATABASE_NAME" \
                    --snowpipe.schema="$SCHEMA_NAME" \
                    --snowpipe.table="$TABLE_NAME" \
                    --snowpiperest.wal.enable=1 \
                    --snowpiperest.wal.dir=wal \
                    --snowpiperest.wal.flush=1 | ${pkgs.lib.getExe pkgs.spacer}
                '';
              }
              {
                help = "Use curl to insert 1 record by hand";
                name = "test-curl";
                command = ''
                  curl -X PUT -H "Content-Type: application/json" -d '[{"a": 1, "b": "one"}]' "http://localhost:8080/snowpipe/insert"
                '';
              }
              {
                help = "Run the perftest using cargo. Passes arguments to cargo run";
                name = "perftest-run-cargo";
                command = # bash
                  ''
                    pushd ./src
                    cargo run --release -- $@
                    popd
                  '';
              }
              {
                help = "Truncate the table";
                name = "table-truncate";
                category = "table";
                command = ''
                  snow sql --query "TRUNCATE TABLE $DATABASE_NAME.$SCHEMA_NAME.$TABLE_NAME"
                '';
              }
              {
                help = "Get table count";
                name = "table-count";
                category = "table";
                command = ''
                  snow sql --query "SELECT COUNT(*) FROM $DATABASE_NAME.$SCHEMA_NAME.$TABLE_NAME"
                '';
              }
            ];

            packages = [
              # Building the main project needs this
              pkgs.temurin-jre-bin-11
              pkgs.maven
              pkgs.gnumake
              # docker provided out of band
              # Rust stuff
              pkgs.rustc
              pkgs.cargo
              pkgs.clippy
              pkgs.rustfmt
              # misc
              pkgs.lazydocker
            ];
          };
        };
    };
}
