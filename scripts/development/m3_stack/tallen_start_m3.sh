#!/usr/bin/env bash

set -xe

source "$(pwd)/../../docker-integration-tests/common.sh"

# Locally don't care if we hot loop faster
export MAX_TIMEOUT=4

RELATIVE="./../../.."
prepare_build_cmd() {
    build_cmd="cd $RELATIVE && make clean-build docker-dev-prep && cp -r ./docker ./bin/ && $1"
}
DOCKER_ARGS="-d --renew-anon-volumes"

if true; then
  export ENVOY_COORD_PORT=8999
  export ENVOY_COORD_ALT=8507
  export ENVOY_AGG1_PORT=8888
  export ENVOY_AGG2_PORT=8889
  export M3_COORD_HOSTNAME="m3coordinator01-envoy"
  export M3_AGG1_HOSTNAME="m3aggregator01-envoy"
  export M3_AGG2_HOSTNAME="m3aggregator02-envoy"
else
  export ENVOY_COORD_ALT=7507
  export ENVOY_COORD_PORT=7201 #8999
  export ENVOY_AGG1_PORT=6000 #8888
  export ENVOY_AGG2_PORT=6000 #8889
  export M3_COORD_HOSTNAME="m3coordinator01"
  export M3_AGG1_HOSTNAME=m3aggregator01
  export M3_AGG2_HOSTNAME=m3aggregator02
fi


# ----------------------------------------------------------------------------------------------------
# TALLEN CUSTOM
# ----------------------------------------------------------------------------------------------------

echo @tallen agg1 hostname ${M3_AGG1_HOSTNAME}

echo "@TALLEN THIS IS CUSTOM EDITS"
export USE_AGGREGATOR=true
export USE_AGGREGATOR_HA=false
export USE_MULTI_DB_NODES=false
export BUILD_M3DBNODE=true
export BUILD_M3COORDINATOR=true
export BUILD_M3AGGREGATOR=true
export USE_MULTIPROCESS_COORDINATOR=false

# @tallen START THE ENVOY
echo ----------------------------------------------------------------------------------------------------
echo @tallen starting the envoy
echo ----------------------------------------------------------------------------------------------------
#docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS m3envoy
docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3envoy

# ----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------

echo "Bringing up nodes in the background with docker compose, remember to run ./stop.sh when done"

# need to start Jaeger before m3db or else m3db will not be able to talk to the Jaeger agent.
if [[ "$USE_JAEGER" = true ]] ; then
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS jaeger
    sleep 3
    # rely on 204 status code until https://github.com/jaegertracing/jaeger/issues/1450 is resolved.
    JAEGER_STATUS=$(curl -s -o /dev/null -w '%{http_code}' localhost:14269)
    if [ $JAEGER_STATUS -ne 204 ]; then
        echo "Jaeger could not start"
        return 1
    fi
fi

M3DBNODE_DEV_IMG=$(docker images m3dbnode:dev | fgrep -iv repository | wc -l | xargs)
M3COORDINATOR_DEV_IMG=$(docker images m3coordinator:dev | fgrep -iv repository | wc -l | xargs)
M3AGGREGATOR_DEV_IMG=$(docker images m3aggregator:dev | fgrep -iv repository | wc -l | xargs)

if [[ "$M3DBNODE_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3DBNODE" == true ]]; then
    prepare_build_cmd "make m3dbnode-linux-amd64"
    echo "Building m3dbnode binary first"
    bash -c "$build_cmd"

    docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS m3db_seed
else
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3db_seed
fi

# Bring up any other replicas
if [[ "$USE_MULTI_DB_NODES" = true ]] ; then
    echo "Running multi node"
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3db_data01
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3db_data02
else
    echo "Running single node"
fi

# Use standard coordinator config when bringing up coordinator first time
# Note: Use ".tmp" suffix to be git ignored.
cp ./m3coordinator-standard.yml ./m3coordinator.yml.tmp
if [[ "$USE_MULTIPROCESS_COORDINATOR" = true ]]; then
    cat ./m3coordinator-snippet-multiprocess.yml >> ./m3coordinator.yml.tmp
fi

if [[ "$M3COORDINATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3COORDINATOR" == true ]]; then
    prepare_build_cmd "make m3coordinator-linux-amd64"
    echo "Building m3coordinator binary first"
    bash -c "$build_cmd"

    docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS m3coordinator01
else
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3coordinator01
fi

echo "Wait for coordinator API to be up"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
  "curl -vvvsSf localhost:${ENVOY_COORD_PORT}/health"

if [[ "$USE_AGGREGATOR" = true ]]; then
    echo "@tallen using aggregator"
    echo "Running aggregator pipeline"
    if [[ "$USE_AGGREGATOR_HA" != true ]]; then
        # Use single replica.
        curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3aggregator/placement/init -d '{
            "num_shards": 64,
            "replication_factor": 1,
            "instances": [
                {
                    "id": "'"${M3_AGG1_HOSTNAME}"'",
                    "isolation_group": "rack-a",
                    "zone": "embedded",
                    "weight": 1024,
                    "endpoint": "'"${M3_AGG1_HOSTNAME}"':'"${ENVOY_AGG1_PORT}"'",
                    "hostname": "'"${M3_AGG1_HOSTNAME}"'",
                    "port": '"${ENVOY_AGG1_PORT}"'
                }
            ]
        }'
    else
        # Use two replicas.
        curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3aggregator/placement/init -d '{
            "num_shards": 64,
            "replication_factor": 2,
            "instances": [
                {
                    "id": "'"${M3_AGG1_HOSTNAME}"'",
                    "isolation_group": "rack-a",
                    "zone": "embedded",
                    "weight": 1024,
                    "endpoint": "'"${M3_AGG1_HOSTNAME}"':'"${ENVOY_AGG1_PORT}"'",
                    "hostname": "'"${M3_AGG1_HOSTNAME}"'",
                    "port": '"${ENVOY_AGG1_PORT}"'
                },
                {
                    "id": "'"${M3_AGG2_HOSTNAME}"'",
                    "isolation_group": "rack-b",
                    "zone": "embedded",
                    "weight": 1024,
                    "endpoint": "'"${M3_AGG2_HOSTNAME}"':'"${ENVOY_AGG2_PORT}"'",
                    "hostname": "'"${M3_AGG2_HOSTNAME}"'",
                    "port": '"${ENVOY_AGG2_PORT}"'
                }
            ]
        }'
    fi

    echo "Initializing m3msg inbound topic for m3aggregator ingestion from m3coordinators"
    curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: default_env" localhost:${ENVOY_COORD_PORT}/api/v1/topic/init -d '{
        "numberOfShards": 64
    }'

    echo "Adding m3aggregator as a consumer to the aggregator ingest topic"
    curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: default_env" localhost:${ENVOY_COORD_PORT}/api/v1/topic -d '{
    "consumerService": {
        "serviceId": {
        "name": "m3aggregator",
        "environment": "default_env",
        "zone": "embedded"
        },
        "consumptionType": "REPLICATED",
        "messageTtlNanos": "600000000000"
    }
    }' # msgs will be discarded after 600000000000ns = 10mins

    # Create outbound m3msg topic for m3 aggregators to coordinators
    echo "Initializing m3msg outbound topic for m3 aggregators to coordinators"
    curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: default_env" localhost:${ENVOY_COORD_PORT}/api/v1/topic/init -d '{
        "numberOfShards": 64
    }'

    if [[ "$M3AGGREGATOR_DEV_IMG" == "0" ]] || [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_M3AGGREGATOR" == true ]]; then
        prepare_build_cmd "make m3aggregator-linux-amd64"
        echo "Building m3aggregator binary first"
        bash -c "$build_cmd"

        docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS m3aggregator01
    else
        docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3aggregator01
    fi

    if [[ "$USE_AGGREGATOR_HA" == true ]]; then
        # Bring up the second replica
        docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3aggregator02
    fi
else
    echo "Not running aggregator pipeline"
fi

echo "Initializing namespaces"
curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace -d '{
  "name": "metrics_0_30m",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "30m",
      "blockSizeDuration": "10m",
      "bufferFutureDuration": "5m",
      "bufferPastDuration": "5m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "10m"
    },
    "aggregationOptions": {
      "aggregations": [
        {
          "aggregated": false
        }
      ]
    },
    "stagingState": {
      "status": "INITIALIZING"
    }
  }
}'
curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace -d '{
  "name": "metrics_30s_24h",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "24h",
      "blockSizeDuration": "2h",
      "bufferFutureDuration": "10m",
      "bufferPastDuration": "10m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "2h"
    },
    "aggregationOptions": {
      "aggregations": [
        {
          "aggregated": true,
          "attributes": {
            "resolutionDuration": "30s"
          }
        }
      ]
    },
    "stagingState": {
      "status": "INITIALIZING"
    }
  }
}'
echo "Done initializing namespaces"

echo "Validating namespace"
[ "$(curl -sSf localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace | jq .registry.namespaces.metrics_0_30m.indexOptions.enabled)" == true ]
[ "$(curl -sSf localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace | jq .registry.namespaces.metrics_30s_24h.indexOptions.enabled)" == true ]
echo "Done validating namespace"

echo "Waiting for namespaces to be ready"
[ $(curl -sSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"metrics_0_30m\", \"force\": true }" | grep -c true) -eq 1 ]
[ $(curl -sSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"metrics_30s_24h\", \"force\": true }" | grep -c true) -eq 1 ]
echo "Done waiting for namespaces to be ready"

echo "Initializing topology"
if [[ "$USE_MULTI_DB_NODES" = true ]] ; then
    curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 3,
        "instances": [
            {
                "id": "m3db_seed",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_seed:9000",
                "hostname": "m3db_seed",
                "port": 9000
            },
            {
                "id": "m3db_data01",
                "isolation_group": "rack-b",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_data01:9000",
                "hostname": "m3db_data01",
                "port": 9000
            },
            {
                "id": "m3db_data02",
                "isolation_group": "rack-c",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_data02:9000",
                "hostname": "m3db_data02",
                "port": 9000
            }
        ]
    }'
else
    curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 1,
        "instances": [
            {
                "id": "m3db_seed",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_seed:9000",
                "hostname": "m3db_seed",
                "port": 9000
            }
        ]
    }'
fi

echo "Validating topology"
[ "$(curl -sSf localhost:${ENVOY_COORD_PORT}/api/v1/services/m3db/placement | jq .placement.instances.m3db_seed.id)" == '"m3db_seed"' ]
echo "Done validating topology"

echo "Waiting until shards are marked as available"
ATTEMPTS=100 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:'"${ENVOY_COORD_PORT}"'/api/v1/services/m3db/placement | grep -c INITIALIZING)" -eq 0 ]'

if [[ "$USE_AGGREGATOR" = true ]]; then
    echo "Initializing M3Coordinator topology"
    curl -vvvsSf -X POST localhost:${ENVOY_COORD_PORT}/api/v1/services/m3coordinator/placement/init -d '{
        "instances": [
            {
                "id": "'"${M3_COORD_HOSTNAME}"'",
                "zone": "embedded",
                "endpoint": "'"${M3_COORD_HOSTNAME}"':'"${ENVOY_COORD_ALT}"'",
                "hostname": "'"${M3_COORD_HOSTNAME}"'",
                "port": '"${ENVOY_COORD_ALT}"'
            }
        ]
    }'
    echo "Done initializing M3Coordinator topology"

#    echo "Validating M3Coordinator topology"
#    [ "$(curl -sSf localhost:${ENVOY_COORD_PORT}/api/v1/services/m3coordinator/placement | jq .placement.instances.${M3_COORD_HOSTNAME}.id)" == '"'"${M3_COORD_HOSTNAME}"'"' ]
#    echo "Done validating topology"

    # Do this after placement for m3coordinator is created.
    echo "Adding coordinator as a consumer to the aggregator outbound topic"
    curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: default_env" localhost:${ENVOY_COORD_PORT}/api/v1/topic -d '{
        "consumerService": {
                "serviceId": {
                "name": "m3coordinator",
                "environment": "default_env",
                "zone": "embedded"
            },
            "consumptionType": "SHARED",
            "messageTtlNanos": "600000000000"
        }
    }' # msgs will be discarded after 600000000000ns = 10mins

    # Restart with aggregator coordinator config
    docker-compose --compatibility -f docker-compose.yml stop m3coordinator01

    # Note: Use ".tmp" suffix to be git ignored.
    cp ./m3coordinator-aggregator.yml ./m3coordinator.yml.tmp
    echo "@tallen agg config for coordinator"
    if [[ "$USE_MULTIPROCESS_COORDINATOR" = true ]]; then
        cat ./m3coordinator-snippet-multiprocess.yml >> ./m3coordinator.yml.tmp
    fi

    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS m3coordinator01
fi

echo "Starting Prometheus"
if [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_PROMETHEUS" == true ]]; then
   docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS prometheus01
else
   docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS prometheus01
fi

if [[ "$USE_PROMETHEUS_HA" = true ]] ; then
   echo "Starting Prometheus HA replica"
   docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS prometheus02
fi

echo "Starting Grafana"
if [[ "$FORCE_BUILD" == true ]] || [[ "$BUILD_GRAFANA" == true ]]; then
    docker-compose --compatibility -f docker-compose.yml up --build $DOCKER_ARGS grafana
else
    docker-compose --compatibility -f docker-compose.yml up $DOCKER_ARGS grafana
fi

if [[ "$USE_JAEGER" = true ]] ; then
    echo "Jaeger UI available at localhost:16686"
fi
echo "Prometheus available at localhost:9090"
if [[ "$USE_PROMETHEUS_HA" = true ]] ; then
    echo "Prometheus HA replica available at localhost:9091"
fi
echo "Grafana available at localhost:3000"
echo "Run ./stop.sh to shutdown nodes when done"
