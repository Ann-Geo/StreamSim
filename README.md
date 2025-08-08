# Data Streaming to HPC Simulator for Scitific Workflows

### Summary

A Golang-based simulator that simulates the streaming behaviours of scientific workflows in a data streaming to HPC environment. It utilizes the amqp091-go (version 1.10.0) RabbitMQ AMQP client library to implement RabbitMQ APIs. The simulator accepts the streaming characteristics of workflows, type of streaming architecture (e.g., DTS, PRS, or MSS), streaming service specific parameters (e.g., type of acknowledgements, number of queues, prefetch count), experiment configurations (e.g., number of producers and consumers, message count, experiment duration), and infrastructure or toolkit specific options (e.g., url for connection, number of connections, TLS). For a given message count or test duration, the simulator runs the experiment with the specified number of producers and consumers. Each producer is identical in function and is responsible for generating workload based on the input workload characteristics and sending data to the RabbitMQ server according to the specified parameters. Similarly, each consumer is identical and is designed to receive messages from the RabbitMQ server based on the same set of parameters. 

### Pre-requisites
* Requires golang version of atleast go1.23.12
* Slurm
* OpenMPI

### Build instructions
* export GOROOT=/path/to/go
* export PATH=$GOROOT/bin:$PATH
* export PATH=$PATH:/path/to/go/binary
* go build -tags mpi -o main

### Creating a RabbitMQ cluster

* To create a RabbitMQ cluster using S3M API (https://s3m.apps.olivine.ccs.ornl.gov/docs/gen/getting-started.html) first obtain a TOKEN using, https://s3m-myolcf.apps.olivine.ccs.ornl.gov
* Export the token as TOKEN=<token>
* Provision a rabbitmq cluster using:
{curl -X POST "https://s3m.apps.olivine.ccs.ornl.gov/olcf/v1alpha/ streaming/rabbitmq/provision\_cluster" -H "Authorization: TOKEN" -H "Content-Type: application/json" -d '{"kind": "general", "name": "rabbitmq", "resourceSettings":{"cpus": 12, "ram-gbs": 32, "nodes": 3, "max-msg-size": 536870912} }'}
This will give a url that can be used to connect to the RabbitMQ cluster, specify that url in config/framework/rabbitmq/ds2hpc_rabbitmq_config_olivine.json file in the "amqpsUrl:" field.

### Running a simple test
* This test will start 2 consumers, 2 producers and a coordinator.
* Start consumers first:
srun -n 2 ./main -f rabbitmq -e config/experiment/throughput/2/experiment_config_consumer.json -fc config/framework/rabbitmq/ds2hpc_rabbitmq_config_olivine.json -t config/tunables/deleria/combination_adv/rmq_tunable_ind_sync.json -w config/workload/deleria/2/deleria.json
* Start producer:
srun -n 2 ./main -f rabbitmq -e config/experiment/throughput/2/experiment_config_producer.json -fc config/framework/rabbitmq/ds2hpc_rabbitmq_config_olivine.json -t config/tunables/deleria/combination_adv/rmq_tunable_ind_sync.json -w config/workload/deleria/2/deleria.json
* Start coordinator:
./main -f rabbitmq -e config/experiment/throughput/2/experiment_config_coordinator.json -fc config/framework/rabbitmq/ds2hpc_rabbitmq_config_olivine.json -t config/tunables/deleria/combination_adv/rmq_tunable_ind_sync.json -w config/workload/deleria/2/deleria.json
