package constants

import "time"

const (
	//toolkit
	SCISTREAM  = "scistream"
	DS2HPC     = "ds2hpc"
	NO_TOOLKIT = "none"

	//framework
	RABBITMQ = "rabbitmq"

	//encryption
	TLS_ENCRYPT    = "Tls"
	NO_TLS_ENCRYPT = "NoTls"

	//experiment
	LATENCY_TEST             = "latency"
	THROUGHPUT_TEST          = "throughput"
	PRODUCER_ROLE            = "producer"
	CONSUMER_ROLE            = "consumer"
	COORDINATOR_ROLE         = "coordinator"
	COUNT_TEST_MODE          = "msgCount"
	DURATION_TEST_MODE       = "duration"
	WORK_SHARING_PATTERN     = "work-sharing"
	WORK_SHARING_FB_PATTERN  = "work-sharing-fb"
	BROADCAST_PATTERN        = "broadcast"
	BROADCAST_GATHER_PATTERN = "broadcast-gather"

	//workload
	PULL_CONSUMPTION   = "pull"
	PUSH_CONSUMPTION   = "push"
	MPI_PARALLELISM    = "MPI"
	NONMPI_PARALLELISM = "nonMPI"
	BATCH_SEND         = "batched"
	INDIVIDUAL_SEND    = "individual"
	PAYLOAD_PLAINTEXT  = "plaintext"
	PAYLOAD_JSON       = "json"
	PAYLOAD_BINARY     = "binary"
	PAYLOAD_HDF5       = "hdf5"

	//custom queue types and properties if any
	RESULT_Q                   = "result_Q"
	RESULT_Q_INDEX             = 0
	EMPTY_Q_NAME               = ""
	LATENCY_REQUEST_Q          = "lat_requestQ"   //
	LATENCY_REPLY_Q            = "lat_replyQ"     //
	THROUGHPUT_Q               = "throughputQ"    //
	CONS_MSGCOUNT_Q            = "cons_msgcountQ" //
	CONS_MSGCOUNT_Q_INDEX      = 0                //only one msgcount queue for all consumers
	CONS_SHUTDOWN_Q            = "cons_shutdownQ" //is fanout queue
	CONS_SHUTDOWN_Q_MAX_LENGTH = 1                //1 because each cons has its own queue
	//and only 1 shutdown signal is send to the queue
	PROD_Q_ASSIGN_Q       = "prodQ_assignQ"
	PROD_Q_ASSIGN_Q_INDEX = 0
	CONS_Q_ASSIGN_Q       = "consQ_assignQ"
	CONS_Q_ASSIGN_Q_INDEX = 0
	CONS_DEADLETTER_Q     = "consQ_deadletter"
	DEFAULT_Q_INDEX       = 0

	//msgcount queue consumer settings
	MSGCOUNT_Q_CONS_PREFETCH_COUNT = 1

	//shutdown queue consumer settings
	SHUTDOWN_Q_CONS_PREFETCH_COUNT = 1

	//q assign q consumer settings
	Q_ASSIGN_Q_CONS_PREFETCH_COUNT = 1

	//result throughput q prefetch count
	RESULT_Q_PREFETCH_COUNT = 1

	//dead letter queue prefetch count
	DEADLETTER_Q_CONS_PREFETCH_COUNT = 1

	//reply Queue prefetch count for latency test
	REPLY_Q_PROD_PREFETCH_COUNT = 1
	CORRELATION_ID_LENGTH       = 32

	//custom exchange names and properties
	EMPTY_EX_NAME          = ""
	CONS_SHUTDOWN_EX       = "cons_shutdownEx"
	CONS_SHUTDOWN_EX_INDEX = 0 //only one shutdown queue for all consumers
	CONS_DEADLETTER_EX     = "cons_deadletterEx"
	PROD_REPLY_EX          = "prod_replyEx"
	PROD_REPLY_EX_INDEX    = 0
	BROADCAST_EX           = "broadcastEx"
	BROADCAST_EX_INDEX     = 0

	//signals
	SHUTDOWN_SIGNAL = "shutdown"

	//tunables
	PERSISTENT               = "persistent"
	NONE                     = "none"
	PUB_NO_ACKS              = "no_acks"
	PUB_INDIVIDUAL_ASYNC     = "individual_async"
	PUB_BATCH_SYNC           = "batch_sync"
	PUB_INDIVIDUAL_SYNC      = "individual_sync"
	CONS_ACK_AUTO            = "automatic"
	CONS_ACK_MANUAL          = "manual"
	CONS_ACK_MANUAL_SINGLE   = "single"
	CONS_ACK_MANUAL_MULTIPLE = "multiple"
	CONS_PREFETCH_SIZE       = 0
	CONS_GLOBAL_PREFETCH     = false

	//rabbitmq specific
	PLAINTEXT_CONTENT = "text/plain"
	JSON_CONTENT      = "application/json"
	HDF5_CONTENT      = "application/x-hdf5"
	BINARY_CONTENT    = "application/octet-stream"
	//queue types and args for Rabbitmq
	QUORUM_Q  = "quorum"
	STREAM_Q  = "stream"
	CLASSIC_Q = "classic"
	//exchange types and args for Rabbitmq
	FANOUT_EX                      = "fanout"
	DIRECT_EX                      = "direct"
	DEAD_LETTER_STRATEGY           = "at-least-once"
	Q_OVERFLOW_TYPE_DROP_HEAD      = "drop-head"
	Q_OVERFLOW_TYPE_REJECT_PUBLISH = "reject-publish"
	Q_OVERFLOW_TYPE_DEAD_LETTER    = "dead-letter"

	//s3m cluster specific
	//assuming cluster ram-gbs is set to max allowed - 32gbs
	MAX_MEMORY_QUEUE_SIZE = 26000000000

	//reliablilty
	PROD_RETRIES   = 1000
	RETRY_INTERVAL = 200 * time.Millisecond //in millisecond

	//helpers
	LITTLE_ENDIAN    = "Little-endian"
	BIG_ENDIAN       = "Big-endian"
	CONS_MSG_DIVIDER = 2

	//result storage
	RESULT_STORE_PATH = "../latency_results/"
)
