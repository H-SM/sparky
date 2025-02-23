Following is the complete output for the script `sparky.py`: 

```python
=== Creating RDDs ===
RDD from list: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                                  

RDD from text file: ['Hello This is HSM.', 'This is a sample spark RDD over text file.', 'Sparky is out to go wild and zap everyone.']

RDD from CSV: ['id,name,mob', '261723,Harman,8792327121', '223173,Sid,87923228201', '261281,Saksham,8792317121']

=== Demonstrating Transformations ===
Squared numbers: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

Even numbers: [2, 4, 6, 8, 10]

Words from text: ['Hello', 'This', 'is', 'HSM.', 'This', 'is', 'a', 'sample', 'spark', 'RDD', 'over', 'text', 'file.', 'Sparky', 'is', 'out', 'to', 'go', 'wild', 'and', 'zap', 'everyone.']

=== Demonstrating Actions ===
Collected elements: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

Total elements: 10

Sum of elements: 55

First 5 elements: [1, 2, 3, 4, 5]
```

## Assignment 2 console - 

```
➜  spark_projects git:(main) ✗ spark-submit --driver-memory 4g TaxiNYC.py
25/02/23 20:18:15 INFO SparkContext: Running Spark version 3.5.4
25/02/23 20:18:15 INFO SparkContext: OS info Mac OS X, 15.3, aarch64
25/02/23 20:18:15 INFO SparkContext: Java version 17.0.14
25/02/23 20:18:15 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/02/23 20:18:15 INFO ResourceUtils: ==============================================================
25/02/23 20:18:15 INFO ResourceUtils: No custom resources configured for spark.driver.
25/02/23 20:18:15 INFO ResourceUtils: ==============================================================
25/02/23 20:18:15 INFO SparkContext: Submitted application: NYC Taxi Analysis
25/02/23 20:18:15 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/02/23 20:18:15 INFO ResourceProfile: Limiting resource is cpu
25/02/23 20:18:15 INFO ResourceProfileManager: Added ResourceProfile id: 0
25/02/23 20:18:15 INFO SecurityManager: Changing view acls to: harmansinghmalhotra
25/02/23 20:18:15 INFO SecurityManager: Changing modify acls to: harmansinghmalhotra
25/02/23 20:18:15 INFO SecurityManager: Changing view acls groups to: 
25/02/23 20:18:15 INFO SecurityManager: Changing modify acls groups to: 
25/02/23 20:18:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: harmansinghmalhotra; groups with view permissions: EMPTY; users with modify permissions: harmansinghmalhotra; groups with modify permissions: EMPTY
25/02/23 20:18:15 INFO Utils: Successfully started service 'sparkDriver' on port 49725.
25/02/23 20:18:15 INFO SparkEnv: Registering MapOutputTracker
25/02/23 20:18:15 INFO SparkEnv: Registering BlockManagerMaster
25/02/23 20:18:15 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
25/02/23 20:18:15 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
25/02/23 20:18:15 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/02/23 20:18:15 INFO DiskBlockManager: Created local directory at /private/var/folders/n7/gfjkgkmj2tj4rkl8f4gx_bvh0000gn/T/blockmgr-052abf26-2af4-4fd6-95b9-b3a7972d5130
25/02/23 20:18:15 INFO MemoryStore: MemoryStore started with capacity 2.2 GiB
25/02/23 20:18:15 INFO SparkEnv: Registering OutputCommitCoordinator
25/02/23 20:18:15 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
25/02/23 20:18:15 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/02/23 20:18:15 INFO Executor: Starting executor ID driver on host 192.168.1.48
25/02/23 20:18:15 INFO Executor: OS info Mac OS X, 15.3, aarch64
25/02/23 20:18:15 INFO Executor: Java version 17.0.14
25/02/23 20:18:15 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
25/02/23 20:18:15 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@2f1e7e5c for default.
25/02/23 20:18:15 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 49730.
25/02/23 20:18:15 INFO NettyBlockTransferService: Server created on 192.168.1.48:49730
25/02/23 20:18:15 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
25/02/23 20:18:15 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.48, 49730, None)
25/02/23 20:18:15 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.1.48:49730 with 2.2 GiB RAM, BlockManagerId(driver, 192.168.1.48, 49730, None)
25/02/23 20:18:15 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.48, 49730, None)
25/02/23 20:18:15 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.48, 49730, None)
25/02/23 20:18:15 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
25/02/23 20:18:15 INFO SharedState: Warehouse path is 'file:/Users/harmansinghmalhotra/spark_projects/spark-warehouse'.
25/02/23 20:18:16 INFO InMemoryFileIndex: It took 14 ms to list leaf files for 1 paths.
25/02/23 20:18:16 INFO SparkContext: Starting job: parquet at NativeMethodAccessorImpl.java:0
25/02/23 20:18:16 INFO DAGScheduler: Got job 0 (parquet at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:18:16 INFO DAGScheduler: Final stage: ResultStage 0 (parquet at NativeMethodAccessorImpl.java:0)
25/02/23 20:18:16 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:18:16 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:16 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[1] at parquet at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:18:16 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 103.3 KiB, free 2.2 GiB)
25/02/23 20:18:16 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 37.2 KiB, free 2.2 GiB)
25/02/23 20:18:16 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.1.48:49730 (size: 37.2 KiB, free: 2.2 GiB)
25/02/23 20:18:16 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:16 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[1] at parquet at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:18:16 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
25/02/23 20:18:16 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9172 bytes) 
25/02/23 20:18:16 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
25/02/23 20:18:16 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2470 bytes result sent to driver
25/02/23 20:18:16 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 126 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:18:16 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
25/02/23 20:18:16 INFO DAGScheduler: ResultStage 0 (parquet at NativeMethodAccessorImpl.java:0) finished in 0.328 s
25/02/23 20:18:16 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:16 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
25/02/23 20:18:16 INFO DAGScheduler: Job 0 finished: parquet at NativeMethodAccessorImpl.java:0, took 0.342566 s
25/02/23 20:18:16 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 192.168.1.48:49730 in memory (size: 37.2 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:17 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 64.99075 ms
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 200.9 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 34.9 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.1.48:49730 (size: 34.9 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO SparkContext: Created broadcast 1 from count at NativeMethodAccessorImpl.java:0
25/02/23 20:18:17 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:17 INFO DAGScheduler: Registering RDD 5 (count at NativeMethodAccessorImpl.java:0) as input to shuffle 0
25/02/23 20:18:17 INFO DAGScheduler: Got map stage job 1 (count at NativeMethodAccessorImpl.java:0) with 8 output partitions
25/02/23 20:18:17 INFO DAGScheduler: Final stage: ShuffleMapStage 1 (count at NativeMethodAccessorImpl.java:0)
25/02/23 20:18:17 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:18:17 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:17 INFO DAGScheduler: Submitting ShuffleMapStage 1 (MapPartitionsRDD[5] at count at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 17.0 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 7.8 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.1.48:49730 (size: 7.8 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:17 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 1 (MapPartitionsRDD[5] at count at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:18:17 INFO TaskSchedulerImpl: Adding task set 1.0 with 8 tasks resource profile 0
25/02/23 20:18:17 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 2) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 3) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 4) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 5) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 6) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 7) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 8) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:17 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
25/02/23 20:18:17 INFO Executor: Running task 1.0 in stage 1.0 (TID 2)
25/02/23 20:18:17 INFO Executor: Running task 2.0 in stage 1.0 (TID 3)
25/02/23 20:18:17 INFO Executor: Running task 3.0 in stage 1.0 (TID 4)
25/02/23 20:18:17 INFO Executor: Running task 4.0 in stage 1.0 (TID 5)
25/02/23 20:18:17 INFO Executor: Running task 5.0 in stage 1.0 (TID 6)
25/02/23 20:18:17 INFO Executor: Running task 6.0 in stage 1.0 (TID 7)
25/02/23 20:18:17 INFO Executor: Running task 7.0 in stage 1.0 (TID 8)
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 6.700458 ms
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:18:17 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:18:17 INFO Executor: Finished task 2.0 in stage 1.0 (TID 3). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 7.0 in stage 1.0 (TID 8). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 5.0 in stage 1.0 (TID 6). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 4.0 in stage 1.0 (TID 5). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 6.0 in stage 1.0 (TID 7). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 84 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:18:17 INFO Executor: Finished task 1.0 in stage 1.0 (TID 2). 2136 bytes result sent to driver
25/02/23 20:18:17 INFO Executor: Finished task 3.0 in stage 1.0 (TID 4). 2179 bytes result sent to driver
25/02/23 20:18:17 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 5) in 84 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 6.0 in stage 1.0 (TID 7) in 84 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 3) in 85 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 7.0 in stage 1.0 (TID 8) in 85 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 4) in 85 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 2) in 87 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:18:17 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 6) in 86 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:18:17 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
25/02/23 20:18:17 INFO DAGScheduler: ShuffleMapStage 1 (count at NativeMethodAccessorImpl.java:0) finished in 0.104 s
25/02/23 20:18:17 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:18:17 INFO DAGScheduler: running: Set()
25/02/23 20:18:17 INFO DAGScheduler: waiting: Set()
25/02/23 20:18:17 INFO DAGScheduler: failed: Set()
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 4.958417 ms
25/02/23 20:18:17 INFO SparkContext: Starting job: count at NativeMethodAccessorImpl.java:0
25/02/23 20:18:17 INFO DAGScheduler: Got job 2 (count at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:18:17 INFO DAGScheduler: Final stage: ResultStage 3 (count at NativeMethodAccessorImpl.java:0)
25/02/23 20:18:17 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
25/02/23 20:18:17 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:17 INFO DAGScheduler: Submitting ResultStage 3 (MapPartitionsRDD[8] at count at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 12.5 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.1.48:49730 (size: 5.9 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 192.168.1.48:49730 in memory (size: 7.8 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:17 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 3 (MapPartitionsRDD[8] at count at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:18:17 INFO TaskSchedulerImpl: Adding task set 3.0 with 1 tasks resource profile 0
25/02/23 20:18:17 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 9) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8999 bytes) 
25/02/23 20:18:17 INFO Executor: Running task 0.0 in stage 3.0 (TID 9)
25/02/23 20:18:17 INFO ShuffleBlockFetcherIterator: Getting 8 (480.0 B) non-empty blocks including 8 (480.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:18:17 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 4 ms
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 5.875916 ms
25/02/23 20:18:17 INFO Executor: Finished task 0.0 in stage 3.0 (TID 9). 3995 bytes result sent to driver
25/02/23 20:18:17 INFO TaskSetManager: Finished task 0.0 in stage 3.0 (TID 9) in 61 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:18:17 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
25/02/23 20:18:17 INFO DAGScheduler: ResultStage 3 (count at NativeMethodAccessorImpl.java:0) finished in 0.067 s
25/02/23 20:18:17 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:17 INFO TaskSchedulerImpl: Killing all running tasks in stage 3: Stage finished
25/02/23 20:18:17 INFO DAGScheduler: Job 2 finished: count at NativeMethodAccessorImpl.java:0, took 0.071750 s
Number of records: 3066766
Schema:
root
 |-- VendorID: long (nullable = true)
 |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
 |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: double (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: long (nullable = true)
 |-- DOLocationID: long (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)

25/02/23 20:18:17 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:17 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 25.517417 ms
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 203.6 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Removed broadcast_3_piece0 on 192.168.1.48:49730 in memory (size: 5.9 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 35.8 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 192.168.1.48:49730 (size: 35.8 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO SparkContext: Created broadcast 4 from javaToPython at NativeMethodAccessorImpl.java:0
25/02/23 20:18:17 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:17 INFO InMemoryFileIndex: It took 0 ms to list leaf files for 1 paths.
25/02/23 20:18:17 INFO InMemoryFileIndex: It took 0 ms to list leaf files for 1 paths.
25/02/23 20:18:17 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:17 INFO FileSourceStrategy: Post-Scan Filters: (length(trim(value#62, None)) > 0)
25/02/23 20:18:17 INFO CodeGenerator: Code generated in 8.935625 ms
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 199.6 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 34.3 KiB, free 2.2 GiB)
25/02/23 20:18:17 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 192.168.1.48:49730 (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:18:17 INFO SparkContext: Created broadcast 5 from csv at NativeMethodAccessorImpl.java:0
25/02/23 20:18:17 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:18 INFO SparkContext: Starting job: csv at NativeMethodAccessorImpl.java:0
25/02/23 20:18:18 INFO DAGScheduler: Got job 3 (csv at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:18:18 INFO DAGScheduler: Final stage: ResultStage 4 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:18:18 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:18:18 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:18 INFO DAGScheduler: Submitting ResultStage 4 (MapPartitionsRDD[27] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 13.5 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 6.4 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 192.168.1.48:49730 (size: 6.4 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:18 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 4 (MapPartitionsRDD[27] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:18:18 INFO TaskSchedulerImpl: Adding task set 4.0 with 1 tasks resource profile 0
25/02/23 20:18:18 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 10) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:18 INFO Executor: Running task 0.0 in stage 4.0 (TID 10)
25/02/23 20:18:18 INFO CodeGenerator: Code generated in 3.913667 ms
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/taxi_zone_lookup.csv, range: 0-12331, partition values: [empty row]
25/02/23 20:18:18 INFO CodeGenerator: Code generated in 3.946167 ms
25/02/23 20:18:18 INFO Executor: Finished task 0.0 in stage 4.0 (TID 10). 1659 bytes result sent to driver
25/02/23 20:18:18 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 10) in 37 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:18:18 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool 
25/02/23 20:18:18 INFO DAGScheduler: ResultStage 4 (csv at NativeMethodAccessorImpl.java:0) finished in 0.045 s
25/02/23 20:18:18 INFO DAGScheduler: Job 3 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 4: Stage finished
25/02/23 20:18:18 INFO DAGScheduler: Job 3 finished: csv at NativeMethodAccessorImpl.java:0, took 0.047197 s
25/02/23 20:18:18 INFO CodeGenerator: Code generated in 2.925917 ms
25/02/23 20:18:18 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:18 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 199.6 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 34.3 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 192.168.1.48:49730 (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO SparkContext: Created broadcast 7 from csv at NativeMethodAccessorImpl.java:0
25/02/23 20:18:18 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:18 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:18 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 199.5 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 34.3 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 192.168.1.48:49730 (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO SparkContext: Created broadcast 8 from javaToPython at NativeMethodAccessorImpl.java:0
25/02/23 20:18:18 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:18 INFO SparkContext: Starting job: sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57
25/02/23 20:18:18 INFO DAGScheduler: Got job 4 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) with 8 output partitions
25/02/23 20:18:18 INFO DAGScheduler: Final stage: ResultStage 5 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57)
25/02/23 20:18:18 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:18:18 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:18 INFO DAGScheduler: Submitting ResultStage 5 (PythonRDD[46] at sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57), which has no missing parents
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 13.3 KiB, free 2.2 GiB)
25/02/23 20:18:18 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 192.168.1.48:49730 (size: 13.3 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:18 INFO BlockManagerInfo: Removed broadcast_6_piece0 on 192.168.1.48:49730 in memory (size: 6.4 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO DAGScheduler: Submitting 8 missing tasks from ResultStage 5 (PythonRDD[46] at sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:18:18 INFO TaskSchedulerImpl: Adding task set 5.0 with 8 tasks resource profile 0
25/02/23 20:18:18 INFO TaskSetManager: Starting task 0.0 in stage 5.0 (TID 11) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO BlockManagerInfo: Removed broadcast_5_piece0 on 192.168.1.48:49730 in memory (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO TaskSetManager: Starting task 1.0 in stage 5.0 (TID 12) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 2.0 in stage 5.0 (TID 13) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 3.0 in stage 5.0 (TID 14) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 4.0 in stage 5.0 (TID 15) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 5.0 in stage 5.0 (TID 16) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 6.0 in stage 5.0 (TID 17) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO TaskSetManager: Starting task 7.0 in stage 5.0 (TID 18) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:18 INFO Executor: Running task 0.0 in stage 5.0 (TID 11)
25/02/23 20:18:18 INFO Executor: Running task 3.0 in stage 5.0 (TID 14)
25/02/23 20:18:18 INFO Executor: Running task 2.0 in stage 5.0 (TID 13)
25/02/23 20:18:18 INFO Executor: Running task 4.0 in stage 5.0 (TID 15)
25/02/23 20:18:18 INFO Executor: Running task 5.0 in stage 5.0 (TID 16)
25/02/23 20:18:18 INFO Executor: Running task 6.0 in stage 5.0 (TID 17)
25/02/23 20:18:18 INFO Executor: Running task 7.0 in stage 5.0 (TID 18)
25/02/23 20:18:18 INFO Executor: Running task 1.0 in stage 5.0 (TID 12)
25/02/23 20:18:18 INFO BlockManagerInfo: Removed broadcast_7_piece0 on 192.168.1.48:49730 in memory (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO CodeGenerator: Code generated in 14.718083 ms
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:18:18 INFO PythonRunner: Times: total = 313, boot = 270, init = 43, finish = 0
25/02/23 20:18:18 INFO PythonRunner: Times: total = 313, boot = 272, init = 41, finish = 0
25/02/23 20:18:18 INFO PythonRunner: Times: total = 331, boot = 280, init = 51, finish = 0
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:18:18 INFO Executor: Finished task 1.0 in stage 5.0 (TID 12). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO Executor: Finished task 7.0 in stage 5.0 (TID 18). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO TaskSetManager: Finished task 1.0 in stage 5.0 (TID 12) in 374 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:18:18 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:18:18 INFO TaskSetManager: Finished task 7.0 in stage 5.0 (TID 18) in 374 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:18:18 INFO PythonAccumulatorV2: Connected to AccumulatorServer at host: 127.0.0.1 port: 49731
25/02/23 20:18:18 INFO Executor: Finished task 5.0 in stage 5.0 (TID 16). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO TaskSetManager: Finished task 5.0 in stage 5.0 (TID 16) in 378 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:18:18 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:18:18 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 192.168.1.48:49730 in memory (size: 34.9 KiB, free: 2.2 GiB)
25/02/23 20:18:18 INFO PythonRunner: Times: total = 367, boot = 291, init = 76, finish = 0
25/02/23 20:18:18 INFO PythonRunner: Times: total = 368, boot = 304, init = 64, finish = 0
25/02/23 20:18:18 INFO PythonRunner: Times: total = 376, boot = 323, init = 53, finish = 0
25/02/23 20:18:18 INFO Executor: Finished task 0.0 in stage 5.0 (TID 11). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO Executor: Finished task 2.0 in stage 5.0 (TID 13). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO Executor: Finished task 6.0 in stage 5.0 (TID 17). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO TaskSetManager: Finished task 0.0 in stage 5.0 (TID 11) in 412 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:18:18 INFO TaskSetManager: Finished task 6.0 in stage 5.0 (TID 17) in 412 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:18:18 INFO TaskSetManager: Finished task 2.0 in stage 5.0 (TID 13) in 413 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:18:18 INFO PythonRunner: Times: total = 383, boot = 334, init = 49, finish = 0
25/02/23 20:18:18 INFO Executor: Finished task 4.0 in stage 5.0 (TID 15). 1896 bytes result sent to driver
25/02/23 20:18:18 INFO TaskSetManager: Finished task 4.0 in stage 5.0 (TID 15) in 417 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:18:34 INFO PythonRunner: Times: total = 16341, boot = 275, init = 222, finish = 15844
25/02/23 20:18:34 INFO Executor: Finished task 3.0 in stage 5.0 (TID 14). 1985 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 3.0 in stage 5.0 (TID 14) in 16377 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:18:34 INFO TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool 
25/02/23 20:18:34 INFO DAGScheduler: ResultStage 5 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) finished in 16.384 s
25/02/23 20:18:34 INFO DAGScheduler: Job 4 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:34 INFO TaskSchedulerImpl: Killing all running tasks in stage 5: Stage finished
25/02/23 20:18:34 INFO DAGScheduler: Job 4 finished: sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57, took 16.386938 s
25/02/23 20:18:34 INFO SparkContext: Starting job: sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57
25/02/23 20:18:34 INFO DAGScheduler: Got job 5 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) with 8 output partitions
25/02/23 20:18:34 INFO DAGScheduler: Final stage: ResultStage 6 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57)
25/02/23 20:18:34 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:18:34 INFO DAGScheduler: Missing parents: List()
25/02/23 20:18:34 INFO DAGScheduler: Submitting ResultStage 6 (PythonRDD[47] at sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57), which has no missing parents
25/02/23 20:18:34 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 33.6 KiB, free 2.2 GiB)
25/02/23 20:18:34 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 12.4 KiB, free 2.2 GiB)
25/02/23 20:18:34 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on 192.168.1.48:49730 (size: 12.4 KiB, free: 2.2 GiB)
25/02/23 20:18:34 INFO SparkContext: Created broadcast 10 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:34 INFO DAGScheduler: Submitting 8 missing tasks from ResultStage 6 (PythonRDD[47] at sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:18:34 INFO TaskSchedulerImpl: Adding task set 6.0 with 8 tasks resource profile 0
25/02/23 20:18:34 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 19) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 1.0 in stage 6.0 (TID 20) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 2.0 in stage 6.0 (TID 21) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 3.0 in stage 6.0 (TID 22) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 4.0 in stage 6.0 (TID 23) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 5.0 in stage 6.0 (TID 24) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 6.0 in stage 6.0 (TID 25) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO TaskSetManager: Starting task 7.0 in stage 6.0 (TID 26) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:18:34 INFO Executor: Running task 2.0 in stage 6.0 (TID 21)
25/02/23 20:18:34 INFO Executor: Running task 1.0 in stage 6.0 (TID 20)
25/02/23 20:18:34 INFO Executor: Running task 4.0 in stage 6.0 (TID 23)
25/02/23 20:18:34 INFO Executor: Running task 5.0 in stage 6.0 (TID 24)
25/02/23 20:18:34 INFO Executor: Running task 6.0 in stage 6.0 (TID 25)
25/02/23 20:18:34 INFO Executor: Running task 7.0 in stage 6.0 (TID 26)
25/02/23 20:18:34 INFO Executor: Running task 0.0 in stage 6.0 (TID 19)
25/02/23 20:18:34 INFO Executor: Running task 3.0 in stage 6.0 (TID 22)
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:18:34 INFO PythonRunner: Times: total = 30, boot = -16023, init = 16053, finish = 0
25/02/23 20:18:34 INFO Executor: Finished task 1.0 in stage 6.0 (TID 20). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 1.0 in stage 6.0 (TID 20) in 40 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:18:34 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:18:34 INFO PythonRunner: Times: total = 43, boot = -15993, init = 16036, finish = 0
25/02/23 20:18:34 INFO Executor: Finished task 0.0 in stage 6.0 (TID 19). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 19) in 56 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:18:34 INFO PythonRunner: Times: total = 48, boot = -16066, init = 16114, finish = 0
25/02/23 20:18:34 INFO PythonRunner: Times: total = 64, boot = -16010, init = 16074, finish = 0
25/02/23 20:18:34 INFO Executor: Finished task 6.0 in stage 6.0 (TID 25). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO Executor: Finished task 4.0 in stage 6.0 (TID 23). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO PythonRunner: Times: total = 64, boot = -15997, init = 16061, finish = 0
25/02/23 20:18:34 INFO Executor: Finished task 2.0 in stage 6.0 (TID 21). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 4.0 in stage 6.0 (TID 23) in 74 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:18:34 INFO TaskSetManager: Finished task 2.0 in stage 6.0 (TID 21) in 75 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:18:34 INFO TaskSetManager: Finished task 6.0 in stage 6.0 (TID 25) in 75 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:18:34 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:18:34 INFO PythonRunner: Times: total = 80, boot = -37, init = 117, finish = 0
25/02/23 20:18:34 INFO Executor: Finished task 7.0 in stage 6.0 (TID 26). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 7.0 in stage 6.0 (TID 26) in 87 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:18:34 INFO PythonRunner: Times: total = 86, boot = -16059, init = 16144, finish = 1
25/02/23 20:18:34 INFO Executor: Finished task 5.0 in stage 6.0 (TID 24). 1856 bytes result sent to driver
25/02/23 20:18:34 INFO TaskSetManager: Finished task 5.0 in stage 6.0 (TID 24) in 91 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:18:50 INFO PythonRunner: Times: total = 16382, boot = -16009, init = 16121, finish = 16270
25/02/23 20:18:50 INFO Executor: Finished task 3.0 in stage 6.0 (TID 22). 3502 bytes result sent to driver
25/02/23 20:18:50 INFO TaskSetManager: Finished task 3.0 in stage 6.0 (TID 22) in 16388 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:18:50 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
25/02/23 20:18:50 INFO DAGScheduler: ResultStage 6 (sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57) finished in 16.392 s
25/02/23 20:18:50 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:50 INFO TaskSchedulerImpl: Killing all running tasks in stage 6: Stage finished
25/02/23 20:18:50 INFO DAGScheduler: Job 5 finished: sortBy at /Users/harmansinghmalhotra/spark_projects/TaxiNYC.py:57, took 16.394632 s
25/02/23 20:18:51 INFO Instrumentation: [b1871d78] Stage class: LinearRegression
25/02/23 20:18:51 INFO Instrumentation: [b1871d78] Stage uid: LinearRegression_11e007acbead
25/02/23 20:18:51 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:51 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 40.338792 ms
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:18:51 INFO SparkContext: Created broadcast 11 from rdd at Instrumentation.scala:62
25/02/23 20:18:51 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:51 INFO Instrumentation: [b1871d78] training: numPartitions=8 storageLevel=StorageLevel(1 replicas)
25/02/23 20:18:51 INFO Instrumentation: [b1871d78] {"labelCol":"fare_amount","featuresCol":"features"}
25/02/23 20:18:51 INFO Instrumentation: [b1871d78] {"numFeatures":4}
25/02/23 20:18:51 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:51 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 21.70675 ms
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:18:51 INFO SparkContext: Created broadcast 12 from rdd at LinearRegression.scala:348
25/02/23 20:18:51 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:51 WARN Instrumentation: [b1871d78] regParam is zero, which might cause numerical instability and overfitting.
25/02/23 20:18:51 INFO SparkContext: Starting job: treeAggregate at WeightedLeastSquares.scala:107
25/02/23 20:18:51 INFO DAGScheduler: Registering RDD 66 (treeAggregate at WeightedLeastSquares.scala:107) as input to shuffle 1
25/02/23 20:18:51 INFO DAGScheduler: Got job 6 (treeAggregate at WeightedLeastSquares.scala:107) with 2 output partitions
25/02/23 20:18:51 INFO DAGScheduler: Final stage: ResultStage 8 (treeAggregate at WeightedLeastSquares.scala:107)
25/02/23 20:18:51 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 7)
25/02/23 20:18:51 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 7)
25/02/23 20:18:51 INFO DAGScheduler: Submitting ShuffleMapStage 7 (MapPartitionsRDD[66] at treeAggregate at WeightedLeastSquares.scala:107), which has no missing parents
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 85.6 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 33.2 KiB, free 2.2 GiB)
25/02/23 20:18:51 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on 192.168.1.48:49730 (size: 33.2 KiB, free: 2.2 GiB)
25/02/23 20:18:51 INFO SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:51 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 7 (MapPartitionsRDD[66] at treeAggregate at WeightedLeastSquares.scala:107) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:18:51 INFO TaskSchedulerImpl: Adding task set 7.0 with 8 tasks resource profile 0
25/02/23 20:18:51 INFO TaskSetManager: Starting task 0.0 in stage 7.0 (TID 27) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 1.0 in stage 7.0 (TID 28) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 2.0 in stage 7.0 (TID 29) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 3.0 in stage 7.0 (TID 30) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 4.0 in stage 7.0 (TID 31) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 5.0 in stage 7.0 (TID 32) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 6.0 in stage 7.0 (TID 33) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO TaskSetManager: Starting task 7.0 in stage 7.0 (TID 34) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:51 INFO Executor: Running task 1.0 in stage 7.0 (TID 28)
25/02/23 20:18:51 INFO Executor: Running task 3.0 in stage 7.0 (TID 30)
25/02/23 20:18:51 INFO Executor: Running task 6.0 in stage 7.0 (TID 33)
25/02/23 20:18:51 INFO Executor: Running task 5.0 in stage 7.0 (TID 32)
25/02/23 20:18:51 INFO Executor: Running task 2.0 in stage 7.0 (TID 29)
25/02/23 20:18:51 INFO Executor: Running task 4.0 in stage 7.0 (TID 31)
25/02/23 20:18:51 INFO Executor: Running task 7.0 in stage 7.0 (TID 34)
25/02/23 20:18:51 INFO Executor: Running task 0.0 in stage 7.0 (TID 27)
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 19.201375 ms
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 9.908709 ms
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 3.51175 ms
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 3.409458 ms
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:18:51 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:18:51 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:18:51 INFO Executor: Finished task 1.0 in stage 7.0 (TID 28). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO Executor: Finished task 4.0 in stage 7.0 (TID 31). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO Executor: Finished task 2.0 in stage 7.0 (TID 29). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO TaskSetManager: Finished task 4.0 in stage 7.0 (TID 31) in 103 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:18:51 INFO TaskSetManager: Finished task 2.0 in stage 7.0 (TID 29) in 103 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:18:51 INFO TaskSetManager: Finished task 1.0 in stage 7.0 (TID 28) in 103 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:18:51 INFO Executor: Finished task 7.0 in stage 7.0 (TID 34). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO TaskSetManager: Finished task 7.0 in stage 7.0 (TID 34) in 104 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:18:51 INFO Executor: Finished task 5.0 in stage 7.0 (TID 32). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO TaskSetManager: Finished task 5.0 in stage 7.0 (TID 32) in 105 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:18:51 INFO Executor: Finished task 0.0 in stage 7.0 (TID 27). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO Executor: Finished task 6.0 in stage 7.0 (TID 33). 2305 bytes result sent to driver
25/02/23 20:18:51 INFO TaskSetManager: Finished task 6.0 in stage 7.0 (TID 33) in 106 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:18:51 INFO TaskSetManager: Finished task 0.0 in stage 7.0 (TID 27) in 106 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:18:51 INFO CodeGenerator: Code generated in 3.459167 ms
25/02/23 20:18:51 INFO BlockManagerInfo: Removed broadcast_9_piece0 on 192.168.1.48:49730 in memory (size: 13.3 KiB, free: 2.2 GiB)
25/02/23 20:18:51 INFO BlockManagerInfo: Removed broadcast_10_piece0 on 192.168.1.48:49730 in memory (size: 12.4 KiB, free: 2.2 GiB)
25/02/23 20:18:55 INFO CodeGenerator: Code generated in 2.197333 ms
25/02/23 20:18:55 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
25/02/23 20:18:55 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.VectorBLAS
25/02/23 20:18:56 INFO Executor: Finished task 3.0 in stage 7.0 (TID 30). 2348 bytes result sent to driver
25/02/23 20:18:56 INFO TaskSetManager: Finished task 3.0 in stage 7.0 (TID 30) in 4718 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:18:56 INFO TaskSchedulerImpl: Removed TaskSet 7.0, whose tasks have all completed, from pool 
25/02/23 20:18:56 INFO DAGScheduler: ShuffleMapStage 7 (treeAggregate at WeightedLeastSquares.scala:107) finished in 4.724 s
25/02/23 20:18:56 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:18:56 INFO DAGScheduler: running: Set()
25/02/23 20:18:56 INFO DAGScheduler: waiting: Set(ResultStage 8)
25/02/23 20:18:56 INFO DAGScheduler: failed: Set()
25/02/23 20:18:56 INFO DAGScheduler: Submitting ResultStage 8 (MapPartitionsRDD[68] at treeAggregate at WeightedLeastSquares.scala:107), which has no missing parents
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 86.6 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 34.0 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on 192.168.1.48:49730 (size: 34.0 KiB, free: 2.2 GiB)
25/02/23 20:18:56 INFO SparkContext: Created broadcast 14 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:56 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 8 (MapPartitionsRDD[68] at treeAggregate at WeightedLeastSquares.scala:107) (first 15 tasks are for partitions Vector(0, 1))
25/02/23 20:18:56 INFO TaskSchedulerImpl: Adding task set 8.0 with 2 tasks resource profile 0
25/02/23 20:18:56 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 35) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8817 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 1.0 in stage 8.0 (TID 36) (192.168.1.48, executor driver, partition 1, NODE_LOCAL, 8817 bytes) 
25/02/23 20:18:56 INFO Executor: Running task 0.0 in stage 8.0 (TID 35)
25/02/23 20:18:56 INFO Executor: Running task 1.0 in stage 8.0 (TID 36)
25/02/23 20:18:56 INFO ShuffleBlockFetcherIterator: Getting 4 (1595.0 B) non-empty blocks including 4 (1595.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:18:56 INFO ShuffleBlockFetcherIterator: Getting 4 (1336.0 B) non-empty blocks including 4 (1336.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:18:56 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:18:56 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:18:56 INFO Executor: Finished task 1.0 in stage 8.0 (TID 36). 3247 bytes result sent to driver
25/02/23 20:18:56 INFO Executor: Finished task 0.0 in stage 8.0 (TID 35). 2982 bytes result sent to driver
25/02/23 20:18:56 INFO TaskSetManager: Finished task 1.0 in stage 8.0 (TID 36) in 11 ms on 192.168.1.48 (executor driver) (1/2)
25/02/23 20:18:56 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 35) in 12 ms on 192.168.1.48 (executor driver) (2/2)
25/02/23 20:18:56 INFO TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
25/02/23 20:18:56 INFO DAGScheduler: ResultStage 8 (treeAggregate at WeightedLeastSquares.scala:107) finished in 0.016 s
25/02/23 20:18:56 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:18:56 INFO TaskSchedulerImpl: Killing all running tasks in stage 8: Stage finished
25/02/23 20:18:56 INFO DAGScheduler: Job 6 finished: treeAggregate at WeightedLeastSquares.scala:107, took 4.744235 s
25/02/23 20:18:56 INFO Instrumentation: [b1871d78] Number of instances: 2452866.
25/02/23 20:18:56 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.lapack.JNILAPACK
25/02/23 20:18:56 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:18:56 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:18:56 INFO CodeGenerator: Code generated in 8.281542 ms
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:18:56 INFO SparkContext: Created broadcast 15 from rdd at LinearRegression.scala:921
25/02/23 20:18:56 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:18:56 INFO SparkContext: Starting job: treeAggregate at Statistics.scala:58
25/02/23 20:18:56 INFO DAGScheduler: Registering RDD 78 (treeAggregate at Statistics.scala:58) as input to shuffle 2
25/02/23 20:18:56 INFO DAGScheduler: Got job 7 (treeAggregate at Statistics.scala:58) with 2 output partitions
25/02/23 20:18:56 INFO DAGScheduler: Final stage: ResultStage 10 (treeAggregate at Statistics.scala:58)
25/02/23 20:18:56 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 9)
25/02/23 20:18:56 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 9)
25/02/23 20:18:56 INFO DAGScheduler: Submitting ShuffleMapStage 9 (MapPartitionsRDD[78] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 74.8 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 30.4 KiB, free 2.2 GiB)
25/02/23 20:18:56 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on 192.168.1.48:49730 (size: 30.4 KiB, free: 2.2 GiB)
25/02/23 20:18:56 INFO SparkContext: Created broadcast 16 from broadcast at DAGScheduler.scala:1585
25/02/23 20:18:56 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 9 (MapPartitionsRDD[78] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:18:56 INFO TaskSchedulerImpl: Adding task set 9.0 with 8 tasks resource profile 0
25/02/23 20:18:56 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 37) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 1.0 in stage 9.0 (TID 38) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 2.0 in stage 9.0 (TID 39) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 3.0 in stage 9.0 (TID 40) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 4.0 in stage 9.0 (TID 41) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 5.0 in stage 9.0 (TID 42) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 6.0 in stage 9.0 (TID 43) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO TaskSetManager: Starting task 7.0 in stage 9.0 (TID 44) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:18:56 INFO Executor: Running task 0.0 in stage 9.0 (TID 37)
25/02/23 20:18:56 INFO Executor: Running task 5.0 in stage 9.0 (TID 42)
25/02/23 20:18:56 INFO Executor: Running task 2.0 in stage 9.0 (TID 39)
25/02/23 20:18:56 INFO Executor: Running task 4.0 in stage 9.0 (TID 41)
25/02/23 20:18:56 INFO Executor: Running task 1.0 in stage 9.0 (TID 38)
25/02/23 20:18:56 INFO Executor: Running task 7.0 in stage 9.0 (TID 44)
25/02/23 20:18:56 INFO Executor: Running task 6.0 in stage 9.0 (TID 43)
25/02/23 20:18:56 INFO Executor: Running task 3.0 in stage 9.0 (TID 40)
25/02/23 20:18:56 INFO CodeGenerator: Code generated in 8.680167 ms
25/02/23 20:18:56 INFO CodeGenerator: Code generated in 2.99275 ms
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:18:56 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:18:56 INFO Executor: Finished task 7.0 in stage 9.0 (TID 44). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO Executor: Finished task 6.0 in stage 9.0 (TID 43). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO Executor: Finished task 1.0 in stage 9.0 (TID 38). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO TaskSetManager: Finished task 6.0 in stage 9.0 (TID 43) in 37 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:18:56 INFO TaskSetManager: Finished task 7.0 in stage 9.0 (TID 44) in 37 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:18:56 INFO TaskSetManager: Finished task 1.0 in stage 9.0 (TID 38) in 37 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:18:56 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:18:56 INFO Executor: Finished task 5.0 in stage 9.0 (TID 42). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO TaskSetManager: Finished task 5.0 in stage 9.0 (TID 42) in 39 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:18:56 INFO Executor: Finished task 2.0 in stage 9.0 (TID 39). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO Executor: Finished task 4.0 in stage 9.0 (TID 41). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO Executor: Finished task 0.0 in stage 9.0 (TID 37). 2262 bytes result sent to driver
25/02/23 20:18:56 INFO TaskSetManager: Finished task 4.0 in stage 9.0 (TID 41) in 40 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:18:56 INFO TaskSetManager: Finished task 2.0 in stage 9.0 (TID 39) in 40 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:18:56 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 37) in 41 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:18:56 INFO BlockManagerInfo: Removed broadcast_14_piece0 on 192.168.1.48:49730 in memory (size: 34.0 KiB, free: 2.2 GiB)
25/02/23 20:18:56 INFO BlockManagerInfo: Removed broadcast_13_piece0 on 192.168.1.48:49730 in memory (size: 33.2 KiB, free: 2.2 GiB)
25/02/23 20:19:00 INFO Executor: Finished task 3.0 in stage 9.0 (TID 40). 2348 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 3.0 in stage 9.0 (TID 40) in 4489 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:00 INFO TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool 
25/02/23 20:19:00 INFO DAGScheduler: ShuffleMapStage 9 (treeAggregate at Statistics.scala:58) finished in 4.493 s
25/02/23 20:19:00 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:00 INFO DAGScheduler: running: Set()
25/02/23 20:19:00 INFO DAGScheduler: waiting: Set(ResultStage 10)
25/02/23 20:19:00 INFO DAGScheduler: failed: Set()
25/02/23 20:19:00 INFO DAGScheduler: Submitting ResultStage 10 (MapPartitionsRDD[80] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 75.9 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 31.0 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on 192.168.1.48:49730 (size: 31.0 KiB, free: 2.2 GiB)
25/02/23 20:19:00 INFO SparkContext: Created broadcast 17 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:00 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 10 (MapPartitionsRDD[80] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1))
25/02/23 20:19:00 INFO TaskSchedulerImpl: Adding task set 10.0 with 2 tasks resource profile 0
25/02/23 20:19:00 INFO TaskSetManager: Starting task 0.0 in stage 10.0 (TID 45) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 1.0 in stage 10.0 (TID 46) (192.168.1.48, executor driver, partition 1, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:00 INFO Executor: Running task 0.0 in stage 10.0 (TID 45)
25/02/23 20:19:00 INFO Executor: Running task 1.0 in stage 10.0 (TID 46)
25/02/23 20:19:00 INFO ShuffleBlockFetcherIterator: Getting 4 (2.8 KiB) non-empty blocks including 4 (2.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:00 INFO ShuffleBlockFetcherIterator: Getting 4 (2.9 KiB) non-empty blocks including 4 (2.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:00 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:00 INFO Executor: Finished task 0.0 in stage 10.0 (TID 45). 3834 bytes result sent to driver
25/02/23 20:19:00 INFO Executor: Finished task 1.0 in stage 10.0 (TID 46). 4015 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 0.0 in stage 10.0 (TID 45) in 5 ms on 192.168.1.48 (executor driver) (1/2)
25/02/23 20:19:00 INFO TaskSetManager: Finished task 1.0 in stage 10.0 (TID 46) in 5 ms on 192.168.1.48 (executor driver) (2/2)
25/02/23 20:19:00 INFO TaskSchedulerImpl: Removed TaskSet 10.0, whose tasks have all completed, from pool 
25/02/23 20:19:00 INFO DAGScheduler: ResultStage 10 (treeAggregate at Statistics.scala:58) finished in 0.008 s
25/02/23 20:19:00 INFO DAGScheduler: Job 7 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:00 INFO TaskSchedulerImpl: Killing all running tasks in stage 10: Stage finished
25/02/23 20:19:00 INFO DAGScheduler: Job 7 finished: treeAggregate at Statistics.scala:58, took 4.503486 s
25/02/23 20:19:00 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:19:00 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:19:00 INFO CodeGenerator: Code generated in 7.005833 ms
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:00 INFO SparkContext: Created broadcast 18 from rdd at RegressionEvaluator.scala:125
25/02/23 20:19:00 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:00 INFO BlockManagerInfo: Removed broadcast_17_piece0 on 192.168.1.48:49730 in memory (size: 31.0 KiB, free: 2.2 GiB)
25/02/23 20:19:00 INFO SparkContext: Starting job: treeAggregate at Statistics.scala:58
25/02/23 20:19:00 INFO DAGScheduler: Registering RDD 90 (treeAggregate at Statistics.scala:58) as input to shuffle 3
25/02/23 20:19:00 INFO DAGScheduler: Got job 8 (treeAggregate at Statistics.scala:58) with 2 output partitions
25/02/23 20:19:00 INFO DAGScheduler: Final stage: ResultStage 12 (treeAggregate at Statistics.scala:58)
25/02/23 20:19:00 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 11)
25/02/23 20:19:00 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 11)
25/02/23 20:19:00 INFO DAGScheduler: Submitting ShuffleMapStage 11 (MapPartitionsRDD[90] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 75.4 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 30.7 KiB, free 2.2 GiB)
25/02/23 20:19:00 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on 192.168.1.48:49730 (size: 30.7 KiB, free: 2.2 GiB)
25/02/23 20:19:00 INFO SparkContext: Created broadcast 19 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:00 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 11 (MapPartitionsRDD[90] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:19:00 INFO TaskSchedulerImpl: Adding task set 11.0 with 8 tasks resource profile 0
25/02/23 20:19:00 INFO TaskSetManager: Starting task 0.0 in stage 11.0 (TID 47) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 1.0 in stage 11.0 (TID 48) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 2.0 in stage 11.0 (TID 49) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 3.0 in stage 11.0 (TID 50) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 4.0 in stage 11.0 (TID 51) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 5.0 in stage 11.0 (TID 52) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 6.0 in stage 11.0 (TID 53) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO TaskSetManager: Starting task 7.0 in stage 11.0 (TID 54) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:00 INFO Executor: Running task 2.0 in stage 11.0 (TID 49)
25/02/23 20:19:00 INFO Executor: Running task 7.0 in stage 11.0 (TID 54)
25/02/23 20:19:00 INFO Executor: Running task 0.0 in stage 11.0 (TID 47)
25/02/23 20:19:00 INFO Executor: Running task 4.0 in stage 11.0 (TID 51)
25/02/23 20:19:00 INFO Executor: Running task 3.0 in stage 11.0 (TID 50)
25/02/23 20:19:00 INFO Executor: Running task 5.0 in stage 11.0 (TID 52)
25/02/23 20:19:00 INFO Executor: Running task 6.0 in stage 11.0 (TID 53)
25/02/23 20:19:00 INFO Executor: Running task 1.0 in stage 11.0 (TID 48)
25/02/23 20:19:00 INFO CodeGenerator: Code generated in 10.050167 ms
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:19:00 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:19:00 INFO Executor: Finished task 0.0 in stage 11.0 (TID 47). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO Executor: Finished task 4.0 in stage 11.0 (TID 51). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 4.0 in stage 11.0 (TID 51) in 26 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:19:00 INFO TaskSetManager: Finished task 0.0 in stage 11.0 (TID 47) in 27 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:19:00 INFO Executor: Finished task 2.0 in stage 11.0 (TID 49). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 2.0 in stage 11.0 (TID 49) in 27 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:19:00 INFO Executor: Finished task 1.0 in stage 11.0 (TID 48). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 1.0 in stage 11.0 (TID 48) in 28 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:19:00 INFO Executor: Finished task 5.0 in stage 11.0 (TID 52). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:19:00 INFO TaskSetManager: Finished task 5.0 in stage 11.0 (TID 52) in 28 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:19:00 INFO Executor: Finished task 6.0 in stage 11.0 (TID 53). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 6.0 in stage 11.0 (TID 53) in 28 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:19:00 INFO Executor: Finished task 7.0 in stage 11.0 (TID 54). 2262 bytes result sent to driver
25/02/23 20:19:00 INFO TaskSetManager: Finished task 7.0 in stage 11.0 (TID 54) in 29 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:19:01 INFO BlockManagerInfo: Removed broadcast_16_piece0 on 192.168.1.48:49730 in memory (size: 30.4 KiB, free: 2.2 GiB)
25/02/23 20:19:01 INFO BlockManagerInfo: Removed broadcast_11_piece0 on 192.168.1.48:49730 in memory (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:01 INFO BlockManagerInfo: Removed broadcast_12_piece0 on 192.168.1.48:49730 in memory (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:04 INFO Executor: Finished task 3.0 in stage 11.0 (TID 50). 2348 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 3.0 in stage 11.0 (TID 50) in 3827 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:04 INFO TaskSchedulerImpl: Removed TaskSet 11.0, whose tasks have all completed, from pool 
25/02/23 20:19:04 INFO DAGScheduler: ShuffleMapStage 11 (treeAggregate at Statistics.scala:58) finished in 3.829 s
25/02/23 20:19:04 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:04 INFO DAGScheduler: running: Set()
25/02/23 20:19:04 INFO DAGScheduler: waiting: Set(ResultStage 12)
25/02/23 20:19:04 INFO DAGScheduler: failed: Set()
25/02/23 20:19:04 INFO DAGScheduler: Submitting ResultStage 12 (MapPartitionsRDD[92] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_20 stored as values in memory (estimated size 76.5 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_20_piece0 stored as bytes in memory (estimated size 31.3 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO BlockManagerInfo: Added broadcast_20_piece0 in memory on 192.168.1.48:49730 (size: 31.3 KiB, free: 2.2 GiB)
25/02/23 20:19:04 INFO SparkContext: Created broadcast 20 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:04 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 12 (MapPartitionsRDD[92] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1))
25/02/23 20:19:04 INFO TaskSchedulerImpl: Adding task set 12.0 with 2 tasks resource profile 0
25/02/23 20:19:04 INFO TaskSetManager: Starting task 0.0 in stage 12.0 (TID 55) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 1.0 in stage 12.0 (TID 56) (192.168.1.48, executor driver, partition 1, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:04 INFO Executor: Running task 0.0 in stage 12.0 (TID 55)
25/02/23 20:19:04 INFO Executor: Running task 1.0 in stage 12.0 (TID 56)
25/02/23 20:19:04 INFO ShuffleBlockFetcherIterator: Getting 4 (2.8 KiB) non-empty blocks including 4 (2.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:04 INFO ShuffleBlockFetcherIterator: Getting 4 (2.9 KiB) non-empty blocks including 4 (2.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:04 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:04 INFO Executor: Finished task 1.0 in stage 12.0 (TID 56). 4015 bytes result sent to driver
25/02/23 20:19:04 INFO Executor: Finished task 0.0 in stage 12.0 (TID 55). 3834 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 1.0 in stage 12.0 (TID 56) in 8 ms on 192.168.1.48 (executor driver) (1/2)
25/02/23 20:19:04 INFO TaskSetManager: Finished task 0.0 in stage 12.0 (TID 55) in 8 ms on 192.168.1.48 (executor driver) (2/2)
25/02/23 20:19:04 INFO TaskSchedulerImpl: Removed TaskSet 12.0, whose tasks have all completed, from pool 
25/02/23 20:19:04 INFO DAGScheduler: ResultStage 12 (treeAggregate at Statistics.scala:58) finished in 0.010 s
25/02/23 20:19:04 INFO DAGScheduler: Job 8 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:04 INFO TaskSchedulerImpl: Killing all running tasks in stage 12: Stage finished
25/02/23 20:19:04 INFO DAGScheduler: Job 8 finished: treeAggregate at Statistics.scala:58, took 3.841334 s
25/02/23 20:19:04 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:19:04 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_21 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_21_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO BlockManagerInfo: Added broadcast_21_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:04 INFO SparkContext: Created broadcast 21 from rdd at RegressionEvaluator.scala:125
25/02/23 20:19:04 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:04 INFO SparkContext: Starting job: treeAggregate at Statistics.scala:58
25/02/23 20:19:04 INFO DAGScheduler: Registering RDD 102 (treeAggregate at Statistics.scala:58) as input to shuffle 4
25/02/23 20:19:04 INFO DAGScheduler: Got job 9 (treeAggregate at Statistics.scala:58) with 2 output partitions
25/02/23 20:19:04 INFO DAGScheduler: Final stage: ResultStage 14 (treeAggregate at Statistics.scala:58)
25/02/23 20:19:04 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 13)
25/02/23 20:19:04 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 13)
25/02/23 20:19:04 INFO DAGScheduler: Submitting ShuffleMapStage 13 (MapPartitionsRDD[102] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_22 stored as values in memory (estimated size 75.4 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO MemoryStore: Block broadcast_22_piece0 stored as bytes in memory (estimated size 30.7 KiB, free 2.2 GiB)
25/02/23 20:19:04 INFO BlockManagerInfo: Added broadcast_22_piece0 in memory on 192.168.1.48:49730 (size: 30.7 KiB, free: 2.2 GiB)
25/02/23 20:19:04 INFO SparkContext: Created broadcast 22 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:04 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 13 (MapPartitionsRDD[102] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:19:04 INFO TaskSchedulerImpl: Adding task set 13.0 with 8 tasks resource profile 0
25/02/23 20:19:04 INFO TaskSetManager: Starting task 0.0 in stage 13.0 (TID 57) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 1.0 in stage 13.0 (TID 58) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 2.0 in stage 13.0 (TID 59) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 3.0 in stage 13.0 (TID 60) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 4.0 in stage 13.0 (TID 61) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 5.0 in stage 13.0 (TID 62) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 6.0 in stage 13.0 (TID 63) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO TaskSetManager: Starting task 7.0 in stage 13.0 (TID 64) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:04 INFO Executor: Running task 2.0 in stage 13.0 (TID 59)
25/02/23 20:19:04 INFO Executor: Running task 0.0 in stage 13.0 (TID 57)
25/02/23 20:19:04 INFO Executor: Running task 1.0 in stage 13.0 (TID 58)
25/02/23 20:19:04 INFO Executor: Running task 3.0 in stage 13.0 (TID 60)
25/02/23 20:19:04 INFO Executor: Running task 5.0 in stage 13.0 (TID 62)
25/02/23 20:19:04 INFO Executor: Running task 6.0 in stage 13.0 (TID 63)
25/02/23 20:19:04 INFO Executor: Running task 7.0 in stage 13.0 (TID 64)
25/02/23 20:19:04 INFO Executor: Running task 4.0 in stage 13.0 (TID 61)
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:19:04 INFO Executor: Finished task 1.0 in stage 13.0 (TID 58). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:19:04 INFO Executor: Finished task 7.0 in stage 13.0 (TID 64). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO Executor: Finished task 4.0 in stage 13.0 (TID 61). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 1.0 in stage 13.0 (TID 58) in 13 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:19:04 INFO TaskSetManager: Finished task 7.0 in stage 13.0 (TID 64) in 13 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:19:04 INFO TaskSetManager: Finished task 4.0 in stage 13.0 (TID 61) in 13 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:19:04 INFO Executor: Finished task 2.0 in stage 13.0 (TID 59). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO Executor: Finished task 6.0 in stage 13.0 (TID 63). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 6.0 in stage 13.0 (TID 63) in 14 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:19:04 INFO TaskSetManager: Finished task 2.0 in stage 13.0 (TID 59) in 15 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:19:04 INFO Executor: Finished task 5.0 in stage 13.0 (TID 62). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 5.0 in stage 13.0 (TID 62) in 15 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:19:04 INFO Executor: Finished task 0.0 in stage 13.0 (TID 57). 2262 bytes result sent to driver
25/02/23 20:19:04 INFO TaskSetManager: Finished task 0.0 in stage 13.0 (TID 57) in 16 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:19:04 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:19:04 INFO BlockManagerInfo: Removed broadcast_20_piece0 on 192.168.1.48:49730 in memory (size: 31.3 KiB, free: 2.2 GiB)
25/02/23 20:19:05 INFO BlockManagerInfo: Removed broadcast_19_piece0 on 192.168.1.48:49730 in memory (size: 30.7 KiB, free: 2.2 GiB)
25/02/23 20:19:05 INFO BlockManagerInfo: Removed broadcast_18_piece0 on 192.168.1.48:49730 in memory (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO Executor: Finished task 3.0 in stage 13.0 (TID 60). 2391 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 3.0 in stage 13.0 (TID 60) in 3803 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:08 INFO TaskSchedulerImpl: Removed TaskSet 13.0, whose tasks have all completed, from pool 
25/02/23 20:19:08 INFO DAGScheduler: ShuffleMapStage 13 (treeAggregate at Statistics.scala:58) finished in 3.806 s
25/02/23 20:19:08 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:08 INFO DAGScheduler: running: Set()
25/02/23 20:19:08 INFO DAGScheduler: waiting: Set(ResultStage 14)
25/02/23 20:19:08 INFO DAGScheduler: failed: Set()
25/02/23 20:19:08 INFO DAGScheduler: Submitting ResultStage 14 (MapPartitionsRDD[104] at treeAggregate at Statistics.scala:58), which has no missing parents
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 76.5 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 31.3 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on 192.168.1.48:49730 (size: 31.3 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 23 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:08 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 14 (MapPartitionsRDD[104] at treeAggregate at Statistics.scala:58) (first 15 tasks are for partitions Vector(0, 1))
25/02/23 20:19:08 INFO TaskSchedulerImpl: Adding task set 14.0 with 2 tasks resource profile 0
25/02/23 20:19:08 INFO TaskSetManager: Starting task 0.0 in stage 14.0 (TID 65) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 1.0 in stage 14.0 (TID 66) (192.168.1.48, executor driver, partition 1, NODE_LOCAL, 8817 bytes) 
25/02/23 20:19:08 INFO Executor: Running task 0.0 in stage 14.0 (TID 65)
25/02/23 20:19:08 INFO Executor: Running task 1.0 in stage 14.0 (TID 66)
25/02/23 20:19:08 INFO ShuffleBlockFetcherIterator: Getting 4 (2.9 KiB) non-empty blocks including 4 (2.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:08 INFO ShuffleBlockFetcherIterator: Getting 4 (2.8 KiB) non-empty blocks including 4 (2.8 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:08 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:08 INFO Executor: Finished task 0.0 in stage 14.0 (TID 65). 3834 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 0.0 in stage 14.0 (TID 65) in 6 ms on 192.168.1.48 (executor driver) (1/2)
25/02/23 20:19:08 INFO Executor: Finished task 1.0 in stage 14.0 (TID 66). 4015 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 1.0 in stage 14.0 (TID 66) in 6 ms on 192.168.1.48 (executor driver) (2/2)
25/02/23 20:19:08 INFO TaskSchedulerImpl: Removed TaskSet 14.0, whose tasks have all completed, from pool 
25/02/23 20:19:08 INFO DAGScheduler: ResultStage 14 (treeAggregate at Statistics.scala:58) finished in 0.010 s
25/02/23 20:19:08 INFO DAGScheduler: Job 9 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:08 INFO TaskSchedulerImpl: Killing all running tasks in stage 14: Stage finished
25/02/23 20:19:08 INFO DAGScheduler: Job 9 finished: treeAggregate at Statistics.scala:58, took 3.818126 s
Root Mean Square Error (RMSE): 17.71042474478113
R-squared (R2): 0.014235526777914509
25/02/23 20:19:08 INFO FileSourceStrategy: Pushed Filters: IsNotNull(PULocationID)
25/02/23 20:19:08 INFO FileSourceStrategy: Post-Scan Filters: isnotnull(PULocationID#7L)
25/02/23 20:19:08 INFO FileSourceStrategy: Pushed Filters: IsNotNull(LocationID)
25/02/23 20:19:08 INFO FileSourceStrategy: Post-Scan Filters: isnotnull(LocationID#79)
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 2.328041 ms
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_24 stored as values in memory (estimated size 199.5 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Removed broadcast_23_piece0 on 192.168.1.48:49730 in memory (size: 31.3 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_24_piece0 stored as bytes in memory (estimated size 34.3 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on 192.168.1.48:49730 (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 24 from $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264
25/02/23 20:19:08 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4194304 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:08 INFO SparkContext: Starting job: $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264
25/02/23 20:19:08 INFO DAGScheduler: Got job 10 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:264) with 1 output partitions
25/02/23 20:19:08 INFO DAGScheduler: Final stage: ResultStage 15 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:264)
25/02/23 20:19:08 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:19:08 INFO BlockManagerInfo: Removed broadcast_21_piece0 on 192.168.1.48:49730 in memory (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:08 INFO DAGScheduler: Submitting ResultStage 15 (MapPartitionsRDD[108] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264), which has no missing parents
25/02/23 20:19:08 INFO BlockManagerInfo: Removed broadcast_22_piece0 on 192.168.1.48:49730 in memory (size: 30.7 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_25 stored as values in memory (estimated size 15.5 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_25_piece0 stored as bytes in memory (estimated size 7.6 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_25_piece0 in memory on 192.168.1.48:49730 (size: 7.6 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 25 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:08 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 15 (MapPartitionsRDD[108] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264) (first 15 tasks are for partitions Vector(0))
25/02/23 20:19:08 INFO TaskSchedulerImpl: Adding task set 15.0 with 1 tasks resource profile 0
25/02/23 20:19:08 INFO TaskSetManager: Starting task 0.0 in stage 15.0 (TID 67) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO Executor: Running task 0.0 in stage 15.0 (TID 67)
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 2.864959 ms
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/taxi_zone_lookup.csv, range: 0-12331, partition values: [empty row]
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 1.461958 ms
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 0.969792 ms
25/02/23 20:19:08 INFO Executor: Finished task 0.0 in stage 15.0 (TID 67). 4390 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 0.0 in stage 15.0 (TID 67) in 22 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:19:08 INFO TaskSchedulerImpl: Removed TaskSet 15.0, whose tasks have all completed, from pool 
25/02/23 20:19:08 INFO DAGScheduler: ResultStage 15 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:264) finished in 0.025 s
25/02/23 20:19:08 INFO DAGScheduler: Job 10 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:08 INFO TaskSchedulerImpl: Killing all running tasks in stage 15: Stage finished
25/02/23 20:19:08 INFO DAGScheduler: Job 10 finished: $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264, took 0.026213 s
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 2.430875 ms
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_26 stored as values in memory (estimated size 1026.1 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_26_piece0 stored as bytes in memory (estimated size 4.0 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_26_piece0 in memory on 192.168.1.48:49730 (size: 4.0 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 26 from $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264
25/02/23 20:19:08 INFO FileSourceStrategy: Pushed Filters: IsNotNull(PULocationID)
25/02/23 20:19:08 INFO FileSourceStrategy: Post-Scan Filters: isnotnull(PULocationID#7L)
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 24.093334 ms
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_27 stored as values in memory (estimated size 201.2 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_27_piece0 stored as bytes in memory (estimated size 35.0 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_27_piece0 in memory on 192.168.1.48:49730 (size: 35.0 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 27 from csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:08 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:08 INFO DAGScheduler: Registering RDD 112 (csv at NativeMethodAccessorImpl.java:0) as input to shuffle 5
25/02/23 20:19:08 INFO DAGScheduler: Got map stage job 11 (csv at NativeMethodAccessorImpl.java:0) with 8 output partitions
25/02/23 20:19:08 INFO DAGScheduler: Final stage: ShuffleMapStage 16 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:08 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:19:08 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:08 INFO DAGScheduler: Submitting ShuffleMapStage 16 (MapPartitionsRDD[112] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_28 stored as values in memory (estimated size 54.4 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO MemoryStore: Block broadcast_28_piece0 stored as bytes in memory (estimated size 24.4 KiB, free 2.2 GiB)
25/02/23 20:19:08 INFO BlockManagerInfo: Added broadcast_28_piece0 in memory on 192.168.1.48:49730 (size: 24.4 KiB, free: 2.2 GiB)
25/02/23 20:19:08 INFO SparkContext: Created broadcast 28 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:08 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 16 (MapPartitionsRDD[112] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:19:08 INFO TaskSchedulerImpl: Adding task set 16.0 with 8 tasks resource profile 0
25/02/23 20:19:08 INFO TaskSetManager: Starting task 0.0 in stage 16.0 (TID 68) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 1.0 in stage 16.0 (TID 69) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 2.0 in stage 16.0 (TID 70) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 3.0 in stage 16.0 (TID 71) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 4.0 in stage 16.0 (TID 72) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 5.0 in stage 16.0 (TID 73) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 6.0 in stage 16.0 (TID 74) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO TaskSetManager: Starting task 7.0 in stage 16.0 (TID 75) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:08 INFO Executor: Running task 2.0 in stage 16.0 (TID 70)
25/02/23 20:19:08 INFO Executor: Running task 5.0 in stage 16.0 (TID 73)
25/02/23 20:19:08 INFO Executor: Running task 7.0 in stage 16.0 (TID 75)
25/02/23 20:19:08 INFO Executor: Running task 4.0 in stage 16.0 (TID 72)
25/02/23 20:19:08 INFO Executor: Running task 3.0 in stage 16.0 (TID 71)
25/02/23 20:19:08 INFO Executor: Running task 1.0 in stage 16.0 (TID 69)
25/02/23 20:19:08 INFO Executor: Running task 0.0 in stage 16.0 (TID 68)
25/02/23 20:19:08 INFO Executor: Running task 6.0 in stage 16.0 (TID 74)
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 7.490792 ms
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 1.7655 ms
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 0.977542 ms
25/02/23 20:19:08 INFO CodeGenerator: Code generated in 1.186 ms
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:19:08 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO FilterCompat: Filtering using predicate: noteq(PULocationID, null)
25/02/23 20:19:08 INFO Executor: Finished task 2.0 in stage 16.0 (TID 70). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO Executor: Finished task 7.0 in stage 16.0 (TID 75). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 7.0 in stage 16.0 (TID 75) in 38 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:19:08 INFO TaskSetManager: Finished task 2.0 in stage 16.0 (TID 70) in 38 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:19:08 INFO Executor: Finished task 4.0 in stage 16.0 (TID 72). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 4.0 in stage 16.0 (TID 72) in 39 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:19:08 INFO Executor: Finished task 6.0 in stage 16.0 (TID 74). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO Executor: Finished task 1.0 in stage 16.0 (TID 69). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO Executor: Finished task 5.0 in stage 16.0 (TID 73). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 1.0 in stage 16.0 (TID 69) in 40 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:19:08 INFO TaskSetManager: Finished task 6.0 in stage 16.0 (TID 74) in 40 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:19:08 INFO TaskSetManager: Finished task 5.0 in stage 16.0 (TID 73) in 40 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:19:08 INFO ColumnIndexFilter: No offset index for column PULocationID is available; Unable to do filtering
25/02/23 20:19:08 INFO Executor: Finished task 0.0 in stage 16.0 (TID 68). 3543 bytes result sent to driver
25/02/23 20:19:08 INFO TaskSetManager: Finished task 0.0 in stage 16.0 (TID 68) in 40 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:19:08 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:19:09 INFO Executor: Finished task 3.0 in stage 16.0 (TID 71). 3715 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 3.0 in stage 16.0 (TID 71) in 204 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 16.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ShuffleMapStage 16 (csv at NativeMethodAccessorImpl.java:0) finished in 0.206 s
25/02/23 20:19:09 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:09 INFO DAGScheduler: running: Set()
25/02/23 20:19:09 INFO DAGScheduler: waiting: Set()
25/02/23 20:19:09 INFO DAGScheduler: failed: Set()
25/02/23 20:19:09 INFO ShufflePartitionsUtil: For shuffle(5), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
25/02/23 20:19:09 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 4.058041 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.773958 ms
25/02/23 20:19:09 INFO SparkContext: Starting job: csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO DAGScheduler: Got job 12 (csv at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ResultStage 18 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 17)
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ResultStage 18 (MapPartitionsRDD[117] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_29 stored as values in memory (estimated size 54.6 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_29_piece0 stored as bytes in memory (estimated size 25.0 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_29_piece0 in memory on 192.168.1.48:49730 (size: 25.0 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 29 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 18 (MapPartitionsRDD[117] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 18.0 with 1 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 18.0 (TID 76) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8999 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 18.0 (TID 76)
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Getting 1 (722.0 B) non-empty blocks including 1 (722.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 21.066667 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.150833 ms
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 18.0 (TID 76). 6482 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 18.0 (TID 76) in 29 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 18.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ResultStage 18 (csv at NativeMethodAccessorImpl.java:0) finished in 0.041 s
25/02/23 20:19:09 INFO DAGScheduler: Job 12 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:09 INFO TaskSchedulerImpl: Killing all running tasks in stage 18: Stage finished
25/02/23 20:19:09 INFO DAGScheduler: Job 12 finished: csv at NativeMethodAccessorImpl.java:0, took 0.043277 s
25/02/23 20:19:09 INFO DAGScheduler: Registering RDD 118 (csv at NativeMethodAccessorImpl.java:0) as input to shuffle 6
25/02/23 20:19:09 INFO DAGScheduler: Got map stage job 13 (csv at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ShuffleMapStage 20 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 19)
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ShuffleMapStage 20 (MapPartitionsRDD[118] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_30 stored as values in memory (estimated size 54.9 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_30_piece0 stored as bytes in memory (estimated size 25.1 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_30_piece0 in memory on 192.168.1.48:49730 (size: 25.1 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 30 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 1 missing tasks from ShuffleMapStage 20 (MapPartitionsRDD[118] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 20.0 with 1 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 20.0 (TID 77) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8988 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 20.0 (TID 77)
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 2.112541 ms
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Getting 1 (722.0 B) non-empty blocks including 1 (722.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 20.0 (TID 77). 6259 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 20.0 (TID 77) in 11 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 20.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ShuffleMapStage 20 (csv at NativeMethodAccessorImpl.java:0) finished in 0.015 s
25/02/23 20:19:09 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:09 INFO DAGScheduler: running: Set()
25/02/23 20:19:09 INFO DAGScheduler: waiting: Set()
25/02/23 20:19:09 INFO DAGScheduler: failed: Set()
25/02/23 20:19:09 INFO ShufflePartitionsUtil: For shuffle(6), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 2.074209 ms
25/02/23 20:19:09 INFO SparkContext: Starting job: csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO DAGScheduler: Got job 14 (csv at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ResultStage 23 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 22)
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ResultStage 23 (MapPartitionsRDD[121] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_31 stored as values in memory (estimated size 250.7 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_31_piece0 stored as bytes in memory (estimated size 93.9 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_31_piece0 in memory on 192.168.1.48:49730 (size: 93.9 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 31 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 23 (MapPartitionsRDD[121] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 23.0 with 1 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 23.0 (TID 78) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8999 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 23.0 (TID 78)
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Getting 1 (656.0 B) non-empty blocks including 1 (656.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 2.087875 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.547125 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.562417 ms
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileOutputCommitter: Saved output of task 'attempt_202502232019095989883975440365954_0023_m_000000_78' to file:/Users/harmansinghmalhotra/spark_projects/results/avg_fare_by_borough/_temporary/0/task_202502232019095989883975440365954_0023_m_000000
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: attempt_202502232019095989883975440365954_0023_m_000000_78: Committed. Elapsed time: 0 ms.
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 23.0 (TID 78). 8556 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 23.0 (TID 78) in 35 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 23.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ResultStage 23 (csv at NativeMethodAccessorImpl.java:0) finished in 0.047 s
25/02/23 20:19:09 INFO DAGScheduler: Job 14 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:09 INFO TaskSchedulerImpl: Killing all running tasks in stage 23: Stage finished
25/02/23 20:19:09 INFO DAGScheduler: Job 14 finished: csv at NativeMethodAccessorImpl.java:0, took 0.050491 s
25/02/23 20:19:09 INFO FileFormatWriter: Start to commit write Job 5ff6d26d-2cd6-4b20-af7a-1e7266020f79.
25/02/23 20:19:09 INFO FileFormatWriter: Write Job 5ff6d26d-2cd6-4b20-af7a-1e7266020f79 committed. Elapsed time: 6 ms.
25/02/23 20:19:09 INFO FileFormatWriter: Finished processing stats for write job 5ff6d26d-2cd6-4b20-af7a-1e7266020f79.
25/02/23 20:19:09 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:19:09 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 6.804792 ms
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_32 stored as values in memory (estimated size 201.3 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_32_piece0 stored as bytes in memory (estimated size 34.9 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_32_piece0 in memory on 192.168.1.48:49730 (size: 34.9 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 32 from csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:09 INFO DAGScheduler: Registering RDD 125 (csv at NativeMethodAccessorImpl.java:0) as input to shuffle 7
25/02/23 20:19:09 INFO DAGScheduler: Got map stage job 15 (csv at NativeMethodAccessorImpl.java:0) with 8 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ShuffleMapStage 24 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ShuffleMapStage 24 (MapPartitionsRDD[125] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_33 stored as values in memory (estimated size 50.2 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_33_piece0 stored as bytes in memory (estimated size 21.0 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_33_piece0 in memory on 192.168.1.48:49730 (size: 21.0 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 33 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 8 missing tasks from ShuffleMapStage 24 (MapPartitionsRDD[125] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 24.0 with 8 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 24.0 (TID 79) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 1.0 in stage 24.0 (TID 80) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 2.0 in stage 24.0 (TID 81) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 3.0 in stage 24.0 (TID 82) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 4.0 in stage 24.0 (TID 83) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 5.0 in stage 24.0 (TID 84) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 6.0 in stage 24.0 (TID 85) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 7.0 in stage 24.0 (TID 86) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9631 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 24.0 (TID 79)
25/02/23 20:19:09 INFO Executor: Running task 1.0 in stage 24.0 (TID 80)
25/02/23 20:19:09 INFO Executor: Running task 5.0 in stage 24.0 (TID 84)
25/02/23 20:19:09 INFO Executor: Running task 4.0 in stage 24.0 (TID 83)
25/02/23 20:19:09 INFO Executor: Running task 7.0 in stage 24.0 (TID 86)
25/02/23 20:19:09 INFO Executor: Running task 3.0 in stage 24.0 (TID 82)
25/02/23 20:19:09 INFO Executor: Running task 6.0 in stage 24.0 (TID 85)
25/02/23 20:19:09 INFO Executor: Running task 2.0 in stage 24.0 (TID 81)
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 7.686959 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 2.891875 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.529083 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.642334 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 1.07475 ms
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:19:09 INFO Executor: Finished task 7.0 in stage 24.0 (TID 86). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO Executor: Finished task 5.0 in stage 24.0 (TID 84). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 7.0 in stage 24.0 (TID 86) in 38 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:19:09 INFO Executor: Finished task 6.0 in stage 24.0 (TID 85). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 5.0 in stage 24.0 (TID 84) in 38 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:19:09 INFO TaskSetManager: Finished task 6.0 in stage 24.0 (TID 85) in 38 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:19:09 INFO Executor: Finished task 4.0 in stage 24.0 (TID 83). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 4.0 in stage 24.0 (TID 83) in 40 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:19:09 INFO Executor: Finished task 2.0 in stage 24.0 (TID 81). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 2.0 in stage 24.0 (TID 81) in 40 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 24.0 (TID 79). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 24.0 (TID 79) in 41 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:19:09 INFO Executor: Finished task 1.0 in stage 24.0 (TID 80). 2758 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 1.0 in stage 24.0 (TID 80) in 41 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:19:09 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:19:09 INFO Executor: Finished task 3.0 in stage 24.0 (TID 82). 2930 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 3.0 in stage 24.0 (TID 82) in 211 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 24.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ShuffleMapStage 24 (csv at NativeMethodAccessorImpl.java:0) finished in 0.212 s
25/02/23 20:19:09 INFO DAGScheduler: looking for newly runnable stages
25/02/23 20:19:09 INFO DAGScheduler: running: Set()
25/02/23 20:19:09 INFO DAGScheduler: waiting: Set()
25/02/23 20:19:09 INFO DAGScheduler: failed: Set()
25/02/23 20:19:09 INFO ShufflePartitionsUtil: For shuffle(7), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 6.116291 ms
25/02/23 20:19:09 INFO SparkContext: Starting job: csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO DAGScheduler: Got job 16 (csv at NativeMethodAccessorImpl.java:0) with 1 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ResultStage 26 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 25)
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ResultStage 26 (MapPartitionsRDD[128] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_34 stored as values in memory (estimated size 254.6 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_34_piece0 stored as bytes in memory (estimated size 93.7 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_34_piece0 in memory on 192.168.1.48:49730 (size: 93.7 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 34 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 26 (MapPartitionsRDD[128] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 26.0 with 1 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 26.0 (TID 87) (192.168.1.48, executor driver, partition 0, NODE_LOCAL, 8999 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 26.0 (TID 87)
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Getting 1 (16.9 KiB) non-empty blocks including 1 (16.9 KiB) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
25/02/23 20:19:09 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 4.559125 ms
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileOutputCommitter: Saved output of task 'attempt_202502232019098011653819462089637_0026_m_000000_87' to file:/Users/harmansinghmalhotra/spark_projects/results/summary_stats/_temporary/0/task_202502232019098011653819462089637_0026_m_000000
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: attempt_202502232019098011653819462089637_0026_m_000000_87: Committed. Elapsed time: 0 ms.
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 26.0 (TID 87). 6302 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 26.0 (TID 87) in 28 ms on 192.168.1.48 (executor driver) (1/1)
25/02/23 20:19:09 INFO TaskSchedulerImpl: Removed TaskSet 26.0, whose tasks have all completed, from pool 
25/02/23 20:19:09 INFO DAGScheduler: ResultStage 26 (csv at NativeMethodAccessorImpl.java:0) finished in 0.051 s
25/02/23 20:19:09 INFO DAGScheduler: Job 16 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:09 INFO TaskSchedulerImpl: Killing all running tasks in stage 26: Stage finished
25/02/23 20:19:09 INFO DAGScheduler: Job 16 finished: csv at NativeMethodAccessorImpl.java:0, took 0.052458 s
25/02/23 20:19:09 INFO FileFormatWriter: Start to commit write Job 6d3b9f4b-5439-450f-85b9-e95c45852543.
25/02/23 20:19:09 INFO FileFormatWriter: Write Job 6d3b9f4b-5439-450f-85b9-e95c45852543 committed. Elapsed time: 6 ms.
25/02/23 20:19:09 INFO FileFormatWriter: Finished processing stats for write job 6d3b9f4b-5439-450f-85b9-e95c45852543.
25/02/23 20:19:09 INFO FileSourceStrategy: Pushed Filters: 
25/02/23 20:19:09 INFO FileSourceStrategy: Post-Scan Filters: 
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 6.367833 ms
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_35 stored as values in memory (estimated size 201.5 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_35_piece0 stored as bytes in memory (estimated size 35.1 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_35_piece0 in memory on 192.168.1.48:49730 (size: 35.1 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 35 from csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO FileSourceScanExec: Planning scan with bin packing, max size: 6483459 bytes, open cost is considered as scanning 4194304 bytes.
25/02/23 20:19:09 INFO SparkContext: Starting job: csv at NativeMethodAccessorImpl.java:0
25/02/23 20:19:09 INFO DAGScheduler: Got job 17 (csv at NativeMethodAccessorImpl.java:0) with 8 output partitions
25/02/23 20:19:09 INFO DAGScheduler: Final stage: ResultStage 27 (csv at NativeMethodAccessorImpl.java:0)
25/02/23 20:19:09 INFO DAGScheduler: Parents of final stage: List()
25/02/23 20:19:09 INFO DAGScheduler: Missing parents: List()
25/02/23 20:19:09 INFO DAGScheduler: Submitting ResultStage 27 (MapPartitionsRDD[132] at csv at NativeMethodAccessorImpl.java:0), which has no missing parents
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_36 stored as values in memory (estimated size 265.5 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO MemoryStore: Block broadcast_36_piece0 stored as bytes in memory (estimated size 97.8 KiB, free 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Added broadcast_36_piece0 in memory on 192.168.1.48:49730 (size: 97.8 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO SparkContext: Created broadcast 36 from broadcast at DAGScheduler.scala:1585
25/02/23 20:19:09 INFO DAGScheduler: Submitting 8 missing tasks from ResultStage 27 (MapPartitionsRDD[132] at csv at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7))
25/02/23 20:19:09 INFO TaskSchedulerImpl: Adding task set 27.0 with 8 tasks resource profile 0
25/02/23 20:19:09 INFO TaskSetManager: Starting task 0.0 in stage 27.0 (TID 88) (192.168.1.48, executor driver, partition 0, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 1.0 in stage 27.0 (TID 89) (192.168.1.48, executor driver, partition 1, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 2.0 in stage 27.0 (TID 90) (192.168.1.48, executor driver, partition 2, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 3.0 in stage 27.0 (TID 91) (192.168.1.48, executor driver, partition 3, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 4.0 in stage 27.0 (TID 92) (192.168.1.48, executor driver, partition 4, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 5.0 in stage 27.0 (TID 93) (192.168.1.48, executor driver, partition 5, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 6.0 in stage 27.0 (TID 94) (192.168.1.48, executor driver, partition 6, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO TaskSetManager: Starting task 7.0 in stage 27.0 (TID 95) (192.168.1.48, executor driver, partition 7, PROCESS_LOCAL, 9642 bytes) 
25/02/23 20:19:09 INFO Executor: Running task 2.0 in stage 27.0 (TID 90)
25/02/23 20:19:09 INFO Executor: Running task 5.0 in stage 27.0 (TID 93)
25/02/23 20:19:09 INFO Executor: Running task 3.0 in stage 27.0 (TID 91)
25/02/23 20:19:09 INFO Executor: Running task 4.0 in stage 27.0 (TID 92)
25/02/23 20:19:09 INFO Executor: Running task 7.0 in stage 27.0 (TID 95)
25/02/23 20:19:09 INFO Executor: Running task 1.0 in stage 27.0 (TID 89)
25/02/23 20:19:09 INFO Executor: Running task 0.0 in stage 27.0 (TID 88)
25/02/23 20:19:09 INFO Executor: Running task 6.0 in stage 27.0 (TID 94)
25/02/23 20:19:09 INFO CodeGenerator: Code generated in 6.852667 ms
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 19450377-25933836, partition values: [empty row]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 32417295-38900754, partition values: [empty row]
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 25933836-32417295, partition values: [empty row]
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 6483459-12966918, partition values: [empty row]
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 45384213-47673370, partition values: [empty row]
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 38900754-45384213, partition values: [empty row]
25/02/23 20:19:09 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
25/02/23 20:19:09 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
25/02/23 20:19:09 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 12966918-19450377, partition values: [empty row]
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000004_92
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000001_89
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000005_93
25/02/23 20:19:09 INFO Executor: Finished task 4.0 in stage 27.0 (TID 92). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO Executor: Finished task 5.0 in stage 27.0 (TID 93). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO Executor: Finished task 1.0 in stage 27.0 (TID 89). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000006_94
25/02/23 20:19:09 INFO TaskSetManager: Finished task 4.0 in stage 27.0 (TID 92) in 47 ms on 192.168.1.48 (executor driver) (1/8)
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000007_95
25/02/23 20:19:09 INFO TaskSetManager: Finished task 5.0 in stage 27.0 (TID 93) in 47 ms on 192.168.1.48 (executor driver) (2/8)
25/02/23 20:19:09 INFO Executor: Finished task 7.0 in stage 27.0 (TID 95). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 1.0 in stage 27.0 (TID 89) in 47 ms on 192.168.1.48 (executor driver) (3/8)
25/02/23 20:19:09 INFO Executor: Finished task 6.0 in stage 27.0 (TID 94). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 7.0 in stage 27.0 (TID 95) in 47 ms on 192.168.1.48 (executor driver) (4/8)
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: No need to commit output of task because needsTaskCommit=false: attempt_202502232019094028052441629888138_0027_m_000002_90
25/02/23 20:19:09 INFO TaskSetManager: Finished task 6.0 in stage 27.0 (TID 94) in 48 ms on 192.168.1.48 (executor driver) (5/8)
25/02/23 20:19:09 INFO Executor: Finished task 2.0 in stage 27.0 (TID 90). 3148 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 2.0 in stage 27.0 (TID 90) in 49 ms on 192.168.1.48 (executor driver) (6/8)
25/02/23 20:19:09 INFO CodecPool: Got brand-new decompressor [.gz]
25/02/23 20:19:09 INFO FileScanRDD: Reading File path: file:///Users/harmansinghmalhotra/spark_projects/yellow_tripdata_2023-01.parquet, range: 0-6483459, partition values: [empty row]
25/02/23 20:19:09 INFO FileOutputCommitter: Saved output of task 'attempt_202502232019094028052441629888138_0027_m_000000_88' to file:/Users/harmansinghmalhotra/spark_projects/results/predictions/_temporary/0/task_202502232019094028052441629888138_0027_m_000000
25/02/23 20:19:09 INFO SparkHadoopMapRedUtil: attempt_202502232019094028052441629888138_0027_m_000000_88: Committed. Elapsed time: 0 ms.
25/02/23 20:19:09 INFO Executor: Finished task 0.0 in stage 27.0 (TID 88). 3191 bytes result sent to driver
25/02/23 20:19:09 INFO TaskSetManager: Finished task 0.0 in stage 27.0 (TID 88) in 56 ms on 192.168.1.48 (executor driver) (7/8)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_29_piece0 on 192.168.1.48:49730 in memory (size: 25.0 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_34_piece0 on 192.168.1.48:49730 in memory (size: 93.7 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_27_piece0 on 192.168.1.48:49730 in memory (size: 35.0 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_33_piece0 on 192.168.1.48:49730 in memory (size: 21.0 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_30_piece0 on 192.168.1.48:49730 in memory (size: 25.1 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_32_piece0 on 192.168.1.48:49730 in memory (size: 34.9 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_28_piece0 on 192.168.1.48:49730 in memory (size: 24.4 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_24_piece0 on 192.168.1.48:49730 in memory (size: 34.3 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_25_piece0 on 192.168.1.48:49730 in memory (size: 7.6 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_31_piece0 on 192.168.1.48:49730 in memory (size: 93.9 KiB, free: 2.2 GiB)
25/02/23 20:19:09 INFO BlockManagerInfo: Removed broadcast_26_piece0 on 192.168.1.48:49730 in memory (size: 4.0 KiB, free: 2.2 GiB)
25/02/23 20:19:13 INFO FileOutputCommitter: Saved output of task 'attempt_202502232019094028052441629888138_0027_m_000003_91' to file:/Users/harmansinghmalhotra/spark_projects/results/predictions/_temporary/0/task_202502232019094028052441629888138_0027_m_000003
25/02/23 20:19:13 INFO SparkHadoopMapRedUtil: attempt_202502232019094028052441629888138_0027_m_000003_91: Committed. Elapsed time: 0 ms.
25/02/23 20:19:13 INFO Executor: Finished task 3.0 in stage 27.0 (TID 91). 3320 bytes result sent to driver
25/02/23 20:19:13 INFO TaskSetManager: Finished task 3.0 in stage 27.0 (TID 91) in 4299 ms on 192.168.1.48 (executor driver) (8/8)
25/02/23 20:19:13 INFO TaskSchedulerImpl: Removed TaskSet 27.0, whose tasks have all completed, from pool 
25/02/23 20:19:13 INFO DAGScheduler: ResultStage 27 (csv at NativeMethodAccessorImpl.java:0) finished in 4.317 s
25/02/23 20:19:13 INFO DAGScheduler: Job 17 is finished. Cancelling potential speculative or zombie tasks for this job
25/02/23 20:19:13 INFO TaskSchedulerImpl: Killing all running tasks in stage 27: Stage finished
25/02/23 20:19:13 INFO DAGScheduler: Job 17 finished: csv at NativeMethodAccessorImpl.java:0, took 4.318465 s
25/02/23 20:19:13 INFO FileFormatWriter: Start to commit write Job fdec4882-da07-45ac-a3c1-955d6bebf3f7.
25/02/23 20:19:13 INFO FileFormatWriter: Write Job fdec4882-da07-45ac-a3c1-955d6bebf3f7 committed. Elapsed time: 10 ms.
25/02/23 20:19:13 INFO FileFormatWriter: Finished processing stats for write job fdec4882-da07-45ac-a3c1-955d6bebf3f7.
25/02/23 20:19:13 INFO SparkContext: SparkContext is stopping with exitCode 0.
25/02/23 20:19:13 INFO SparkUI: Stopped Spark web UI at http://192.168.1.48:4040
25/02/23 20:19:13 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
25/02/23 20:19:13 INFO MemoryStore: MemoryStore cleared
25/02/23 20:19:13 INFO BlockManager: BlockManager stopped
25/02/23 20:19:13 INFO BlockManagerMaster: BlockManagerMaster stopped
25/02/23 20:19:13 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
25/02/23 20:19:13 INFO SparkContext: Successfully stopped SparkContext
25/02/23 20:19:14 INFO ShutdownHookManager: Shutdown hook called
25/02/23 20:19:14 INFO ShutdownHookManager: Deleting directory /private/var/folders/n7/gfjkgkmj2tj4rkl8f4gx_bvh0000gn/T/spark-87019d78-f182-4067-a33f-5e22329a821e
25/02/23 20:19:14 INFO ShutdownHookManager: Deleting directory /private/var/folders/n7/gfjkgkmj2tj4rkl8f4gx_bvh0000gn/T/spark-e1389cbb-e3e4-41f9-9767-36520051b134
25/02/23 20:19:14 INFO ShutdownHookManager: Deleting directory /private/var/folders/n7/gfjkgkmj2tj4rkl8f4gx_bvh0000gn/T/spark-87019d78-f182-4067-a33f-5e22329a821e/pyspark-f31e2e74-13db-44f4-98b8-28e53ab692c5
```