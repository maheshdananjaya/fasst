# SmallBank benchmark
The SmallBank OLTP benchmark

## Important parameters
The main experiment parameters are defined in `sb.json`.
  * `num_coro`: Number of coroutines per worker thread
  * `base_port_index`: The 0-based index of the first RDMA port to use
  * `num_ports`: Number of RDMA ports to use starting from `base_port_index`
  * `num_qps`: Number of SEND queues per thread. Can improve performance when
     there are a small number of cores.
  * `postlist`: The maximum number of packets sent using one Doorbell by the
     RPC subsystem.
  * `numa_node`: The NUMA node to run all threads on.
  * `num_machines`: Number of machines in the cluster.
  * `workers_per_machine`: Number of threads per machine.
  * `num_backups`: Number of backup partitions per primary partition.
  * `use_lock_server`: Currently unused.

The configuration of MICA hash tables used for database tables are in the
`sb_json` directory. The number of SmallBank accounts and the workload skew are
specified in `sb_defs.h`.

## Running the benchmark
At machine `i` in `{0, ..., num_machines - 1}`, execute `./run-servers.sh i`
