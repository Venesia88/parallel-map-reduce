# parallel-map-reduce
Implement the scheduling and parallel programming model of big data processing framework, MapReduce, using MPI and pthread. The jobtracker and tasktrackers are implemented as MPI processes, threads are used for executing computing tasks and IO.

## Implementation (in the report)

## Run 
`srun -N<NODES> -c<CPUS> ./hw4 JOB_NAME NUM_REDUCER DELAY INPUT_FILENAME CHUNK SIZE LOCALITY_CONFIG_FILENAME OUTPUT_DIR`
`NODES`: number of nodes
`CPUS`: number of CPUs
`JOB_NAME`: name of the job
`NUM_REDUCER`: number of reducer
`DELAY`: sleeping time in seconds for reading a remote data chunk in a mapper task.
`INPUT_FILENAME`: path of the input file
`CHUNK_SIZE`: number of lines for a data chunk in the input file.
`LOCALITY_CONFIG_FILENAME`: the path of the configuration file that contains a list of mapping between the chunkID and nodeID.
`OUTPUT_DIR`: the path of the output directory.

