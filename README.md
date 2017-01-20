This Spark application will request a time sequence of tiles, split them up into sub-tiles, reduce those sub-tiles in
time (in this case, calculate their mean) in a distributed way, re-assemble those sub-tiles and write them to files.

For more information regarding IDE setup and inspecting Spark jobs on Hadoop, refer to the
[python-spark-quickstart](https://bitbucket.org/vitotap/python-spark-quickstart) project.

# Running the code
Scripts are provided for running the application locally as well as on the Hadoop cluster.

## Locally
The `run-local` script runs the application on the same machine. By default, output tiles (`subtile_average_xx_yy.tif`)
will be written to the working directory; the script will also accept a specific output directory as its first argument,
e.g.: `./run-local /tmp`

## On the Hadoop Cluster
The `run-cluster` script runs the application on the Hadoop cluster. In this case, an output directory as its first
argument is mandatory. Note that this output directory should be writable from the nodes in the cluster, such as a
directory on NFS.
