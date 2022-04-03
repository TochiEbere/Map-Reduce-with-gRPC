This is a distributed MapReduce program to solve the word count problem using python and gRPC.

It contains 3 main python file: driver.py, worker.py, and client.py.

- The driver.py launches the master node which is responsible for spliting and distributing the tasks to the available workers. It accepts as commandline input the driver port
- The worker.py lauches the worker node and is responsible for the map and reduce operations. It accepts as commandline input the worker port
- The client.py provides the input files and the number of reduce operations (m) and the worker ports. It accepts as commandline input the input folder containing the files, number of reduce operations, and worker ports.

The repository also conatins proto files for the worker and driver which contains RPC servcie and data definitions for the gRPC connections.
The worker.proto file contains rpc service definitions for the map and reduce operations

To run the code base on your local machine, begin by running:

```bash
> python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. driver.proto
> python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. worker.proto
```

This will output 4 files namely driver_pb2.py, worker_pb2.py, driver_pb2_grpc.py, worker_pb2_grpc.py. These files provide gRPC client and server code, as well as the regular protocol buffer code for populating, serializing, and retrieving your message types.

Next, start the worker nodes, each in a separate terminal by running:
```bash
> python3 worker.py 5001
> python3 worker.py 5002
> python3 worker.py 5003
> python3 worker.py 5004
> python3 worker.py 5005
```
This is followed by starting the driver node and running the client.py file each in separate terminals

```bash
> python3 driver.py 5000
```
```bash
> python3 client.py 5001 inputs/inputs 4 5001 5002 5003 5004 5005
```
Where 4 is the numberof reduce operations and the parameters following are the running worker ports
