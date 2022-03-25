from collections import Counter
from concurrent import futures
import glob
import sys
import threading
import time

import grpc
import driver_pb2
import driver_pb2_grpc
import worker_pb2
import worker_pb2_grpc
from worker import WorkerServicer


class Driver(driver_pb2_grpc.DriverServicer):
    def __init__(self):
        super().__init__()
        self.workers = {}
        self.map_files = {}

    def Start(self, request, context):
        def get_worker():
            for k, v in self.workers.items():
                if v[0] == 0:
                    return k
            return False

        def mapper(worker_key, file, map_task_id, m):
            self.workers[worker_key][0] = 1
            r = worker_pb2.MapInput(file_path=file, map_task_id=map_task_id, m=m)
            r = self.workers[worker_key][1].Map(r)
            self.workers[worker_key][0] = 0
            print(f"[!] [DRIVER] [MAP] Map operation '{map_task_id}' on the file '{file}' terminated with code: '{r.code}' and message: {r.msg}.")
            return r
        
        def reducer(worker_key, reduce_task_id):
            self.workers[worker_key][0] = 1
            r = worker_pb2.ReduceId(reduce_task_id=reduce_task_id)
            r = self.workers[worker_key][1].Reduce(r)
            print(f"[!] [DRIVER] [REDUCE] Reduce operation '{reduce_task_id}' terminated with code: '{r.code}' and message: {r.msg}.")
            return r

        raw_files = glob.glob(request.file_path + "/*.txt")
        M = request.m
        ports = [int(port) for port in request.ports.split(',')]
        for file in raw_files:
            self.map_files[file] = 0
        
        for port in ports:
            channel = grpc.insecure_channel(f'localhost:{port}')
            grpc.channel_ready_future(channel).result(timeout=10)
            self.workers[port] = [0, worker_pb2_grpc.WorkerStub(channel)]
            self.workers[port][1].SetDriverPort(worker_pb2.DriverPort(port=int(sys.argv[1])))

        # start_time = time.time()

        with futures.ThreadPoolExecutor() as executor:
            for index, file in enumerate(raw_files):
                tmp_worker = get_worker()
                while tmp_worker == False:
                    time.sleep(5)
                    tmp_worker = get_worker()
                executor.submit(mapper, key=tmp_worker, file=file, map_task_id=index, m=M)

        with futures.ThreadPoolExecutor() as executor:
            for index in range(M):
                tmp_worker = get_worker()
                while tmp_worker == False:
                    time.sleep(5)
                    tmp_worker = get_worker()
                executor.submit(reducer, worker_key=tmp_worker, reduce_task_id=index)

        for port, (state, worker_stub)in self.workers.items():
            worker_stub.die(worker_pb2.empty())
        return driver_pb2.Status(code=200, msg='ok')


def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    driver_pb2_grpc.add_DriverServicer_to_server(Driver(), server)
    driver_port = sys.argv[1]
    server.add_insecure_port("127.0.0.1:%s"%(driver_port))
    server.start()
    print("Driver running on 127.0.0.1:%s"%(driver_port))
    # server.wait_for_termination()
    try:
        while True:
            print("Driver is on | nbr threads %i"%(threading.active_count()))
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)

if __name__ == "__main__":
    server()







