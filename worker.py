from collections import Counter
from concurrent import futures
import glob
import sys
import time
import threading

import grpc
import worker_pb2
import worker_pb2_grpc


class WorkerServicer(worker_pb2_grpc.WorkerServicer):
    def __init__(self):
        super().__init__()
        self.port = '4000'
        print("Worker initialised")

    def SetDriverPort(self, request, context):
        self.port = request.port
        return worker_pb2.Status(code=200, msg="ok")

    def Map(self, request, context):
        input_file = request.file_path
        N = request.map_task_id
        M = request.m

        with open(input_file, "r") as file:
            text = file.read()
            text = text.lower()
        # Removing special characters
        text = ''.join([letter if letter in 'abcdefghijklmnopqrstuvwxyz' else ' ' for letter in text])
        words = text.split()

        buckets = list()
        for i in range(M):
            buckets.append(list())

        position = lambda x: ord(x) - 97
        for word in words:
            bucket_position = position(word[0]) % M
            buckets[bucket_position].append(word)

        # save into intermediate files
        for bucket_id, word in enumerate(buckets):
            with open(f"./intermediate/mr-{0}-{1}".format(N, bucket_id), "w+") as file:
                file.write('\n'.join(word))
        return worker_pb2.Status(code=200, msg="ok")

    
    def Reduce(self, request, context):
        counter = Counter()
        reduce_task_id = request.reduce_task_id
        intermediate_files = glob.glob("./intermediate/*-%i"%(reduce_task_id))
        for file in intermediate_files:
            with open(file, "r") as file:
                words = file.read().split()
            counter.update(words)
        with open('./out/out-%i'%(reduce_task_id), "w+") as file:
            file.write('\n'.join('%s %s'%(word, count) for word, count in counter.items()))
        return worker_pb2.Status(code=200, msg='ok')

    def Terminate(self, request, context):
        return worker_pb2.Empty()

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerServicer_to_server(WorkerServicer(), server)
    worker_port = sys.argv[1]
    server.add_insecure_port("127.0.0.1:%s"%(worker_port))
    server.start()
    print("Worker running on 127.0.0.1:%s"%(worker_port))
    # server.wait_for_termination()
    try:
        while True:
            print("Worker is on | nbr threads %i"%(threading.active_count()))
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)


if __name__ == "__main__":
    server()


    









    

