import sys

import grpc
import driver_pb2
import driver_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:4000')

    print("[*] Connecting to the server...")
    grpc.channel_ready_future(channel).result(timeout=10)
    driver_stub = driver_pb2_grpc.DriverStub(channel=channel)
    ports = ','.join(sys.argv[3:])
    reqst = driver_pb2.Data(file_path=sys.argv[1], m=int(sys.argv[2]), ports=ports)
    res = driver_stub.Start(reqst)
    print("[!] Operation terminated with code: %i and message: %s"%(res.code, res.msg))

if __name__ == "__main__":
    run()