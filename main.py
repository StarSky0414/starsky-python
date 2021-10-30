# pip3 install grpcio
# pip3 install grpcio-tools
# pip3 install protobuf
# pip3 install googleapis-common-protos
# python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. .../src/main/proto/message-test.proto

# import proto.outFile
# import make_file_pb2_grpc
import grpc
# 并发库
from concurrent import futures
import time

from file.downFile.downFile import DownFile
from proto.outFile import down_file_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    down_file_pb2_grpc.add_FileServicer_to_server(DownFile(), server)
    server.add_insecure_port('[::]:50051')
    # server.wait_for_termination(10)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()