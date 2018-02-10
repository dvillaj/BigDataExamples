# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", help="Número de puerto")
args = parser.parse_args()

print("Server started. Listening at port %s" %  args.port)

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind(('localhost', int(args.port)))
serversocket.listen(5) # Número máximo de conexiones concurrentes

while True:
    connection, address = serversocket.accept()
    print("Connection accepted from " + repr(address[1]))

    connection.send("Server approved connection\n")
    buf = connection.recv(1026)
    if len(buf) > 0:
        print(repr(address[1]) + ": " + buf)
    connection.close()