# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", help="Número de puerto")
args = parser.parse_args()

print("Server in localhost:%s" % args.port)

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind(('localhost', int(args.port)))
serversocket.listen(5) # become a server socket, maximum 5 connections

while True:
    connection, address = serversocket.accept()
    buf = connection.recv(64)
    if len(buf) > 0:
        print("Message from %s: %s" % (address, buf))