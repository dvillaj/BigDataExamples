# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", help="Número de puerto")
parser.add_argument("--message", help="Mensaje", default = 'hello')
args = parser.parse_args()

import socket

clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientsocket.connect(('localhost', int(args.port)))
clientsocket.send(args.message)
