# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--port", help="Número de puerto", default = "9999")
args = parser.parse_args()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', int(args.port)))

print("Enviando mensajes al puerto %s. Pulsa Control+C para salir" % args.port)
while True:
    inpt = raw_input('Mensaje... ')
    s.send(inpt + "\n")