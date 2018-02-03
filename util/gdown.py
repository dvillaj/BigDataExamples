# -*- coding: utf-8 -*-

import requests
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('fileName', help="Nombre del fichero")
parser.add_argument("--id", help="ID del fichero contenida dentro del link público")
args = parser.parse_args()

if args.id is None:
    parser.error("Es necesario especificar el ID del fichero!")
    sys.exit(1)

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

if __name__ == "__main__":
    download_file_from_google_drive(args.id, args.fileName)