# encoding: utf-8

from flask import Flask, jsonify, request
from pymongo import MongoClient
from flask_restful import Api
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger.utils import swag_from
import bson.json_util 

# Set up Flask
app = Flask(__name__)
app.json_encoder = LazyJSONEncoder
template = dict(
    info={
        'title': 'Awesome API', 
        'version': '1.0.0',
        'description': 'Api de ejemplo',
        'termsOfService': '/terms'
    },
    host=LazyString(lambda: request.host),
    schemes=[LazyString(lambda: 'https' if request.is_secure else 'http')]
)
Swagger(app, template=template)


# Set up Mongo
client = MongoClient() # defaults to localhost
db = client.data

@app.route("/executive/<string:name>", methods=['GET'])
@swag_from('executiveByName.yml')
def executive(name):
    executive = db.executives.find({"name": name}, {'_id' : 0})
    return jsonify(list(executive))

@app.route("/executive", methods=['GET'])
@swag_from('executive.yml')
def executives():
    executive = db.executives.find({}, {'_id' : 0})
    return bson.json_util.dumps(list(executive)) 

if __name__ == "__main__":
    app.run(host = '0.0.0.0', debug=False)