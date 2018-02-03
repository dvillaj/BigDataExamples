from flask import Flask, jsonify
from pymongo import MongoClient
from flask_restful import Api
from flask_restful_swagger import swagger
import bson.json_util 

# Set up Flask
app = Flask(__name__)
api = swagger.docs(Api(app), apiVersion='1', api_spec_url="/api/v1/spec")

# Set up Mongo
client = MongoClient() # defaults to localhost
db = client.data

# Fetch from/to totals, given a pair of email addresses
@app.route("/executive/<name>")
@swagger.operation(notes='some really good notes')
def executive(name):
    """
    Describing one unicorn
    """
    executive = db.executives.find({"name": name}, {'_id' : 0})
    return jsonify(list(executive))

@app.route("/executive")
@swagger.operation(notes='some really good notes. Yes indeed!')
def executives():
    """
    Describing all unicorns
    """
    executive = db.executives.find({}, {'_id' : 0})
    return bson.json_util.dumps(list(executive)) 


if __name__ == "__main__":
    app.run(host = '0.0.0.0', debug=False)