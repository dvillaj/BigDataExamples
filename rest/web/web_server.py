from flask import Flask, render_template
import requests

# Set up Flask
app = Flask(__name__)

@app.route("/executive/<name>")
def executive(name):
    response = requests.get('http://localhost:5000/executive/%s' % name)
    return render_template('table.html', executives=list(response.json()))

@app.route("/executive")
def executives():
    response = requests.get('http://localhost:5000/executive')
    return render_template('table.html', executives=list(response.json()))

if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 5001)