from flask import Flask, request, jsonify, make_response

app = Flask(__name__)

@app.route('/')
def index():
    return "<h1>Hello, World!</h1>"

@app.route('/hello', methods=['POST', 'GET', 'PUT', 'DELETE'])
def hello_world():
    response = make_response('Hello, World!')
    response.status_code = 202
    response.headers['Content-Type'] = 'text/plain'
    return response


@app.route('/greet/<name>')
def hello(name):
    return f"Hello {name}"

@app.route('/add/<int:number1>/<int:number2>')
def add(number1, number2):
    return f"{number1} + {number2} = {number1 + number2}"

@app.route('/handle_url_params')
def handle_params():
    if 'greeting' in request.args.keys() and 'name' in request.args.keys():
        greeting = request.args.get('greeting', '')
        name = request.args.get('name', '')
        return f"{greeting}, {name}"
    else:
        return "Missing 'greeting' or 'name' parameter", 400

if __name__ == '__main__':
    app.run(host='127.0.0.1', debug=True, port=5555)