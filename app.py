from flask import Flask, render_template, request, jsonify, make_response

app = Flask(__name__, template_folder='templates')

@app.route('/')
def index():
    mylist = [10, 35, 56, 72, 91]
    return render_template('index.html', mylist=mylist)


@app.route('/other')
def other():
    message = 'Sample text'
    return render_template('other.html', message=message)


@app.template_filter('reverse')
def reverse(text):
    return text[::-1]


@app.template_filter('repeat')
def repeat(text, times=2):
    return text * times


@app.template_filter('alternate')
def alternate(s):
    return ''.join([c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(s)])


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