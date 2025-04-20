from fastapi

app=Flask(__name__)

@app.route('/')
def hello():
    return "Hello world"

app.run(host='0.0.0.0')
