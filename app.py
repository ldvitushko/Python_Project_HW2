from flask import Flask
import main

app = Flask(__name__)


@app.route('/')
def get_weather_data():
    return main.get_data_from_db().to_html()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
