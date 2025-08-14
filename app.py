from dotenv import load_dotenv
from flask import Flask, render_template, request, Response
import pandas as pd 
import requests
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()

API_KEY = os.environ.get('API_KEY')

app = Flask(__name__, template_folder='templates')

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:admin@localhost/forecasts'
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

db=SQLAlchemy(app)

with app.app_context():
    db.create_all()

@app.route('/')
def index():
    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_table("table_name", conn)

        data = data.to_html(index=False, header=False, justify='center', classes='table table-striped')
    return render_template('index.html', data=data)

@app.route('/post_data', methods=['POST'])
def post_data():
        curveid = request.form['curveid']

        if curveid.isnumeric():
            base_url = 'https://strial1.rdms.refinitiv.com/api'
            CurveID = str(curveid)
            headers = { 'Authorization' : API_KEY }
            ScenarioID = 0

            metadata_url = f"{base_url}/v1/Metadata/{CurveID}"
            print('requesting curve ' + CurveID + ' data..')
            meta_response = requests.get(metadata_url, headers=headers)

            print("== Metadata Curve ==")
            if meta_response.status_code == 200:
                print("Metadata recieved...")
            else:
                print(f"failed to get metadata: {meta_response.status_code}")
                print(meta_response.text)

            values = meta_response.json()

            tags_list = values['tags']
            curve_dict = {tag['name']: tag['value'] for tag in tags_list}
            curve_dict['curveID'] = values['curveID']

            df = pd.DataFrame.from_dict(curve_dict, orient='index', columns=['Value'])

            ForecastDate = df.loc['CreateDate', 'Value'] # for a forecast type, its the same as CreateDate

            metadata_url = f"{base_url}/v1/CurveValues/Forecast/{CurveID}/{ScenarioID}/{ForecastDate}"

            print('requesting curve ' + CurveID + ' data..')
            meta_response = requests.get(metadata_url, headers=headers)

            print("== Curve Values - Forecast ==")
            if meta_response.status_code == 200:
                print("Curve values received successfully...")
            else:
                print(f"failed to get curve values: {meta_response.status_code}")
                print(meta_response.text)

            df_date = pd.DataFrame.from_dict(meta_response.json())
            df_date['valueDate'] = pd.to_datetime(df_date['valueDate'])

            # Store to DB
            df_date.to_sql('table_name', engine)

        return "Successfully stored data to db"

if __name__ == '__main__':
    app.run(host='127.0.0.1', debug=True, port=5555)