from dotenv import load_dotenv
from flask import Flask, render_template, request
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import datetime 
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

last_run_time = None


def forecast_cron_job():
    global last_run_time
    last_run_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    app.logger.info(f"Running forecast_cron_job at {last_run_time}")


# initialize scheduler
scheduler = BackgroundScheduler()
scheduler.start()

# Schedule the job
scheduler.add_job(
    func=forecast_cron_job,
    trigger=IntervalTrigger(seconds=5),
    id='forecast_job',
    name='Forecast job',
    replace_existing=True,
)

# base_url = 'https://strial1.rdms.refinitiv.com/api'
# CurveID = '146496378'
# headers = { 'Authorization' : API_KEY }
# ScenarioID = 0
# # Datetime settings
# yesterday_string = datetime.datetime.now() - datetime.timedelta(days=2)
# yesterday_string = yesterday_string.strftime('%Y-%m-%d')
# ForecastDate = yesterday_string

# curve_data_url = f"{base_url}/v1/CurveValues/Forecast/{CurveID}/{ScenarioID}/{ForecastDate}"

# print('requesting curve ' + CurveID + ' data..')
# curve_data_response = requests.get(curve_data_url, headers=headers)

# print("== Curve Values - Forecast ==")
# if curve_data_response.status_code == 200:
#     print("Curve values received successfully...")
# else:
#     print(f"failed to get curve values: {curve_data_response.status_code}")
#     print(curve_data_response.text)

# df_date = pd.DataFrame.from_dict(curve_data_response.json())
# df_date['valueDate'] = pd.to_datetime(df_date['valueDate'])
# # set the valuedate column as the index
# df_date.set_index('valueDate', inplace=True)

# # Store to DB with upsert logic
# table_name = f'forecast_{CurveID}'
# df_date.reset_index(inplace=True)  # valueDate is now a column

# # Create table if it doesn't exist
# if not engine.dialect.has_table(engine.connect(), table_name):
#     metadata = MetaData()
#     columns = [
#         Column('valueDate', DateTime, primary_key=True, unique=True)
#     ]
#     for col in df_date.columns:
#         if col != 'valueDate':
#             columns.append(Column(col, Float))  # or String/Integer as needed
#     table = Table(table_name, metadata, *columns)
#     metadata.create_all(engine)

# # Reflect the existing table
# metadata = MetaData()
# metadata.reflect(bind=engine)
# table = Table(table_name, metadata, autoload_with=engine)

# # Insert or update the data
# with engine.begin() as conn:
#         for _, row in df_date.iterrows():
#             stmt = insert(table).values(**row.to_dict())
#             update_dict = {col: stmt.excluded[col] for col in df_date.columns if col != 'valueDate'}
#             stmt = stmt.on_conflict_do_update(
#                 index_elements=['valueDate'],
#                 set_=update_dict
#             )
#             conn.execute(stmt)

if __name__ == '__main__':
    app.run(host='127.0.0.1', debug=True, port=5555)