from dotenv import load_dotenv
from flask import Flask, render_template, request
import pandas as pd
import requests
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
from sqlalchemy import Column, Float, DateTime
from sqlalchemy.dialects.postgresql import insert
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64

load_dotenv()  # Get environment variables

app = Flask(__name__, template_folder="templates")

# Database settings
app.config["SQLALCHEMY_DATABASE_URI"] = (
    "postgresql://postgres:admin@localhost/forecasts"
)
engine = create_engine(app.config["SQLALCHEMY_DATABASE_URI"])

db = SQLAlchemy(app)

with app.app_context():
    db.create_all()

# Global variables
CurveID = "146496378"
base_url = "https://strial1.rdms.refinitiv.com/api"
API_KEY = os.environ.get("API_KEY")
headers = {"Authorization": API_KEY}
ScenarioID = 0


@app.route("/")
def index():
    metadata_url = f"{base_url}/v1/Metadata/{CurveID}"
    print("requesting curve " + CurveID + " data..")
    meta_response = requests.get(metadata_url, headers=headers)

    print("== Metadata Curve ==")
    if meta_response.status_code == 200:
        print("Metadata recieved...")
    else:
        print(f"failed to get metadata: {meta_response.status_code}")
        print(meta_response.text)

    values = meta_response.json()

    # Extract the 'tags' list
    tags_list = values["tags"]

    # Create a dictionary with 'name' as key and 'value' as value
    curve_dict = {tag["name"]: tag["value"] for tag in tags_list}

    # Add the 'curveID' to the dictionary
    curve_dict["curveID"] = values["curveID"]

    # Convert the dictionary to a DataFrame, orienting by index
    df = pd.DataFrame.from_dict(curve_dict, orient="index", columns=["Value"])

    with engine.connect() as conn, conn.begin():
        if conn.dialect.has_table(conn, f"forecast_{CurveID}"):
            data = pd.read_sql_table(f"forecast_{CurveID}", conn)
            data["valueDate"] = pd.to_datetime(data["valueDate"])
            data.sort_values("valueDate", inplace=True)
        else:
            data = "No data available for the specified curve ID."

    # Generate plot
    img = None
    if not isinstance(data, str) and not data.empty:
        plt.figure(figsize=(10, 5))
        plt.plot(data['valueDate'], data['value'], marker='o')
        plt.xlabel('Date')
        plt.ylabel('Value')
        plt.title('Forecast Curve')
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        img = base64.b64encode(buf.getvalue()).decode('utf8')
        plt.close()
    
    return render_template(
        "index.html",
        data=data.to_html(classes="table table-striped", justify="left", index=False),
        curveid=CurveID,
        shape=data.shape,
        earliest_date=data["valueDate"].min() if not data.empty else None,
        latest_date=data["valueDate"].max() if not data.empty else None,
        metadata=meta_response.json(),
        metadata_df=df.to_html(
            classes="table table-striped", justify="left", index=True
        ),
        plot_img=img,
    )


@app.route("/post_data", methods=["GET", "POST"])
def post_data():
    # Datetime settings
    yesterday_string = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_string = yesterday_string.strftime("%Y-%m-%d")
    ForecastDate = yesterday_string

    curve_data_url = (
        f"{base_url}/v1/CurveValues/Forecast/{CurveID}/{ScenarioID}/{ForecastDate}"
    )

    print("requesting curve " + CurveID + " data..")
    curve_data_response = requests.get(curve_data_url, headers=headers)

    print("== Curve Values - Forecast ==")
    if curve_data_response.status_code == 200:
        print("Curve values received successfully...")
    else:
        print(f"failed to get curve values: {curve_data_response.status_code}")
        print(curve_data_response.text)

    df_date = pd.DataFrame.from_dict(curve_data_response.json())
    df_date["valueDate"] = pd.to_datetime(df_date["valueDate"])
    # set the valuedate column as the index
    df_date.set_index("valueDate", inplace=True)

    # Store to DB with upsert logic
    table_name = f"forecast_{CurveID}"
    df_date.reset_index(inplace=True)  # valueDate is now a column

    # Create table if it doesn't exist
    if not engine.dialect.has_table(engine.connect(), table_name):
        metadata = MetaData()
        columns = [
            Column("valueDate", DateTime, primary_key=True, unique=True)  # Datetime
        ]
        for col in df_date.columns:
            if col != "valueDate":
                columns.append(Column(col, Float))  # Float as its double precision
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)

    # Reflect the existing table
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = Table(table_name, metadata, autoload_with=engine)

    # Insert or update the data
    with engine.begin() as conn:
        for _, row in df_date.iterrows():
            stmt = insert(table).values(**row.to_dict())
            update_dict = {
                col: stmt.excluded[col] for col in df_date.columns if col != "valueDate"
            }

            # Use on_conflict_do_update to handle conflicts
            stmt = stmt.on_conflict_do_update(
                index_elements=["valueDate"], set_=update_dict
            )
            conn.execute(stmt)

    return "Data successfully inserted into the database"


if __name__ == "__main__":
    app.run(host="127.0.0.1", debug=True, port=5555)