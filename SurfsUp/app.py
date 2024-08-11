# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_, bindparam
from flask import Flask, jsonify
import pandas as pd
import os
import json

#################################################
# Database Setup
#################################################
current_dir = os.path.dirname(__file__)
db_path = os.path.join(current_dir, '../Resources/hawaii.sqlite')
engine = create_engine(f"sqlite:///{db_path}")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
tables = {}
for table_name, table_class in Base.classes.items():
    tables[table_name] = table_class

# Access the tables by name
station_table = tables.get('station')
measurement_table = tables.get('measurement')

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

# Define the homepage route
@app.route('/')
def home():
    return "Welcome to the Climate App. Available routes: /, /api/v1.0/precipitation, /api/v1.0/stations, /api/v1.0/tobs, /api/v1.0/<start>, /api/v1.0/<start>/<end>"

#################################################
# Flask Routes
#################################################

# Define the /api/v1.0/precipitation route
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Add code to retrieve and return the precipitation data as JSON
    most_recent_date = session.query(func.max(measurement_table.date)).scalar()
    one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(months=12)
    # Perform a query to retrieve the data and precipitation scores
    query = f"SELECT date, prcp FROM measurement WHERE date >= '{one_year_ago}'"
    precipitation_data = pd.read_sql(query, engine)
    precipitation_data = precipitation_data.sort_values('date')
    precipitation_data.set_index('date',inplace=True)
    data_dict = precipitation_data['prcp'].to_dict()
    json_data = json.dumps(data_dict)
    return jsonify(json_data)

# Save the query results as a Pandas DataFrame. Explicitly set the column names
#precipitation_data = pd.read_sql(query, engine)

# Define the /api/v1.0/stations route
@app.route('/api/v1.0/stations')
def stations():
    # Add code to retrieve and return the temperature observations for the most active station as JSON
    active_stations = pd.read_sql("SELECT station from station ORDER BY station ASC", engine)
    station_dict = active_stations['station'].to_dict()
    json_data = json.dumps(station_dict)
    return(jsonify(json_data))

# Define the /api/v1.0/tobs route
@app.route('/api/v1.0/tobs')
def tobs():
    # Add code to retrieve and return the temperature observations for the most active station as JSON
    Measurement = Base.classes.measurement
    most_recent_date = session.query(func.max(measurement_table.date)).scalar()
    one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(months=12)
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')  # Convert to string in 'YYYY-MM-DD' format
    results = session.query(Measurement.date, Measurement.tobs).filter(and_(Measurement.station == 'USC00519281', Measurement.date >= one_year_ago_str)).all()
    # Create a DataFrame from the query results
    df = pd.DataFrame(results, columns=['Date', 'Temperature'])
    df.set_index('Date', inplace=True)
    # Convert DataFrame to dictionary
    data_dict = df.to_dict()

    # Return JSON response using jsonify
    return jsonify(data_dict)


# Define the /api/v1.0/<start> and /api/v1.0/<start>/<end> routes
@app.route('/api/v1.0/<start>')
def start_temperatures(start):
    Measurement = Base.classes.measurement
    # Query database to retrieve temperature data from the given start date to the end of the dataset
    results = session.query(func.min(Measurement.tobs), 
                            func.max(Measurement.tobs), 
                            func.avg(Measurement.tobs)
    ).filter(Measurement.date >= start).all()

    # Extract the temperature values from the query results
    min_temp, max_temp, avg_temp = results[0]

    # Create a dictionary with the temperature values
    temperature_data = {
        'start_date': start,
        'min_temperature': min_temp,
        'max_temperature': max_temp,
        'avg_temperature': avg_temp
    }

    # Return JSON response
    return jsonify(temperature_data)


@app.route('/api/v1.0/<start>/<end>')
def temperature_range(start, end=None):
    Measurement = Base.classes.measurement
    # Query database to retrieve temperature data from the given start date to the end of the dataset
    results = session.query(func.min(Measurement.tobs), 
                            func.max(Measurement.tobs), 
                            func.avg(Measurement.tobs)
    ).filter(Measurement.date >= start, Measurement.date <=end).all()

    # Extract the temperature values from the query results
    min_temp, max_temp, avg_temp = results[0]

    # Create a dictionary with the temperature values
    temperature_data = {
        'start_date': start,
        'end_date': end,
        'min_temperature': min_temp,
        'max_temperature': max_temp,
        'avg_temperature': avg_temp
    }

    # Return JSON response
    return jsonify(temperature_data)

if __name__ == '__main__':
    app.run(debug=True)