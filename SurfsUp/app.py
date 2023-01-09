# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Other Dependencies
import numpy as np
import pandas as pd
import datetime as dt
from flask import Flask, jsonify

# Create an app
app = Flask(__name__)


#### SQL Alchemy ORM
# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with = engine, reflect=True)
# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station


# Define what to do when a user hits 'home' page
@app.route("/")
def welcome():
    return (
        f"Surf's Up !!!<br/><br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2010-01-01<br/>"
        f"/api/v1.0/2010-01-01/2017-08-23"
    )


# Define what to do when a user hits 'precipitation' page
@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set
    latest_date_str = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date_str = latest_date_str[0]
    latest_date = dt.datetime.strptime(latest_date_str, '%Y-%m-%d')
    
    # Calculate the date one year from the last date in data set
    latest_date_1 = dt.datetime(latest_date.year - 1, latest_date.month, latest_date.day)

    """Return a list of all dates/precipitations"""
    # Query dates
    results = session.query(measurement.date, func.sum(measurement.prcp)).\
              filter(measurement.date <= latest_date).filter(measurement.date >= latest_date_1).\
              group_by(measurement.date).all()

    # Close session
    session.close()

    # Create a dictionary from the row data and append to a list of all_dates
    all_dates = []
    for date, prcp in results:
        date_dict = {}
        date_dict["date"] = date
        date_dict["prcp"] = round(prcp,2)
        all_dates.append(date_dict)

    # jsonify
    return jsonify(all_dates)


# Define what to do when a user hits 'stations' page
@app.route("/api/v1.0/stations")
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all station names"""
    # Query all stations from merged dataset
    results = session.query(measurement.station, station.name).filter(measurement.station == station.station).\
        group_by(station.name).all()
    
    # Close session
    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    # jsonify
    return jsonify(all_stations)


# Define what to do when a user hits 'tobs' page
@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of tobs for most active station"""
    # Find the most recent date in the data set.
    latest_date_str = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date_str = latest_date_str[0]
    latest_date = dt.datetime.strptime(latest_date_str, '%Y-%m-%d')
    
    # Calculate the date one year from the last date in data set.
    latest_date_1 = dt.datetime(latest_date.year - 1, latest_date.month, latest_date.day)
    
    # Find most active station
    most_active = session.query(measurement.station, func.count(measurement.station)).\
    group_by(measurement.station).order_by(func.count(measurement.station).desc()).first()[0]
    
    # Query most active station
    results = session.query(measurement.date, measurement.tobs).\
              filter(measurement.date <= latest_date).filter(measurement.date >= latest_date_1).\
              filter(measurement.station == most_active).all()

    # Close session
    session.close()

    # Convert list of tuples into normal list
    all_tobs = list(np.ravel(results))

    # jsonify
    return jsonify(all_tobs)


# Define what to do when a user selects 'start date' page
@app.route("/api/v1.0/<start_date>")
def results(start_date):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of min, max, avg temps after selected date"""
    # Query selected dates
    results = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
              filter(measurement.date >= start_date).all()

    # Close session
    session.close()

    # Convert list of tuples into normal list
    all_tobs = list(np.ravel(results))

    # jsonify
    return jsonify(all_tobs)


# Define what to do when a user selects 'start date' and 'end date'
@app.route("/api/v1.0/<start_date>/<end_date>")
def results2(start_date, end_date):

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of min, max, avg temps after selected date"""
    # Query selected dates
    results2 = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
              filter(measurement.date >= start_date).filter(measurement.date <= end_date).all()

    # Close session
    session.close()

    # Convert list of tuples into normal list
    all_tobs = list(np.ravel(results2))

    # jsonify
    return jsonify(all_tobs)


if __name__ == "__main__":
    app.run(debug=True)