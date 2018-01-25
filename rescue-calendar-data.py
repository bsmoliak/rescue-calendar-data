# -*- coding: utf-8 -*-
"""Extract data from a sqlite database file and output as a csv text file.

Args:
    input_file (str): Path to input file, including filename and extension.
    output_file (str): Path to output file, including filename and extension.

Returns:
    Saves a .csv text file with two columns: event and date

Example:
    Extract data in `./calendar.db`::
        $ python rescue-calendar-data.py ./calendar.db ./calendar.csv
"""
import click
from pandas import DataFrame, to_datetime
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """Create a database connection to a SQLite database.

    Args:
        db_file (str): Path to database file, including filename and extension.

    Returns:
        Connection object or None.
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None


def select_events_by_description(conn, selector):
    """Query events by description using a selector

    Args:
        conn: SQLite connection object
        selector (str): substring to select using

    Return:
        pandas dataframe containing rows resulting from query
    """
    cur = conn.cursor()
    cur.execute("SELECT title, dtstart " +
                "FROM `Events`" +
                "WHERE `description` " +
                "LIKE '{}'".format(selector))

    df = DataFrame(cur.fetchall())
    df.columns = ["Event", "Date"]

    return df


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
def main(input_file, output_file):
    """Call functions to filter data and produce a NetCDF file"""

    if input_file.endswith(".db"):
        conn = create_connection(input_file)
    else:
        raise ValueError("input_file must end in .db")

    df = select_events_by_description(conn, "%Sticker%")

    # Convert unix epoch timestamps to datetime
    df["Date"] = to_datetime(df["Date"], unit="ms")
    # Remove commas, since we intend to output a csv file
    df["Event"] = df["Event"].map(lambda x: x.replace(",", ""))
    # Add a julian day column, sort by it, and drop it
    df["JulianDay"] = [int(format(dt, "%j")) for dt in df["Date"]]
    df.sort_values(by="JulianDay", inplace=True)
    df.drop(axis=1, columns="JulianDay", inplace=True)

    if output_file.endswith(".csv"):
        df.to_csv(output_file, index=False)
        print("Extraction complete!")
    else:
        raise ValueError("output_file must end in .csv")


if __name__ == "__main__":
    main()
