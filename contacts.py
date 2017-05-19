"""A test module to demo slqlite3 usage."""

import sqlite3
import logging

# Get logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Add handler at debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def _handle_db_input(db):
    """Handle the connection to a database."""
    if isinstance(db, str):
        db = sqlite3.connect(db)
    elif isinstance(db, sqlite3.Connection):
        pass
    else:
        raise TypeError('database name or connection object expected')
    return db


def _scrub(field):
    """Avoiding Bobby Tables."""
    return ''.join(c for c in field if c.isalnum())


def create_db(sql=None):
    """Demonstrate db creation."""
    if sql is None:
        sql = """
        CREATE TABLE users(
            id INTEGER PRIMARY KEY,
            name TEXT,
            phone TEXT,
            email TEXT unique,
            password TEXT)
        """

    db = sqlite3.connect('mydb.db')
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    return db


def drop_table(db, table):
    """Drop specified table from input database."""
    sql = """DROP TABLE """ + table
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()


def insert_info(db, table, info):
    """Insert specific info into table.

    Parameters
    ----------
    info: dict or list
        Dict[str, List] of values to insert

    db: str or sqlite3.Connection
        name of database or connection to database

    table: str
        name of table to connect to
    """
    db = _handle_db_input(db)
    table = _scrub(table)

    cursor = db.cursor()

    columns = [k for k in info.keys()]
    columns_sql = '(' + ','.join(columns) + ')'
    values_sql = '(' + ','.join(['?'] * len(columns)) + ')'

    values_columnwise = [info[k] for k in columns]
    values_rowwise = list(zip(*values_columnwise))

    sql = """INSERT INTO """ + table + ' ' + \
        columns_sql + """ VALUES""" + values_sql

    logger.info(sql)
    logger.info('Inserting data')

    cursor.executemany(sql, values_rowwise)
    db.commit()


def display_table(db, table):
    """Print the table with the given name to the screen."""
    # Handle inputs
    db = _handle_db_input(db)
    table = _scrub(table)

    # Set up sql stem for table select
    sql = "SELECT * FROM " + table
    # Initiate cursor
    cursor = db.cursor()
    cursor.execute(sql)

    columns = get_table_info(db, table)

    str_stem = '\t\t'.join(['{:8}'] * len(columns))

    logger.info(str_stem.format(*columns))
    separator_list = ["--------"] * len(columns)
    logger.info(str_stem.format(*separator_list))

    # print output to screen
    for row in cursor.fetchall():
        logger.info(str_stem.format(*row))


def get_table_info(db, table):
    """Return the column names of the input table."""
    # Handle inputs
    db = _handle_db_input(db)
    table = _scrub(table)

    sql = "PRAGMA table_info(" + table + ")"

    # Initiate cursor stuff
    cursor = db.cursor()
    cursor.execute(sql)

    columns = [field[1] for field in cursor.fetchall()]

    return columns


def list_tables(db):
    """List the tables present in the database."""
    db = _handle_db_input(db)

    cursor = db.cursor()

    cursor.execute("""SELECT * FROM sqlite_master WHERE type='table'""")

    for row in cursor.fetchall():
        logger.info(row)


if __name__ == '__main__':
    stocks = {
        'symbol': ['BA', 'DGE'],
        'name': ['Boeing', 'Diageo'],
        'price': [177.30, 2289.50],
        'date': ['Jan 4th 2001', '05/03/2016']
    }
    insert_info('mydb.db', 'stocks', stocks)
