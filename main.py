import psycopg2
import yaml
import logging


# name       : p_read_config
# desc       : Procedure that reads config file for database connection
# author     : Igor Sliskovic
#
# requires   : Yml file path and Yml file format
#              database: your_database_name
#              user: your_user_name
#              password: your_password
#              host: your_host
#              port: your_port
#
# guarantees : Returns database config info
#
# parameter  : i_yaml_file - Yml file location


def p_read_config(i_yaml_file):
    try:
        with open(i_yaml_file, 'r') as file:
            o_db_config = yaml.safe_load(file)

        return o_db_config
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print("Error: ", e)
        raise


# name       : p_connect_to_source
# desc       : Procedure that opens connection to source database
# author     : Igor Sliskovic
#
# requires   : existing source_db.yml
#
# guarantees : Opens database connection to source db

def p_connect_to_source():
    l_db_config = p_read_config('config/source_db.yml')
    try:
        o_connection = psycopg2.connect(
            host=l_db_config['host'],
            database=l_db_config['database'],
            user=l_db_config['user'],
            password=l_db_config['password'],
            port=l_db_config['port']
        )
        return o_connection
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print("Error: ", e)
        raise


# name       : p_connect_to_target
# desc       : Procedure that opens connection to target database
# author     : Igor Sliskovic
#
# requires   : existing target_db.yml
#
# guarantees : Opens database connection to target db
def p_connect_to_target():
    l_db_config = p_read_config('config/target_db.yml')
    try:
        o_connection = psycopg2.connect(
            host=l_db_config['host'],
            database=l_db_config['database'],
            user=l_db_config['user'],
            password=l_db_config['password'],
            port=l_db_config['port']
        )
        return o_connection
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print("Error: ", e)
        raise


# name       : p_get_table_definition
# desc       : Procedure queries postgresql to get list of columns of a table
# author     : Igor Sliskovic
#
# requires   : Open connection and a table name
#
# guarantees : List of columns for the inputed table
#
# parameter  : i_connection - Open connection to a database
# parameter  : i_table_name - Table name
def p_get_table_definition(i_connection, i_table_name):
    try:
        with i_connection.cursor() as l_cursor:
            l_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s",
                             (i_table_name,))
            o_table_definition = l_cursor.fetchall()
        return o_table_definition
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print("Error: ", e)
        raise


# name       : p_move_data_for_table
# desc       : Procedure that moves data from one database to another for a given table
# author     : Igor Sliskovic
#
# requires   : For a table in one postgresql database in a certain schema, move the data into the same table to another
#              postgresql database table with same name
#
# guarantees : Data extraction for stage area either a full load if target is empty or incremental load if not
#
# parameter  : i_table_name - Table we want to extract
# parameter  : i_source_schema - Source schema
# parameter  : i_target_schema - Target schema
def p_move_data_for_table(i_table_name, i_source_schema, i_target_schema):
    l_connection_source = None
    l_connection_target = None
    try:
        # Open source connection
        l_connection_source = p_connect_to_source()
        l_source_table_definition = p_get_table_definition(l_connection_source, i_table_name)
        l_source_columns = [column[0] for column in l_source_table_definition]

        # Open target connection
        l_connection_target = p_connect_to_target()
        l_target_table_definition = p_get_table_definition(l_connection_target, i_table_name)
        l_target_columns = [column[0] for column in l_target_table_definition]

        # Generate the INSERT query template with dynamic column names
        insert_query_template = f"INSERT INTO {i_target_schema}.{i_table_name} ({', '.join(l_source_columns)}) " \
                                f"VALUES ({', '.join(['%s'] * len(l_source_columns))})"

        with l_connection_source.cursor() as l_source_cursor, l_connection_target.cursor() as l_target_cursor:
            # Check if the target table is empty
            l_target_cursor.execute(f"SELECT COUNT(*) "
                                    f"FROM {i_target_schema}.{i_table_name}")
            l_target_row_count = l_target_cursor.fetchone()[0]
            if l_target_row_count == 0:
                # If the target table is empty, perform a full load
                l_source_cursor.execute(f"SELECT {', '.join(l_target_columns)} "
                                        f"FROM {i_source_schema}.{i_table_name}")
            else:
                # If the target table is not empty, perform a delta load
                l_target_cursor.execute(f"SELECT MAX(updated_date) "
                                        f"FROM {i_target_schema}.{i_table_name}")
                l_max_update_date = l_target_cursor.fetchone()[0]
                l_source_cursor.execute(f"SELECT {', '.join(l_target_columns)} "
                                        f"FROM {i_source_schema}.{i_table_name} "
                                        f"WHERE updated_date > '{l_max_update_date}'")

            while True:
                # Use fetchmany to retrieve a batch of rows from the source database
                rows = l_source_cursor.fetchmany(size=1000)  # Adjust the batch size as needed
                if not rows:
                    break

                # Insert the data into the target database
                l_target_cursor.executemany(insert_query_template, rows)
                l_connection_target.commit()
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print("Error: ", e)
        raise
    finally:
        l_connection_source.close()
        l_connection_target.close()


# name       : p_main
# desc       : Procedure to start ETL
# author     : Igor Sliskovic
if __name__ == '__main__':
    p_move_data_for_table(i_table_name='products', i_source_schema='Core', i_target_schema='dwh')
