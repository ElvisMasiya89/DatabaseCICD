import psycopg2
import os


def extract_stored_procedures(connection, output_directory):
    cursor = connection.cursor()
    cursor.execute("SELECT proname, prosrc FROM pg_proc WHERE prokind='p'")  # Query to fetch stored procedures

    for row in cursor.fetchall():
        procedure_name = row[0]
        procedure_source = row[1]

        # Create SQL file for each procedure
        file_name = f"{procedure_name}.sql"
        file_path = os.path.join(output_directory, file_name)

        with open(file_path, "w") as sql_file:
            sql_file.write(procedure_source)

    cursor.close()
    connection.close()
def extract_schema(connection, output_directory):
    cursor = connection.cursor()

    # Get all table names in the schema
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='wealthmanagerschema'")

    for row in cursor.fetchall():
        table_name = row[0]

        # Create SQL file for each table
        file_name = f"{table_name}.sql"
        file_path = os.path.join(output_directory, file_name)

        # Dump table definition to SQL file
        with open(file_path, "w") as sql_file:
            os.system(f"pg_dump -t {table_name} > {file_path}")

    cursor.close()


# Replace with your own database connection details
db_host = "DummyIP"
db_port = "DummyPort"
db_name = "DummyDB"
db_user = "DummyUser"
db_password = "DummyPassword"

# Replace with the output directory where you want to save the SQL files
output_directory = r'C:\\DB'

# Connect to the PostgreSQL database
try:
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Extract and generate SQL files for stored procedures
    extract_stored_procedures(connection, output_directory)
    connection.close()

    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    # Extract and generate SQL files for the entire schema
    extract_schema(connection, output_directory + r'\\Tables')

    connection.close()
    print("Extraction complete!")

except psycopg2.Error as e:
    print("Error connecting to the PostgreSQL database:", e)
