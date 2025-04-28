import dlt
import pymssql
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import sqlalchemy

@dlt.source
def azure_sql_server():
    """
    Extracts employee data from Azure SQL Server.
    """
    try:
        conn = pymssql.connect(
            server='azure_sql_server',
            user='username',
            password='password',
            database='database'
        )
        
        query = "SELECT * FROM Employee"
        df_employee = pd.read_sql(query, conn)
        
        conn.close()
        
        return {
            'employee': df_employee
        }
    
    except Exception as e:
        print(f"Error extracting data from Azure SQL Server: {e}")
        return None


@dlt.source
def google_worksheet():
    """
    Extracts training history data from Google Worksheet.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        
        sheet = client.open('TrainingHistory').sheet1
        
        data = sheet.get_all_records()
        
        df_training_history = pd.DataFrame(data)
        
        return {
            'training_history': df_training_history
        }
    
    except Exception as e:
        print(f"Error extracting data from Google Worksheet: {e}")
        return None


@dlt.transform
def transform_data(employee=azure_sql_server.employee, training_history=google_worksheet.training_history):
    """
    Transforms the extracted data.
    """
    try:
        employee['BirthDate'] = pd.to_datetime(employee['BirthDate'])
        training_history['TrainingDate'] = pd.to_datetime(training_history['TrainingDate'])
        
        
        return {
            'transformed_employee': employee,
            'transformed_training_history': training_history
        }
    
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None


@dlt.destination
def postgres_db(transformed_employee=transform_data.transformed_employee, transformed_training_history=transform_data.transformed_training_history):
    """
    Loads transformed data into PostgreSQL.
    """
    try:
        engine = sqlalchemy.create_engine('postgresql://username:password@localhost:5432/database')
        
        transformed_employee.to_sql('dim_employee', engine, if_exists='replace', index=False)
        
        transformed_training_history.to_sql('fact_training_history', engine, if_exists='replace', index=False)
        
        return {
            'status': 'Data loaded successfully'
        }
    
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
        return None


@dlt.pipeline
def etl_pipeline():
    """
    Defines the ETL pipeline.
    """
    return [
        azure_sql_server,
        google_worksheet,
        transform_data,
        postgres_db
    ]


if __name__ == '__main__':
    etl_pipeline.run()