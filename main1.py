from google.cloud import bigquery
from google.oauth2 import service_account
import json
from tabulate import tabulate
import pandas as pd
import matplotlib.pyplot as plt
import re

# Load your GCP credentials from the JSON file
#C:\Users\91939\hello2\eloquent-drive-413114-b059730a68b8.json 
#credentials = service_account.Credentials.from_service_account_file('service-account.json')
service_account_info = json.load(open('eloquent-drive-413114-b059730a68b8.json'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id ='eloquent-drive-413114'
client = bigquery.Client(credentials= credentials,project=project_id)
# Set your dataset name

dataset_name = 'inflation_dataset'
def select_data():
    query = f"SELECT * FROM {project_id}.{dataset_name}.inflationranks"
    try:
        result = client.query(query).result()
        data_tuples = [(row['Countries'], row['Inflation2022'], row['Global_rank'], row['Available_data']) for row in result]
        return data_tuples
    except Exception as e:
        print(f"Error selecting data: {e}")
        return []




def insert_data(Countries, Inflation2022, Global_rank, Available_data):
    # Check if 'Countries' contains only alphabetic characters
    if not re.match("^[a-zA-Z]+$", Countries):
        print("Invalid input for 'Countries'. Please enter alphabetic characters only.")
        return

    # Check if 'Available_data' matches the specified format XXXX-XXXX
    if not re.match("^\d{4}-\d{4}$", Available_data):
        print("Invalid input for 'Available_data'. Please enter the format XXXX-XXXX where X is a numeric character.")
        return

    query = f"""
        INSERT INTO {project_id}.{dataset_name}.inflationranks(Countries,Inflation2022,Global_rank,Available_data)
        VALUES ('{Countries}', {Inflation2022}, {Global_rank}, '{Available_data}')
    """
    client.query(query)
    print("Data inserted successfully.")



def update_data(Countries, new_inflation):
    query = f"""
        UPDATE {project_id}.{dataset_name}.inflationranks
        SET Inflation2022 = @new_inflation
        WHERE Countries = @countries
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_inflation", "FLOAT", new_inflation),
            bigquery.ScalarQueryParameter("countries", "STRING", Countries),
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        print(f"Query executed successfully: {query_job.query}")
        print(f"Rows affected: {query_job.num_dml_affected_rows}")
        print("Data updated successfully.")
    except Exception as e:
        print(f"Error updating data: {e}")



def delete_data(Countries):
    query = f"""
        DELETE FROM {project_id}.{dataset_name}.inflationranks
        WHERE Countries = '{Countries}'
    """
    client.query(query)


def visualize_data(data):
    countries = [row[0] for row in data]
    inflation = [row[1] for row in data]

    plt.bar(countries, inflation)
    plt.xlabel('Countries')
    plt.ylabel('Inflation2022')
    plt.title('Inflation2022 Across Countries')
    plt.show()

def main():
    role = input("Enter your role (1 for Administrator, 2 for User): ")
    while True:
        print("\nOperations:")
        print("1. Insert Data")
        print("2. Update Data")
        print("3. Delete Data")
        print("4. Display Data")
        print("5. Visualize Data")
        print("6. Exit")
        
        operation = input("Enter the operation number you want to perform: ")

        if operation == '1' and role=='1':  # Insert Data
            Countries = input("Enter countries: ")
            Inflation2022 = float(input("Enter inflation: "))
            Global_rank = int(input("Enter global rank: "))
            Available_data = input("Enter available data: ")
            insert_data(Countries,Inflation2022,Global_rank,Available_data)

        elif operation == '2' and role=='1':  # Update Data
            Countries = input("Enter countries to update: ")
            new_inflation = float(input("Enter new inflation: "))
            update_data(Countries, new_inflation)
      

        elif operation == '3' and role=='1':  # Delete Data
            Countries = input("Enter countries to delete: ")
            delete_data(Countries)
            print("Data deleted successfully.")

        elif operation == '4':  # Display Data
            data = select_data()
            print("Table:")
            print(tabulate(data, headers=["Countries", "Inflation2022", "Global_rank","Available_data"  ]))

        elif operation == '5':  # Visualize Data
            data = select_data()
            visualize_data(data)

        elif operation == '6':  # Exit
            break

        else:
            print("Invalid operation. Please try again.")


if __name__ == "__main__":
    main()