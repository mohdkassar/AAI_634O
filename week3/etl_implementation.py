import pandas as pd
from pymongo import MongoClient

def extract_patient_data(csv_file='patients.csv'):
    patients_df = pd.read_csv(csv_file)
    print("Extracted Patient Data:")
    print(patients_df)
    print("\n")
    return patients_df

def extract_diagnostic_data(json_file='./diagnostics.json'):
    diagnostic_df = pd.read_json(json_file)
    
    print("Extracted Diagnostic Data:")
    print(diagnostic_df)
    print("\n")
    return diagnostic_df

def transform_patient_data(patients_df, min_age_threshold=40):
    filtered_patients = patients_df[patients_df['age'] >= min_age_threshold]
    
    print(f"Filtered patients: {len(filtered_patients)} patient documents")
    print("\n")
    return filtered_patients

def transform_diagnostic_data(diagnostic_df, patients_df):
    transformed_diagnostics = pd.merge(
        diagnostic_df,
        patients_df[['patient_id', 'name', 'age', 'gender']],
        on='patient_id',
        how='inner'
    )
    
    print("Transformed Diagnostics Documents:")
    print(transformed_diagnostics)
    print("\n")
    return transformed_diagnostics

def load_patient_data(db, patients_df):
    patients_collection = db.patients
    
    patient_records = patients_df.to_dict('records')
    
    patients_collection.delete_many({})
    
    result = patients_collection.insert_many(patient_records)
    print(f"Inserted {len(result.inserted_ids)} patient records into MongoDB")
    print("\n")

def load_diagnostic_data(db, diagnostic_df):
    diagnostics_collection = db.diagnostics
    
    diagnostic_records = diagnostic_df.to_dict('records')
    
    diagnostics_collection.delete_many({})
    
    result = diagnostics_collection.insert_many(diagnostic_records)
    print(f"Inserted {len(result.inserted_ids)} diagnostic records into MongoDB")
    print("\n")
    return True

def run_etl_pipeline(csv_file='./patients.csv', 
                     min_age_threshold=40,
                     connection_string='mongodb://localhost:27017/',
                     db_name='healthcare_db'):
    
    print("Starting ETL Pipeline")
    print("\n")
    
    # Extraction
    patients_df = extract_patient_data(csv_file)
    diagnostic_df = extract_diagnostic_data()

   # Transformation 
    transformed_patients = transform_patient_data(patients_df, min_age_threshold)
    enriched_diagnostics = transform_diagnostic_data(diagnostic_df, transformed_patients)
    
    client = MongoClient(connection_string)
    db = client[db_name]

    # Loading    
    load_patient_data(db, transformed_patients)
    load_diagnostic_data(db, enriched_diagnostics)
    
    print("ETL Pipeline Completed")
    print("\n")

if __name__ == "__main__":
    run_etl_pipeline(
        csv_file='patients.csv',
        min_age_threshold=40,
        connection_string='mongodb://localhost:27017/',
        db_name='healthcare_db'
    )