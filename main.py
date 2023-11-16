import os
import pandas as pd
from google.cloud import storage
def convert_excel_to_csv(request, context):
    source_bucket_name = "destination_for_fun"
    destination_bucket_name = "destination_for_fun_1"
    archive_bucket_name="destination_for_fun_2"
    service_account_key_file = "service account key.json"
    client = storage.Client.from_service_account_json(service_account_key_file)
     # Get the source and destination buckets
    source_bucket = client.get_bucket(source_bucket_name)
    destination_bucket = client.get_bucket(destination_bucket_name)
    archive_bucket= client.get_bucket(archive_bucket_name)
    # Iterate through all new files in the source bucket
    
    for blob in source_bucket.list_blobs():
        print(blob.name)

        if blob.name.endswith(".xlsx"):

            # Read the Excel file into a pandas DataFrame

            df = pd.read_excel(blob.download_as_bytes(), sheet_name=None)
            # Iterate through each sheet in the Excel file

            for sheet_name, sheet_df in df.items():
                csv_file_name = f"{os.path.splitext(blob.name)[0]}_{sheet_name}.csv"
                # Convert the sheet DataFrame to a CSV file

                csv_data = sheet_df.to_csv(index=False)
                # Upload the CSV file to the destination bucket
                destination_bucket.blob(csv_file_name).upload_from_string(csv_data)
        else:
            new_blob = source_bucket.blob(blob.name)
            destination_bucket.copy_blob(new_blob, destination_bucket, blob.name)
    archive_bucket.copy_blob(new_blob, archive_bucket, blob.name)
    blob.delete()
