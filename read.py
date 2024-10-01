import firebase_admin
from firebase_admin import credentials, db
import json
import csv

# Initialize Firebase
cred = credentials.Certificate(r'C:\Users\ES\OneDrive\Desktop\ADAS\log-object-detection-firebase-adminsdk-svcpu-2a933d9402.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://log-object-detection-default-rtdb.firebaseio.com/'
})

# Function to read the latest entry once from a given child
def get_latest_entry(child_name):
    ref = db.reference(child_name)
    # Query to get the latest entry
    latest_entry = ref.order_by_key().limit_to_last(1).get()
    
    if latest_entry:
        # Extract the UID and data from the result
        for uid, entry in latest_entry.items():
            print(f"Latest Entry in {child_name} (UID: {uid}): {entry}")
            return uid, entry
    else:
        print(f"No data found in {child_name}.")
        return None, None

# Function to store data in CSV in JSON format
def store_data_in_csv(data, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        fieldnames = ['json_data']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write data as JSON string
        writer.writerow({'json_data': json.dumps(data)})

# Function to check the key (WRITE or READ) and handle accordingly
def process_latest_entry(child_name, uid, entry):
    ref = db.reference(f'{child_name}/{uid}')
    
    # Check if "WRITE" exists
    write_data = ref.child("WRITE").get()
    
    if write_data:
        print(f"Found WRITE data for UID: {uid} in {child_name}")
        store_data_in_csv(write_data, f'latest_entry_{child_name}.csv')
        ref.child("WRITE").delete()  # Optionally delete "WRITE" after processing
        ref.child("READ").set(write_data)  # Optionally create "READ" after processing
    else:
        # If "READ" exists or "WRITE" is missing, store "No data found"
        read_data = ref.child("READ").get()
        if read_data:
            print(f"No new WRITE data found for UID: {uid} in {child_name}. Already marked as READ.")
        else:
            print(f"No WRITE or READ data found for UID: {uid} in {child_name}.")
        store_data_in_csv("No data found", f'latest_entry_{child_name}.csv')

# Get the latest entry from "CAMERA ON"
uid_camera, latest_entry_camera = get_latest_entry('CAMERA ON')

# Process the latest entry if there's any data for "CAMERA ON"
if uid_camera and latest_entry_camera:
    process_latest_entry('CAMERA ON', uid_camera, latest_entry_camera)

# Get the latest entry from "MOBILE ON"
uid_mobile, latest_entry_mobile = get_latest_entry('MOBILE ON')

# Process the latest entry if there's any data for "MOBILE ON"
if uid_mobile and latest_entry_mobile:
    process_latest_entry('MOBILE ON', uid_mobile, latest_entry_mobile)
