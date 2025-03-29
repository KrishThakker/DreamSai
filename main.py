import pandas as pd
from csv import reader
from os.path import exists
import os
import pendulum
from requests import Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import configparser

# Load configuration from config file
def load_config():
    config = configparser.ConfigParser()
    config_file = 'config.ini'
    
    # Create default config if it doesn't exist
    if not exists(config_file):
        print('\033[1;33;40mConfig file not found. Creating default config.ini\033[0m')
        config['PATHS'] = {
            'working_directory': '/Users/krish/Documents/DreamSai/'
        }
        config['WHATSAPP'] = {
            'api_version': 'v13.0',
            'sender_id': '110575298428506',
            'api_token': 'EAAGJasZAx3KMBAFdR6v3q403r08XyZBvmFg1sHr7DYoxZBsItZAKOJoLjBsuX8mqg1zlZBunamhHeN5A5JiWq9EJWhKekD94m4w6VAATkoBypoRPnj8c0FBMaDC1MoJYNoZAhZBFf1JZA2UX7b2jkr17GpFHGpPoDLAdfVZA3QJsZBAPuDZCxdCI1J9sOIM2wPjGjIe9kDhxrcA0gZDZD'
        }
        config['FILES'] = {
            'excel_file': 'test.xlsx',
            'excluded_files': 'requirements.txt,test.xlsx,main.py,instructions.txt,config.ini'
        }
        
        with open(config_file, 'w') as f:
            config.write(f)
        print('\033[1;32;40mDefault config.ini created. Please review and update as needed, and run the script again.\033[0m')
    
    config.read(config_file)
    return config

# Load configuration
config = load_config()
path = config['PATHS']['working_directory']
excluded_files = config['FILES']['excluded_files'].split(',')

def format_delivery_details(name, number, address):
    return (
        '----------------------------------------\n'
        f'Delivery Details:\n'
        f'  Name:         {name}\n'
        f'  Phone Number: {number}\n'
        f'  Address:      {address}\n'
        '----------------------------------------\n'
    )

def format_driver_letter(driver_name, date, deliveries):
    return (
        f'Dear {driver_name},\n\n'
        'On behalf of the DreamSai team, we sincerely thank you for your invaluable help, thank you for your time and effort.\n'
        'with deliveries this week. Your contribution means a great deal to us and our community.\n\n'
        f'Your Delivery Schedule for Saturday {date}:\n\n'
        '============================================\n\n'
        f'{deliveries}\n'
        '============================================\n\n'
        'Important Notes:\n'
        '- Please confirm receipt of this message\n'
        '- Contact us if you have any questions\n'
        '- Drive safely!\n\n'
        'Best regards,\n'
        'DreamSai Team'
    )

def validate_phone_number(number):
    """Validate and format phone numbers"""
    # Remove any non-digit characters
    clean_number = ''.join(filter(str.isdigit, str(number)))
    
    # Basic UK phone number validation
    if len(clean_number) < 10 or len(clean_number) > 11:
        return None
    
    # Format number to standard format
    if len(clean_number) == 10:
        clean_number = '0' + clean_number
    return clean_number

def process_excel_file(excel_file):
    """Process Excel file with error handling"""
    try:
        print('\033[1;32;40mReading Excel file...\033[0m')
        xl = pd.read_excel(excel_file)
        
        # Validate required columns
        required_columns = ['Name', 'Phone', 'Address', 'Driver', 'Driver Phone']
        missing_columns = [col for col in required_columns if col not in xl.columns]
        if missing_columns:
            print(f'\033[1;31;40mError: Missing required columns: {", ".join(missing_columns)}\033[0m')
            return None
            
        # Convert to CSV with progress indicator
        print('\033[1;32;40mConverting to CSV format...\033[0m')
        xl.to_csv(r'test_csv.csv', index=None, header=True)
        return True
        
    except Exception as e:
        print(f'\033[1;31;40mError processing Excel file: {str(e)}\033[0m')
        return None

def main():
    # Check if the Excel file exists before processing
    excel_file = config['FILES']['excel_file']
    if not exists(excel_file):
        print(f'\033[1;31;40mError: {excel_file} file not found. Please make sure the file exists in the current directory.\033[0m')
        return

    if_final_exists = exists('all.txt')

    if if_final_exists == True:
        os.remove('all.txt')
    text = open('all.txt', 'w')
    
    if_test_exists = exists('test_csv.csv')
    if not if_test_exists:
        if not process_excel_file(excel_file):
            return

    with open('test_csv.csv', 'r') as file:
        csv_reader = reader(file)
        header = next(csv_reader)
        
        if header != None:
            total_lines = sum(1 for _ in file)  # Count total lines
            file.seek(0)  # Reset file pointer
            next(csv_reader)  # Skip header again
            
            print(f'\033[1;32;40mProcessing {total_lines} deliveries...\033[0m')
            
            for line_num, line in enumerate(csv_reader, 1):
                name = line[0]
                raw_number = line[1]
                number = validate_phone_number(raw_number)
                if not number:
                    print(f'\033[1;33;40mWarning: Invalid phone number {raw_number} for {name}\033[0m')
                    continue
                    
                address = line[2]
                person = line[3]
                person_file = (person+'.txt')

                to_write = format_delivery_details(name, number, address)

                with open('all.txt', 'a') as text:
                    text.write(to_write + '\n')

                person_file_exists = exists(person_file)
                if person_file_exists == True:
                    with open(person_file, 'a') as pfile:
                        pfile.write(to_write)
                else:
                    with open(person_file, 'x') as pfile:
                        day = (pendulum.today()).strftime('%A')
                        if day == 'Saturday':
                            date = (pendulum.today()).strftime('%d/%m/%Y')
                        else:
                            date = pendulum.now().next(pendulum.SATURDAY).strftime('%d/%m/%Y')
                        
                        pfile.write(format_driver_letter(person, date, to_write))

                # Show progress
                if line_num % 5 == 0:  # Show progress every 5 items
                    print(f'\033[1;32;40mProcessed {line_num}/{total_lines} deliveries\033[0m')

# Edit Path 
path = '/Users/krish/Documents/DreamSai/'
# Edit Path ^

main()

print('\033[1;32;40mReading Excel File...\n')

dname_list = []
dnumber_list = []
            
with open('test_csv.csv', 'r') as file:
    csv_reader = reader(file)
    header = next(csv_reader)
    if header != None:
        for line in csv_reader:
            dname = line[3]
            raw_dnumber = line[4]
            dnumber = validate_phone_number(raw_dnumber)
            if not dnumber:
                print(f'\033[1;33;40mWarning: Invalid driver phone number {raw_dnumber} for {dname}\033[0m')
                continue
                
            dnumber = '44' + dnumber[1:]  # Convert to international format
            
            if dname not in dname_list:
                dname_list.append(dname)
                dnumber_list.append(dnumber)

for i in dname_list:
    fr = 'whatsapp:'+dnumber_list[dname_list.index(i)]
    dname_file = i+'.txt'
    with open(dname_file, 'r') as df:
        msg = df.read()

    BASE_URL = 'https://graph.facebook.com/'
    API_VERSION = config['WHATSAPP']['api_version'] + '/'
    SENDER = config['WHATSAPP']['sender_id'] + '/'
    ENDPOINT = 'messages'
    URL = BASE_URL + API_VERSION + SENDER + ENDPOINT
    API_TOKEN = config['WHATSAPP']['api_token']
    TO = dnumber_list[dname_list.index(i)]

    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Content-Type': 'application/json'
    }
    parameters = {
        'messaging_product': 'whatsapp',
        'recipient_type': 'individual',
        'to': TO,
        'type': 'text',
        'text': {'body':msg}
    }
    session = Session()
    session.headers.update(headers)
    try:
        response = session.post(URL, json=parameters)
        data = json.loads(response.text)
        print('\033[1;32;40mSending Message to '+i+'...\n')
        
        # Add status check for the API response
        if response.status_code != 200:
            print(f'\033[1;31;40mError sending message to {i}. Status code: {response.status_code}')
            print(f'Response: {data}\033[0m')
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print('\033[1;31;40mConnection Error:\n\n'+str(e)+'\033[0m')
    except Exception as e:
        print(f'\033[1;31;40mUnexpected error sending message to {i}:\n{str(e)}\033[0m')

    
os.remove('test_csv.csv')

# Create a backup directory for the generated files
backup_dir = path + 'backups/' + pendulum.now().format('YYYY-MM-DD_HH-mm-ss')
if not exists(backup_dir):
    os.makedirs(backup_dir)
    print(f'\033[1;32;40mCreating backup directory: {backup_dir}\033[0m')

# Get list of files to be deleted
list = os.listdir(path)
for file in excluded_files:
    if file in list:
        list.remove(file)
if 'backups' in list:
    list.remove('backups')

# Backup files before deletion
for i in list:
    fpath = path + i
    backup_path = backup_dir + '/' + i
    try:
        with open(fpath, 'r') as src_file, open(backup_path, 'w') as backup_file:
            backup_file.write(src_file.read())
        print(f'\033[1;32;40mBacked up: {i}\033[0m')
    except Exception as e:
        print(f'\033[1;31;40mFailed to backup {i}: {str(e)}\033[0m')
    os.remove(fpath)

print('\033[1;32;40mDone!')