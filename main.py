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
    if if_test_exists == True:
        pass
    else:
        xl = pd.read_excel(excel_file)
        xl.to_csv(r'test_csv.csv', index = None, header = True)

    with open('test_csv.csv', 'r') as file:
        csv_reader = reader(file)
        header = next(csv_reader)
        
        if header != None:
            for line in csv_reader:
                name = line[0]
                number = '0'+str(line[1])
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
            dnumber = '44'+str(line[4])
            if dname in dname_list:
                pass
            else:
                dname_list.append(dname)
            
            if dnumber in dnumber_list:
                pass
            else:
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