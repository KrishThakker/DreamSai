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

def validate_excel_data(data):
    """Validate Excel data structure and content"""
    errors = []
    warnings = []
    
    # Check for empty rows
    empty_rows = data.index[data.isnull().all(1)].tolist()
    if empty_rows:
        warnings.append(f"Found {len(empty_rows)} empty rows that will be skipped")
    
    # Check for required fields
    for idx, row in data.iterrows():
        row_num = idx + 2  # Adding 2 to account for 0-based index and header row
        if pd.isnull(row['Name']):
            errors.append(f"Row {row_num}: Missing recipient name")
        if pd.isnull(row['Address']):
            errors.append(f"Row {row_num}: Missing address")
        if pd.isnull(row['Driver']):
            errors.append(f"Row {row_num}: Missing driver name")
    
    # Print validation results
    if warnings:
        print('\033[1;33;40m' + '\n'.join(warnings) + '\033[0m')
    if errors:
        print('\033[1;31;40mValidation Errors:\n' + '\n'.join(errors) + '\033[0m')
        return False
    
    return True

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
        
        # Validate data content
        if not validate_excel_data(xl):
            return None
            
        # Convert to CSV with progress indicator
        print('\033[1;32;40mConverting to CSV format...\033[0m')
        xl.to_csv(r'test_csv.csv', index=None, header=True)
        return True
        
    except Exception as e:
        print(f'\033[1;31;40mError processing Excel file: {str(e)}\033[0m')
        return None

def generate_summary_report(total_deliveries, processed_deliveries, invalid_numbers, duplicate_deliveries):
    """Generate a summary report of the processing"""
    return (
        '\n============= Processing Summary =============\n'
        f'Total entries processed:     {total_deliveries}\n'
        f'Successful deliveries:       {processed_deliveries}\n'
        f'Invalid phone numbers:       {len(invalid_numbers)}\n'
        f'Duplicate deliveries:        {len(duplicate_deliveries)}\n'
        '-------------------------------------------\n'
        f'Success rate:                {(processed_deliveries/total_deliveries)*100:.1f}%\n'
        '\nDetails:\n'
        + ('\nInvalid phone numbers:\n' + '\n'.join(f'- {name}: {number}' for name, number in invalid_numbers) if invalid_numbers else '')
        + ('\nDuplicate deliveries:\n' + '\n'.join(f'- {name} ({count} times)' for name, count in duplicate_deliveries.items()) if duplicate_deliveries else '')
        + '\n===========================================\n'
    )

def send_whatsapp_messages(dname_list, dnumber_list, config):
    """Send WhatsApp messages to drivers"""
    success_count = 0
    failed_messages = []
    
    for i in dname_list:
        try:
            dname_file = i + '.txt'
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
                'text': {'body': msg}
            }
            
            session = Session()
            session.headers.update(headers)
            
            print(f'\033[1;32;40mSending message to {i}...\033[0m')
            response = session.post(URL, json=parameters)
            data = json.loads(response.text)
            
            if response.status_code == 200:
                success_count += 1
                print(f'\033[1;32;40mMessage sent successfully to {i}\033[0m')
            else:
                failed_messages.append((i, response.status_code, data))
                print(f'\033[1;31;40mFailed to send message to {i}. Status code: {response.status_code}\033[0m')
                
        except Exception as e:
            failed_messages.append((i, 'Exception', str(e)))
            print(f'\033[1;31;40mError sending message to {i}: {str(e)}\033[0m')
    
    # Print messaging summary
    print(f'\n\033[1;32;40mMessaging Summary:\n'
          f'Successfully sent: {success_count}/{len(dname_list)} messages\n'
          f'Failed messages: {len(failed_messages)}\033[0m')
    
    if failed_messages:
        print('\n\033[1;31;40mFailed Messages Details:')
        for name, code, error in failed_messages:
            print(f'- {name}: {code} - {error}\033[0m')
    
    return success_count == len(dname_list)

def process_driver_list(csv_file):
    """Process the CSV file to extract driver information"""
    dname_list = []
    dnumber_list = []
    
    print('\033[1;32;40mProcessing driver information...\033[0m')
    
    try:
        with open(csv_file, 'r') as file:
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
        
        print(f'\033[1;32;40mFound {len(dname_list)} unique drivers\033[0m')
        return dname_list, dnumber_list
        
    except Exception as e:
        print(f'\033[1;31;40mError processing driver information: {str(e)}\033[0m')
        return [], []

def cleanup_files(path, backup_dir, excluded_files):
    """Clean up generated files and create backups"""
    try:
        # Remove CSV file
        if exists('test_csv.csv'):
            os.remove('test_csv.csv')
        
        # Create backup directory
        if not exists(backup_dir):
            os.makedirs(backup_dir)
            print(f'\033[1;32;40mCreating backup directory: {backup_dir}\033[0m')

        # Get list of files to be deleted
        files_to_process = [f for f in os.listdir(path) 
                          if f not in excluded_files and f != 'backups']

        # Backup and delete files
        for filename in files_to_process:
            fpath = path + filename
            backup_path = backup_dir + '/' + filename
            try:
                with open(fpath, 'r') as src_file, open(backup_path, 'w') as backup_file:
                    backup_file.write(src_file.read())
                print(f'\033[1;32;40mBacked up: {filename}\033[0m')
                os.remove(fpath)
            except Exception as e:
                print(f'\033[1;31;40mFailed to process {filename}: {str(e)}\033[0m')

        return True
    except Exception as e:
        print(f'\033[1;31;40mError during cleanup: {str(e)}\033[0m')
        return False

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
            total_lines = sum(1 for _ in file)
            file.seek(0)
            next(csv_reader)
            
            print(f'\033[1;32;40mProcessing {total_lines} deliveries...\033[0m')
            
            # Track statistics
            processed_deliveries = 0
            invalid_numbers = []
            duplicate_deliveries = {}
            processed_addresses = set()
            
            for line_num, line in enumerate(csv_reader, 1):
                name = line[0]
                raw_number = line[1]
                address = line[2]
                
                # Check for duplicate deliveries
                delivery_key = f"{name.lower()}:{address.lower()}"
                if delivery_key in processed_addresses:
                    duplicate_deliveries[name] = duplicate_deliveries.get(name, 1) + 1
                    print(f'\033[1;33;40mWarning: Duplicate delivery found for {name} at {address}\033[0m')
                    continue
                
                processed_addresses.add(delivery_key)
                
                number = validate_phone_number(raw_number)
                if not number:
                    invalid_numbers.append((name, raw_number))
                    print(f'\033[1;33;40mWarning: Invalid phone number {raw_number} for {name}\033[0m')
                    continue
                
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

                processed_deliveries += 1
                
                # Show progress
                if line_num % 5 == 0:
                    print(f'\033[1;32;40mProcessed {line_num}/{total_lines} deliveries\033[0m')
            
            # Print summary report
            summary = generate_summary_report(total_lines, processed_deliveries, invalid_numbers, duplicate_deliveries)
            print('\033[1;32;40m' + summary + '\033[0m')
            
            # Save summary to file
            with open('processing_summary.txt', 'w') as summary_file:
                summary_file.write(summary)
            print('\033[1;32;40mSummary saved to processing_summary.txt\033[0m')

    # Process driver information
    dname_list, dnumber_list = process_driver_list('test_csv.csv')

    # Send WhatsApp messages if there are drivers
    if dname_list:
        send_whatsapp_messages(dname_list, dnumber_list, config)
    else:
        print('\033[1;33;40mNo drivers to message\033[0m')

    # Cleanup and backup files
    backup_dir = path + 'backups/' + pendulum.now().format('YYYY-MM-DD_HH-mm-ss')
    if cleanup_files(path, backup_dir, excluded_files):
        print('\033[1;32;40mCleanup completed successfully\033[0m')
    else:
        print('\033[1;31;40mCleanup encountered some errors\033[0m')

    print('\033[1;32;40mDone!\033[0m')

# Remove the duplicate code after main()
if __name__ == "__main__":
    main()