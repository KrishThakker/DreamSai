import pandas as pd
from csv import reader
from os.path import exists
import os
import pendulum
from requests import Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import configparser
import logging
from datetime import datetime
from time import sleep
from typing import List, Tuple, Dict

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
        'On behalf of the DreamSai team, we sincerely thank you for your invaluable help, thank you for your time and effort. \n'
        'We are grateful for your dedication and commitment to our cause. Your efforts are truly appreciated and make a significant impact on our community.\n\n'
        f'Your Delivery Schedule for Saturday {date}:\n\n'
        '============================================\n\n'
        f'{deliveries}\n'
        '============================================\n\n'
        'Important Notes:\n'
        '- Please confirm receipt of this message by replying to this message\n'
        '- Contact us if you have any questions or need assistance\n'
        '- Drive safely!\n\n'
        'Best regards,\n'
        'Krish, from the DreamSai team'
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
    """Validate Excel data structure and content with logging"""
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

# Add logging configuration
def setup_logging():
    """Configure logging for the application"""
    log_file = f'logs/dreamsai_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return log_file

class WhatsAppMessenger:
    """Handle WhatsApp message sending with retries"""
    def __init__(self, config: dict, max_retries: int = 3, retry_delay: int = 5):
        self.config = config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.success_count = 0
        self.failed_messages = []
        
        self.base_url = 'https://graph.facebook.com/'
        self.api_version = config['WHATSAPP']['api_version'] + '/'
        self.sender = config['WHATSAPP']['sender_id'] + '/'
        self.endpoint = 'messages'
        self.url = self.base_url + self.api_version + self.sender + self.endpoint
        self.api_token = config['WHATSAPP']['api_token']
        
        self.session = Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json'
        })

    def send_message(self, driver_name: str, phone_number: str, message: str) -> bool:
        """Send a WhatsApp message with retries"""
        for attempt in range(self.max_retries):
            try:
                logging.info(f"Sending message to {driver_name} (Attempt {attempt + 1}/{self.max_retries})")
                
                parameters = {
                    'messaging_product': 'whatsapp',
                    'recipient_type': 'individual',
                    'to': phone_number,
                    'type': 'text',
                    'text': {'body': message}
                }
                
                response = self.session.post(self.url, json=parameters)
                data = json.loads(response.text)
                
                if response.status_code == 200:
                    self.success_count += 1
                    logging.info(f"Successfully sent message to {driver_name}")
                    return True
                    
                logging.warning(f"Failed to send message to {driver_name}. Status: {response.status_code}")
                if attempt < self.max_retries - 1:
                    logging.info(f"Retrying in {self.retry_delay} seconds...")
                    sleep(self.retry_delay)
                    
            except Exception as e:
                logging.error(f"Error sending message to {driver_name}: {str(e)}")
                if attempt < self.max_retries - 1:
                    sleep(self.retry_delay)
        
        self.failed_messages.append((driver_name, phone_number, "Max retries exceeded"))
        return False

    def get_results(self) -> Tuple[int, List[Tuple]]:
        """Return the results of message sending"""
        return self.success_count, self.failed_messages

def retry_failed_messages(messenger: WhatsAppMessenger, failed_messages: List[Tuple]) -> None:
    """Retry sending failed messages"""
    if not failed_messages:
        return
    
    logging.info("Retrying failed messages...")
    retry_count = 0
    
    for driver_name, phone_number, _ in failed_messages:
        try:
            with open(f"{driver_name}.txt", 'r') as f:
                message = f.read()
            
            if messenger.send_message(driver_name, phone_number, message):
                retry_count += 1
                
        except Exception as e:
            logging.error(f"Error retrying message for {driver_name}: {str(e)}")
    
    logging.info(f"Successfully resent {retry_count}/{len(failed_messages)} failed messages")

# Update the send_whatsapp_messages function
def send_whatsapp_messages(dname_list: List[str], dnumber_list: List[str], config: dict) -> bool:
    """Send WhatsApp messages to drivers using the new messenger class"""
    messenger = WhatsAppMessenger(config)
    
    for name, number in zip(dname_list, dnumber_list):
        try:
            with open(f"{name}.txt", 'r') as f:
                message = f.read()
            messenger.send_message(name, number, message)
        except Exception as e:
            logging.error(f"Error processing message for {name}: {str(e)}")
    
    success_count, failed_messages = messenger.get_results()
    
    # Print messaging summary
    logging.info(f"\nMessaging Summary:\n"
                f"Successfully sent: {success_count}/{len(dname_list)} messages\n"
                f"Failed messages: {len(failed_messages)}")
    
    if failed_messages:
        logging.warning("\nFailed Messages Details:")
        for name, number, error in failed_messages:
            logging.warning(f"- {name}: {error}")
        
        # Attempt to retry failed messages
        retry_failed_messages(messenger, failed_messages)
    
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

def get_delivery_date(date_str: str = None) -> str:
    """Get the delivery date, either from input or next Saturday"""
    if date_str:
        try:
            date = pendulum.parse(date_str)
            # Ensure the date is a Saturday
            if date.day_of_week != pendulum.SATURDAY:
                date = date.next(pendulum.SATURDAY)
        except Exception:
            logging.warning(f"Invalid date format: {date_str}. Using next Saturday.")
            date = pendulum.now().next(pendulum.SATURDAY)
    else:
        date = pendulum.now()
        if date.day_of_week == pendulum.SATURDAY:
            if date.hour >= 12:  # After noon, use next Saturday
                date = date.next(pendulum.SATURDAY)
        else:
            date = date.next(pendulum.SATURDAY)
    
    return date.strftime('%d/%m/%Y')

class DeliveryManager:
    """Manage deliveries and their organization"""
    def __init__(self, config: dict):
        self.config = config
        self.deliveries_by_date = {}  # Dictionary to store deliveries by date
        self.drivers_by_date = {}     # Dictionary to store drivers by date
        self.processed_deliveries = 0
        self.invalid_numbers = []
        self.duplicate_deliveries = {}
        self.processed_addresses = set()

    def add_delivery(self, name: str, number: str, address: str, driver: str, date: str = None):
        """Add a delivery to the manager"""
        delivery_date = get_delivery_date(date)
        
        # Initialize date entries if they don't exist
        if delivery_date not in self.deliveries_by_date:
            self.deliveries_by_date[delivery_date] = []
        if delivery_date not in self.drivers_by_date:
            self.drivers_by_date[delivery_date] = {}

        # Check for duplicate deliveries
        delivery_key = f"{name.lower()}:{address.lower()}:{delivery_date}"
        if delivery_key in self.processed_addresses:
            self.duplicate_deliveries[name] = self.duplicate_deliveries.get(name, 1) + 1
            logging.warning(f"Duplicate delivery found for {name} at {address} for {delivery_date}")
            return False

        self.processed_addresses.add(delivery_key)
        
        # Validate phone number
        clean_number = validate_phone_number(number)
        if not clean_number:
            self.invalid_numbers.append((name, number))
            logging.warning(f"Invalid phone number {number} for {name}")
            return False

        # Add delivery to the appropriate date
        delivery = {
            'name': name,
            'number': clean_number,
            'address': address,
            'driver': driver
        }
        self.deliveries_by_date[delivery_date].append(delivery)
        
        # Update driver information
        if driver not in self.drivers_by_date[delivery_date]:
            self.drivers_by_date[delivery_date][driver] = []
        self.drivers_by_date[delivery_date][driver].append(delivery)
        
        self.processed_deliveries += 1
        return True

    def generate_driver_files(self):
        """Generate individual driver files for each date"""
        for date, drivers in self.drivers_by_date.items():
            for driver, deliveries in drivers.items():
                filename = f"{driver}_{date.replace('/', '-')}.txt"
                
                # Create the driver's delivery text
                delivery_text = ""
                for delivery in deliveries:
                    delivery_text += format_delivery_details(
                        delivery['name'],
                        delivery['number'],
                        delivery['address']
                    )
                
                # Write the complete letter
                with open(filename, 'w') as f:
                    f.write(format_driver_letter(driver, date, delivery_text))
                logging.info(f"Generated delivery file for {driver} for {date}")

    def generate_summary(self) -> str:
        """Generate a detailed summary including date-specific information"""
        summary = "\n============= Processing Summary =============\n"
        summary += f"Total entries processed:     {self.processed_deliveries}\n"
        summary += f"Invalid phone numbers:       {len(self.invalid_numbers)}\n"
        summary += f"Duplicate deliveries:        {len(self.duplicate_deliveries)}\n\n"
        
        for date in sorted(self.deliveries_by_date.keys()):
            deliveries = self.deliveries_by_date[date]
            drivers = self.drivers_by_date[date]
            summary += f"\nDate: {date}\n"
            summary += f"  Total deliveries: {len(deliveries)}\n"
            summary += f"  Number of drivers: {len(drivers)}\n"
            summary += "  Drivers and their delivery counts:\n"
            for driver, deliveries in drivers.items():
                summary += f"    - {driver}: {len(deliveries)} deliveries\n"
        
        return summary + "\n=========================================\n"

# Update the main function to use DeliveryManager
def main():
    log_file = setup_logging()
    logging.info("Starting DreamSai delivery processing")
    
    try:
        # Initialize delivery manager
        delivery_manager = DeliveryManager(config)
        
        # Process Excel file
        if not process_excel_file(config['FILES']['excel_file']):
            return

        # Read and process deliveries
        with open('test_csv.csv', 'r') as file:
            csv_reader = reader(file)
            header = next(csv_reader)
            
            if header != None:
                total_lines = sum(1 for _ in file)
                file.seek(0)
                next(csv_reader)
                
                logging.info(f"Processing {total_lines} deliveries...")
                
                for line_num, line in enumerate(csv_reader, 1):
                    delivery_manager.add_delivery(
                        name=line[0],
                        number=line[1],
                        address=line[2],
                        driver=line[3],
                        date=line[5] if len(line) > 5 else None  # Optional date column
                    )
                    
                    if line_num % 5 == 0:
                        logging.info(f"Processed {line_num}/{total_lines} deliveries")

        # Generate driver files
        delivery_manager.generate_driver_files()
        
        # Generate and save summary
        summary = delivery_manager.generate_summary()
        logging.info(summary)
        with open('processing_summary.txt', 'w') as f:
            f.write(summary)

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
        logging.info("Processing completed successfully")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise
    finally:
        logging.info(f"Log file saved to: {log_file}")

# Remove the duplicate code after main()
if __name__ == "__main__":
    main()