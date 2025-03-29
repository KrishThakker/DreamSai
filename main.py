import pandas as pd
from csv import reader
from os.path import exists
import os
import pendulum
from requests import Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


def main():
    # Check if the Excel file exists before processing
    if not exists('test.xlsx'):
        print('\033[1;31;40mError: test.xlsx file not found. Please make sure the file exists in the current directory.\033[0m')
        return

    if_final_exists = exists('all.txt')

    if if_final_exists == True:
        os.remove('all.txt')
    text = open('all.txt', 'w')
    
    if_test_exists = exists('test_csv.csv')
    if if_test_exists == True:
        pass
    else:
        xl = pd.read_excel('test.xlsx')
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

                to_write = ('\nName : '+name+'\nPhone Number : '+number+'\nAddress : '+address+'\n')

                with open('all.txt', 'a') as text:
                    text.write(to_write)

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

                        pfile.write('Hi '+person+',\n\nOn behalf of the DreamSai team, I just wanted to thank you so much for helping with deliveries this week, it means so much and we really appreciate it.')
                        pfile.write('\n\nBelow are your deliveries for this Saturday '+date+' :\n')
                        pfile.write(to_write)

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
    API_VERSION = 'v13.0/'
    SENDER = '110575298428506/'
    # Edit Sender Number ^
    ENDPOINT = 'messages'
    URL = BASE_URL + API_VERSION + SENDER + ENDPOINT
    API_TOKEN = 'EAAGJasZAx3KMBAFdR6v3q403r08XyZBvmFg1sHr7DYoxZBsItZAKOJoLjBsuX8mqg1zlZBunamhHeN5A5JiWq9EJWhKekD94m4w6VAATkoBypoRPnj8c0FBMaDC1MoJYNoZAhZBFf1JZA2UX7b2jkr17GpFHGpPoDLAdfVZA3QJsZBAPuDZCxdCI1J9sOIM2wPjGjIe9kDhxrcA0gZDZD'
    # Edit API Token ^
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

list = os.listdir(path)
list.remove('requirements.txt')
list.remove('test.xlsx')
list.remove('main.py')
list.remove('instructions.txt')

for i in list:
    fpath = path+i
    os.remove(fpath)

print('\033[1;32;40mDone!')