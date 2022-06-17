# importing python package
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

def authenticate():
    cred = credentials.Certificate("/Users/christoph/Repos/Letss/creds/letss-11cc7-firebase-adminsdk-q3zuh-a094e8d41f.json")
    firebase_admin.initialize_app(cred)

def map_location(location):
    if location=='Amsterdam':
        return {u'administrativeArea': u'Noord-Holland',
                u'country': u'Netherlands',
                u'isoCountryCode': u'NL',
                u'locality': u'Amsterdam',
                u'subAdministrativeArea': u'Amsterdam',
                u'subLocality': u'Amsterdam-Centrum'}
    else:
        return None

def parse_categories(categories):
    tmp = [x.strip() for x in categories[1:-1].split(',')]
    print(tmp)
    return tmp
    
if __name__ == '__main__':
    authenticate()

    # read contents of csv file
    df = pd.read_csv("templates.csv", sep=";", header=0)

    templates = firestore.client().collection(u'templates')


    for i, row in df.iterrows():
        template = templates.document(row['id'])
        template.set({
            u'name': row["name"].strip(),
            u'description': row['description'].strip(),
            u'categories': parse_categories(row['categories']),
            u'location': map_location(row['location']),
            u'status': row['status'].strip(),
            u'sponsored': row['sponsored'],
            u'timestamp': row['timestamp'].strip(),
        })
