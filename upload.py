# importing python package
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

def authenticate():
    cred = credentials.Certificate("/Users/christoph/Repos/Letss/creds/letss-11cc7-firebase-adminsdk-q3zuh-a094e8d41f.json")
    firebase_admin.initialize_app(cred)

def map_location(location):
    if location=='amsterdam':
        return {u'administrativeArea': u'Noord-Holland',
                u'country': u'Netherlands',
                u'isoCountryCode': u'NL',
                u'locality': u'Amsterdam',
                u'subAdministrativeArea': u'Amsterdam',
                u'subLocality': u'Amsterdam-Centrum'}
    elif pd.isna(location):
        return {u'administrativeArea': None,
                u'country': None,
                u'isoCountryCode': None,
                u'locality': None,
                u'subAdministrativeArea': None,
                u'subLocality': None}
    else:
        print("Didn't recognize %s" % location)
        return None

def parse_categories(categories):
    tmp = [x.strip() for x in categories.split(',')]
    return tmp

def validate_df(df):
    columns = ['id', 'name', 'description', 'categories', 'location', 'timestamp', 'status', 'sponsored']
    nonnull_columns = ['id', 'name', 'description', 'categories', 'timestamp', 'status', 'sponsored']
    assert (df.columns == columns).all()
    assert(df[nonnull_columns].isnull().any().any() == False)
    assert(df["id"].is_unique)

def print_df(df):
    print(df.head())

if __name__ == '__main__':
    authenticate()
    dtypes = {'id': 'str', 'name': 'str', 'description': 'str', 'categories': 'str', 'location': 'str', 'timestamp': 'str', 'status': 'str', 'sponsored': 'bool'}
    parse_dates = ['timestamp']
    # read contents of csv file
    df = pd.read_csv("templates.csv", delimiter=';',header=0, dtype=dtypes, parse_dates=parse_dates)

    print_df(df)
    validate_df(df)

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
            u'timestamp': row['timestamp'],
        })

    print("Uploaded %d templates" % len(df))
