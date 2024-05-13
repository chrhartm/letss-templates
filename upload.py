# importing python package
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import requests
import json
import os
import time
import argparse

parser = argparse.ArgumentParser(description="Upload templates and download images",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-s", "--start", default=0, type=int, help="index in csv to start processing from")
parser.add_argument("-n", "--number", default=None, type=int, help="number of elements to process")
parser.add_argument("-u", "--upload", default=False, action=argparse.BooleanOptionalAction, help="flag for if anything should be uploaded")
parser.add_argument("-c", "--colors", default=False, action=argparse.BooleanOptionalAction, help="generate images with different colors")
parser.add_argument("-i", "--image", default=False, action=argparse.BooleanOptionalAction, help="flag for generating and downloading images")
parser.add_argument("-p", "--path", default=None, type=str, help="path to csv file")


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
    elif location == 'eagx utrecht':
        return {u'administrativeArea': None,
                u'country': u'Virtual',
                u'isoCountryCode': u'EAGx Utrecht',
                u'locality': u'EAGx Utrecht',
                u'subAdministrativeArea': None,
                u'subLocality': None}
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
    columns = ['timestamp', 'name', 'description', 'categories', 'location', 'status', 'language', 'sponsored', 'persona', 'ID']
    nonnull_columns = ['timestamp', 'name', 'categories', 'status', 'language']
    assert(df[nonnull_columns].isnull().any().any() == False)

def generate_image(template, id, color):
    functionUrl = "https://europe-west1-letss-11cc7.cloudfunctions.net/activity-promotionImage"
    payload = {"passphrase": "29rdGDPouc7icnspsdf31S",
                "activity": template["name"],
                "persona": template["persona"],
                "id": id,
                "color": color}
    headers = {'content-type': 'application/json'}
    response = requests.post(functionUrl, data=json.dumps(payload), headers=headers)
    return response.json()["url"]["url"]

def download_picture(url, filename):
    img_data = requests.get(url).content

    if not os.path.exists("./images"):
        os.makedirs("./images")

    with open('./images/'+filename+ '.png', 'wb') as handler:
        handler.write(img_data)

    print("Downloaded " + filename)

def print_df(df):
    print(df.head(5))

def clean_df(df):
    df["name"] = df["name"].str.strip()
    df["description"] = df["description"].str.strip()
    if config["upload"]:
        df.drop(df[df["status"] != "QUEUED"].index, inplace=True)
    else:
        df.drop(df[df["status"] == "IDEA"].index, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["status"] = df["status"].str.strip().apply(lambda x: "ACTIVE" if x=="QUEUED" else x)
    df["persona"] = df["persona"].str.strip()
    df["language"] = df["language"].str.strip()
    df["sponsored"] = df["sponsored"].apply(lambda x: x != "No")
    df["ID"] = df["ID"].str.strip()

if __name__ == '__main__':
    args = parser.parse_args()
    config = vars(args)
    print(config)

    authenticate()
#     dtypes = {'id': 'str', 'name': 'str', 'description': 'str', 'categories': 'str', 'location': 'str', 'timestamp': 'str', 'status': 'str', 'sponsored': 'str'}
    parse_dates = ['timestamp']
    # read contents of csv file
    df = pd.read_csv(config["path"], delimiter=';',header=0, dtype=str, parse_dates=parse_dates)

    colors = ["#FF9800", "#ED7014", "#FA8128", "#FC6103", "#DD571C", "#FF5800", "#FF4F00", "#00B9BC", "#00A5A7", "#009093", "#007C7D", "#006769", "#005254"]

    print_df(df)
    clean_df(df)
    validate_df(df)

    print(df.shape)

    templates = firestore.client().collection(u'templates')
    IDindex = df.columns.get_loc("ID")
    end = config["start"]+config["number"]-1 if config["number"] else df.shape[0]-1

    print(config["start"], end)

    for i, row in df.loc[config["start"]:end].iterrows():
        if config["upload"]:
            update_time, response = templates.add({
                u'name': row["name"],
                # Removed description
                u'description': "", # row['description'],
                u'categories': parse_categories(row['categories']),
                u'location': map_location(row['location']),
                u'status': row['status'],
                u'language' : row['language'],
                u'sponsored': row['sponsored'],
                u'timestamp': row['timestamp'],
                u'persona': row['persona']
            })
            fileId = response.id
        else:
            print(row["ID"])
            if row["ID"] == "": 
                fileId = "test"
            else:
                fileId = row["ID"]

        print(i, fileId, row["name"])
        df.iloc[i,IDindex] = fileId

        if config["image"]:
            if config["colors"]:
                cycle_colors = colors
            else:
                cycle_colors = [colors[0]]

            for color in cycle_colors:
                if config['colors']:
                    imageTitle = fileId + '_' + color
                else:
                    imageTitle = fileId

                url = generate_image(row, imageTitle, color)
                time.sleep(1)
                download_picture(url, imageTitle)

    df.to_csv("output.csv", index=False)