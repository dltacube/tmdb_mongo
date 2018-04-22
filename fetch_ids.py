import datetime
import json
import re
from datetime import date, timedelta

import pymongo
import requests
import os
import logging
from helpers import check_rate_limit
from interface import output_summary
from pymongo import MongoClient

logging.basicConfig(filename="info.log", level=logging.DEBUG)

ID_TYPES = {
    'movies': 'movie_ids_MM_DD_YYYY.json.gz',
    'TV Series': 'tv_series_ids_MM_DD_YYYY.json.gz',
    'People': 'person_ids_MM_DD_YYYY.json.gz',
    'Collections': 'collection_ids_MM_DD_YYYY.json.gz',
    'TV Networks': 'tv_network_ids_MM_DD_YYYY.json.gz',
    'Keywords': 'keyword_ids_MM_DD_YYYY.json.gz',
    'Production Companies': 'production_company_ids_MM_DD_YYYY.json.gz'
}
FILE_TYPES = {
    'movie_ids_MM_DD_YYYY.json.gz': 'movies',
    'tv_series_ids_MM_DD_YYYY.json.gz': 'tv_series',
    'person_ids_MM_DD_YYYY.json.gz': 'person',
    'collection_ids_MM_DD_YYYY.json.gz': 'collection',
    'tv_network_ids_MM_DD_YYYY.json.gz': 'tv_network',
    'keyword_ids_MM_DD_YYYY.json.gz': 'keyword',
    'production_company_ids_MM_DD_YYYY.json.gz': 'production_company'
}
TODAY = '{:%m_%d_%Y}'.format(date.today())
YESTERDAY = '{:%m_%d_%Y}'.format(date.today() - timedelta(1))
BASE_URL = 'http://files.tmdb.org/p/exports/'
changed_ids = []
deleted_ids = []


def decompress_files():
    os.system('gunzip *.gz')


def write_to_disk(file, url):
    r = requests.get(url)
    file.write(r.content)


def clear_folder():
    folder_content = os.listdir()
    for file in folder_content:
        os.remove(file)


def download_latest_ids():
    print("downloading latest id's")
    clear_folder()

    # Download the newest ID files
    for id in ID_TYPES:
        filename = ID_TYPES[id].replace('MM_DD_YYYY', YESTERDAY)
        url = BASE_URL + filename

        try:
            with open(filename, 'r+b') as file:
                write_to_disk(file, url)
        except FileNotFoundError:
            with open(filename, 'x+b') as file:
                write_to_disk(file, url)

    decompress_files()


def exists_on_fomo(id, id_cursor):
    return id_cursor.find_one({"id": id})


def write_to_fomo(id: int, id_cursor: pymongo.collection.Collection, source: str):
    # check if it exists first
    record = exists_on_fomo(id, id_cursor)
    if record:
        if source == 'file':
            deleted_ids.append(id)
            id_cursor.delete_one({'id': id})
        if source == 'api':
            return id_cursor.update_one({'id': id}, {'$set': {'updated': datetime.datetime.utcnow()}})
    else:
        id_cursor.insert_one({"id": id, "updated": datetime.datetime.utcnow()})


def parse_filename(filename):
    p = re.compile('[\d]{2}_[\d]{2}_[\d]{4}')
    date_from_file = re.findall(p, filename)[0]
    old_filename = filename.replace(date_from_file, 'MM_DD_YYYY').replace('.json', '.json.gz')
    return FILE_TYPES[old_filename].lower(), date_from_file


def get_missing_records(id_cursor, id_file):
    res = id_cursor.find()
    ids_from_web597 = []
    [ids_from_web597.append(id['id']) for id in res]

    ids_from_file = []
    [ids_from_file.append(json.loads(x)['id']) for x in id_file]
    # Find the difference between id's in the latest file and the ones in the database
    # TODO: This may be unnecessary if we just query the API and use the incoming 404 error to find out which records to delete
    missing_ids = list(set(ids_from_file).symmetric_difference(ids_from_web597))
    return missing_ids


def update_ids_from_file():
    folder_content = os.listdir()
    for file in folder_content:
        collection, date_str = parse_filename(file)
        id_cursor = cursors[collection]
        with open(file, 'r+') as id_file:
            missing_records = get_missing_records(id_cursor, id_file)

        # n = int(subprocess.check_output(['cat {0} | wc -l'.format(file)], shell=True))

        n = len(missing_records)
        if n > 0:
            for line, record_id in enumerate(missing_records):
                output_summary(n, collection, line)
                write_to_fomo(int(record_id), id_cursor, 'file')
                line += 1
            db.log.insert_one({collection + '_last_update': datetime.datetime.utcnow()})
        else:
            print('No new records to add.')


changelists = {
    'movie': 'movies',
    'tv': 'tv_series',
    'person': 'person'
}


def print_total_changed_ids(start_date, end_date, page, collection):
    c.execute('SELECT * FROM ids.movies WHERE updated > inserted')
    result = c.fetchall()
    print('start: {}, end:{}, page:{}\ttotal:{}\tcollection:{}'.format(start_date, end_date, page, len(result),
                                                                       collection))


def update_ids_from_api(start_date, collection, page=1):
    # Create a 14 day interval for tmdb updates
    end_date = min(start_date + timedelta(14), date.today())
    print_total_changed_ids(start_date, end_date, page, collection)
    data = {'api_key': '124161d628c0cdc6e6875c5210c54689',
            'start_date': start_date,
            'end_date': end_date,
            'page': page}
    r = requests.get('https://api.themoviedb.org/3/{}/changes'.format(collection), params=data)
    check_rate_limit(r)
    rjson = r.json()
    if r.status_code == 200:
        for result in rjson['results']:
            id = result['id']
            changed_ids.append(id)
            write_to_fomo(id, cursors[collection], 'api')
    if rjson['page'] < rjson['total_pages']:
        next_page = rjson['page'] + 1
        update_ids_from_api(start_date=start_date, collection=collection, page=next_page)
    if rjson['page'] == 1:
        if end_date < date.today():
            next_date = end_date + timedelta(1)
            update_ids_from_api(start_date=next_date, collection=collection)


if __name__ == '__main__':
    try:
        os.chdir('ids')
    except FileNotFoundError:
        os.mkdir('ids')
        os.chdir('ids')

    client = MongoClient('mongodb://localhost:27017')
    db = client['ids']
    cursors = {'movies': db.movies, 'tv_series': db.tv, 'person': db.person, 'collection': db.collection,
               'tv_network': db.tv_network, 'keyword': db.keyword, 'production_company': db.production_company}
    # download_latest_ids()
    update_ids_from_file()
    for changelist in changelists:
        update_ids_from_api(start_date=date(2018, 2, 24), collection=changelist, page=1)
