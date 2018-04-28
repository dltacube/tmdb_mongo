import datetime
import unittest
import os
import json
from io import StringIO
from typing import List

import pymongo
from pymongo import MongoClient
from fetch_ids import exists_on_fomo, write_to_fomo, get_missing_records
from populate_db import id_needs_insertion

client = MongoClient('mongodb://localhost:27017')
db = client['ids']

if os.getcwd().split('/')[-1] != 'tests':
    os.chdir('tests')


def set_inserted_column(c: pymongo.collection.Collection, ids: List[int], time: datetime.datetime):
    c.update_many(
        {'id': {'$in': ids}},
        {'$set': {
            'inserted': time
        }
        })


def getEarliestUpdateTime(c: pymongo.collection.Collection) -> datetime.datetime:
    result = c.aggregate([{'$group': {'_id': 'all', 'firstInsert': {'$min': '$updated'}}}]).next()
    print(result)
    return result['firstInsert']


class TestWriteFomFile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.id_cursor = db['movies_test']
        cls.files = os.listdir('../ids/')
        cls.file = open('../ids/' + cls.files[0])
        cls.lines = cls.file.readlines()[:10]
        # extra lines we won't write to db
        # TODO: don't seek back to the beginning. That's stupid =\
        cls.file.seek(0)
        cls.extralines = cls.file.readlines()[10:20]
        cls.ids_unwritten = []
        [cls.ids_unwritten.append(json.loads(id)) for id in cls.extralines]

        cls.ids, cls.idnum = [], []
        # turn ids into a list of dict
        [cls.ids.append(json.loads(id)) for id in cls.lines]
        [cls.idnum.append(id['id']) for id in cls.ids]

    def setUp(self):
        for record in self.idnum:
            write_to_fomo(record, self.id_cursor, 'file')

    def test_exists(self):
        for record in self.ids:
            with self.subTest(id=record['id']):
                result = exists_on_fomo(record['id'], self.id_cursor)
                self.assertIsNotNone(result, msg="record id: {} was not found".format(record['id']))
    # We give it all the records and it finds what's diff on server. That'll be all the unwritten lines!
    def test_find_missing_records(self):
        unwritten = []
        [unwritten.append(record['id']) for record in self.ids_unwritten]
        unwritten.sort()

        file = StringIO()
        for line in self.ids + self.ids_unwritten:
            json.dump(line, file)
            file.write('\n')
        file.seek(0)

        missing = get_missing_records(self.id_cursor, file)
        missing.sort()

        self.assertEqual(missing, unwritten)
    # first thing to note about write_to_fomo
    # any id coming through here exists either on the db or in the file but not both

    # occurs from scanning id files
    def test_write_from_file_exists(self):
        record = self.idnum[0]
        # Since this record exists on the database but not the file, it gets deleted
        write_to_fomo(record, self.id_cursor, 'file')
        self.assertIsNone(exists_on_fomo(record, self.id_cursor))

    def test_write_from_file_not_exists(self):
        record = 2222
        self.assertIsNone(exists_on_fomo(record, self.id_cursor))
        write_to_fomo(record, self.id_cursor, 'file')
        self.assertIsNotNone(exists_on_fomo(record, self.id_cursor))

    # occurs from scanning API changes
    def test_write_from_api_exists(self):
        record = self.idnum[0]
        # here we expect the updated column to change
        result = write_to_fomo(record, self.id_cursor, 'api')
        self.assertTrue(result.acknowledged)

    def test_compare_new_old_record(self):
        # the difference here is that our record now has the updated column
        record = self.idnum[0]
        write_to_fomo(record, self.id_cursor, 'api')
        old_record = self.id_cursor.find_one({'id': record})
        write_to_fomo(record, self.id_cursor, 'api')
        new_record = self.id_cursor.find_one({'id': record})
        self.assertGreater(new_record['updated'], old_record['updated'])

    # No record will have the inserted column. comparison of inserted (null) will be less than updated (anything)
    def test_which_id_needs_updating_all_no_column(self):
        self.assertEqual(id_needs_insertion(self.id_cursor).count(), 10)

    # Test the function responsible for adding a column
    def test_add_column(self):
        set_inserted_column(self.id_cursor, self.idnum, datetime.datetime.utcnow())

        for record in self.id_cursor.find():
            with self.subTest(record=record):
                self.assertIn('inserted', record.keys())
                self.assertIsInstance(record['inserted'], datetime.datetime)

    # This one has the column, but the insertion date is later
    def test_which_id_needs_updating_none_column(self):
        set_inserted_column(self.id_cursor, self.idnum, datetime.datetime.utcnow())
        # [print(rec) for rec in self.id_cursor.find()]
        self.assertEqual(id_needs_insertion(self.id_cursor).count(), 0)

    def test_which_id_needs_updating_all_of_them(self):
        earliest = getEarliestUpdateTime(self.id_cursor)
        earlier = earliest - datetime.timedelta(days=1)
        set_inserted_column(self.id_cursor, self.idnum, earlier)
        self.assertEqual(id_needs_insertion(self.id_cursor).count(), 10)

    def test_which_id_needs_updating_half_of_them(self):
        earliest = getEarliestUpdateTime(self.id_cursor)
        earlier = earliest - datetime.timedelta(days=1)
        set_inserted_column(self.id_cursor, self.idnum[:5], earlier)
        set_inserted_column(self.id_cursor, self.idnum[5:], datetime.datetime.utcnow())
        how_many_need_tobe_inserted = id_needs_insertion(self.id_cursor).count()
        self.assertEqual(how_many_need_tobe_inserted, 5, msg="this many need update: {}".format(how_many_need_tobe_inserted))

    def tearDown(self):
        self.id_cursor.delete_many({})

    @classmethod
    def tearDownClass(cls):
        cls.id_cursor.drop()
        cls.file.close()


if __name__ == '__main__':
    print("inside __main__")
    unittest.main()
