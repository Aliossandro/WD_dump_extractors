# -*- coding: utf-8 -*-
"""
Created on Dec 08 2016

@author: Alessandro
"""

# import os
import sys
# sys.setdefaultencoding("utf8")

import re
# import pandas as pd
# import io
# import ujson
import bz2
import collections
# import datetime
# import itertools
# import psycopg2
import pymongo as pm
# from sqlalchemy import create_engine
# import subprocess
import unicodedata
# import csv
import HTMLParser
from shutil import copyfileobj
import ujson
# import item_examine

# file_name = 'wikidatawiki-20161001-pages-meta-history1.xml-p002177658p002421529.bz2'


def h_parser(line):
    h = HTMLParser.HTMLParser()
    parsed_line = h.unescape(line)

    return parsed_line


# def process_buffer(buf):
#     tnode = cet.fromstring(buf)
#     print tnode



def extr_rev_data(rev_id,  time, revision):
    # global stat_counter
    # no_statements = 0
    # no_labels = 0
    # no_sitelinks = 0
    # no_aliases = 0
    # no_descriptions = 0
    # no_references = 0
    # properties_used = None
    dict_item = {}

    try:
        revision = ujson.loads(revision)
        item_id = revision['id']

        # if 'claims' in revision:
        #     no_statements = len(revision['claims'])
        #     counter_refs = 0
        #     try:
        #         properties_used = revision['claims'].keys()
        #
        #         for key in revision['claims'].keys():
        #             for i in revision['claims'][key]:
        #                 if 'references' in i:
        #                     counter_refs += 1
        #
        #         no_references = counter_refs
        #
        #     except AttributeError:
        #         properties_used = None

        # if 'labels' in revision:
        #     no_labels = len(revision['labels'])
        #
        # if 'sitelinks' in revision:
        #     no_sitelinks = len(revision['sitelinks'])
        #
        # if 'descriptions' in revision:
        #     no_descriptions = len(revision['descriptions'])
        #
        # if 'aliases' in revision:
        #     no_aliases = len(revision['aliases'])


        dict_item[item_id] = {}
        # dict_item['no_statements'] = no_statements
        # dict_item['no_labels'] = no_labels
        dict_item[item_id][rev_id] = {}

        dict_item[item_id][rev_id]['time_stamp'] = time
        # dict_item['no_aliases'] = no_aliases
        # dict_item['no_sitelinks'] = no_sitelinks
        # dict_item['no_references'] = no_references
        # dict_item['no_descriptions'] = no_descriptions
        # dict_item['properties_used'] = properties_used

        dict_item[item_id][rev_id]['labels'] = revision['labels']

        return dict_item

    except KeyError as k:
        print(k, 'key error')
        print(revision)
    except TypeError as t:
        print(t, 'type error')
        print(revision)

    #


def list_cleaner(rev_list):
    if '<timestamp>' in rev_list:
        rev_list = rev_list.replace('\t', '')
        rev_list = rev_list.replace('\n', '')
        rev_list = rev_list.replace('T', ' ')
        rev_list = rev_list.replace('Z', '')
        rev_list = re.sub(r'<timestamp>|</timestamp>', '', rev_list)
        rev_list = rev_list.lstrip(' ')

    elif '<text xml:space="preserve">' in rev_list:
        rev_list = rev_list.replace('<text xml:space="preserve">', '')
        rev_list = rev_list.replace('</text>', '')
        rev_list = rev_list.replace('\n', '')
        rev_list = h_parser(rev_list)
        rev_list = rev_list.decode('utf-8')
        rev_list = unicodedata.normalize('NFKD', unicode(rev_list)).encode('utf-8', 'ignore')
        rev_list = rev_list.lstrip(' ')

    else:
        rev_list = rev_list.replace('\t', '')
        rev_list = rev_list.replace('\n', '')
        rev_list = re.sub(
            r"<id>|</id>|<parentid>|</parentid>|<timestamp>|</timestamp>|<username>|</username>|<ip>|</ip>|<comment>|</comment>",
            '', rev_list)
        rev_list = rev_list.lstrip(' ')

    return rev_list


def file_extractor(file_name):
    rev_id = None
    time = None
    item_id = None
    user = None
    revision = None
    prev_line = '<none>'

    #    comment = None
    #    sha1 = None
    # par_id = None
    item_text = ''
    # counter = 0

    # try:
    #     params = {
    #         'database': 'wikidb',
    #         'user': 'postgres',
    #         'password': 'postSonny175',
    #         'host': 'localhost',
    #         'port': '5432'
    #     }
    #     conn = psycopg2.connect(**params)
    # except:
    #     print "I am unable to connect to the database."




    with bz2.BZ2File(file_name, 'rb') as inputfile:
        revision_list = []
        revision_processed = []
        counter = 0

        for line in inputfile:

            revision_list.append(line)

            if '</revision>' in line:
                clean_list = ['<revision>', '<contributor>', '</contributor>', '<model>', '<format>', '<sha1>']
                clean_list_2 = ['</page>', '<page>', '<ns>', '<title>', '<redirect']

                revision_clean = [revision for revision in revision_list if not any(x in revision for x in clean_list)]

                if '</page>' in revision_clean[0]:
                    counter += 1
                    # del revision_clean[0:5]
                    revision_clean = [revision for revision in revision_clean if
                                      not any(x in revision for x in clean_list_2)]
                    del revision_clean[0]
                    revision_clean.insert(1, 'no parent id')

                if '<username>' in revision_clean[3]:
                    del revision_clean[4]

                if '<comment>' not in revision_clean[4]:
                    revision_clean.insert(4, 'no comment')

                if '<minor />' in revision_clean[5]:
                    revision_clean[4] = revision_clean[6]
                    del revision_clean[5:7]

                revision_clean = map(list_cleaner, revision_clean)

                try:
                    revision_clean[5] = ujson.loads(revision_clean[5])
                    # revision_save = revision_clean
                    rev_process = extr_rev_data(revision_clean[0],  revision_clean[2], revision_clean[5])
                    revision_processed.append(rev_process)

                except ValueError as e:
                    print(e, revision_clean)


                revision_list = []

                if counter >= 1000:
                    dd = collections.defaultdict(dict)

                    for d in revision_processed:
                        for k, v in d.items():
                            dd[k].update(v)

                    # revision_processed = filter(None, revision_processed)
                    # revision_processed_clean = zip(*revision_processed)

                    try:
                        conn = pm.MongoClient()
                        db = conn.wikidb
                        collection = db.labelHistory
                        result = collection.update_many(dd)
                        print("Data updated with id", result)

                        # print 'imported'
                    except :
                        print('Data not updated')
                        # print 'not imported'
                        # print revision_clean

                    revision_processed = []
                    counter = 0
                    # print 'done!'
                    # break

                continue

        ### after last line
        # revision_processed = filter(None, revision_processed)

        for d in revision_processed:
            for k, v in d.items():
                dd[k].update(v)

        # revision_processed = filter(None, revision_processed)
        # revision_processed_clean = zip(*revision_processed)

        try:
            conn = pm.MongoClient()
            db = conn.wikidb
            collection = db.labelHistory
            result = collection.update_many(dd)
            print("Data updated with id", result)

            # print 'imported'
        except:
            print('Data not updated')
            # print 'not imported'
            # print revision_clean

        revision_processed = []
        counter = 0
        print(file_name + 'exported!')



def main():

    fin = sys.argv[1]
    file_extractor(fin)


if __name__ == "__main__":
    main()
