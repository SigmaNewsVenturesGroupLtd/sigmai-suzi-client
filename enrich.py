import logging
from datetime import datetime, timedelta
import argparse

import pymongo
import sigmai.elastic.elastic as elastic
from dateutil import parser
from elasticsearch2 import Elasticsearch
from itertools import islice, chain
from config import *
from suzi_inferer import score_articles

es_index = ES_INDEX

mongo_client = pymongo.MongoClient(MONGO_HOST, tz_aware=True)
mongo_collection = mongo_client.get_database(MONGO_DB)[MONGO_COLLECTION]


def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([next(batchiter)], batchiter)


url = "http://localhost:5001/api/v1/score"


def mapper(doc):
    doc = doc['_source']
    doc = {
        'esid': doc['id'],
        'title': doc['title'],
        'snip': doc['snippet'],
        'datiha': parser.parse(doc['dateHarvested'])
    }
    doc['dintha'] = int(doc['datiha'].strftime("%Y%m%d"))
    return doc


def main(start_date, end_date):
    logging.info(
        "Starting: {} - {}...".format(start_date, end_date),
        extra={})
    current_date = start_date
    total_docs = 0
    try:
        while current_date <= end_date:
            total_docs += process_date(current_date)
            logging.info("processing %s total docs processed %i" % (str(current_date), total_docs))
            current_date += timedelta(days=1)
    except Exception as e:
        logging.exception(e)
    logging.info(
        "Finished (total docs: {})".format(total_docs),
        extra={})


def process_date(current_date):
    """
    Enriches all articles for the given date from the given index
    :param current_date:
    :param index_name:
    :return:
    """
    total_docs = 0
    start = current_date
    end = current_date + timedelta(days=1)
    query = {
        "query": {
            "constant_score": {
                "filter": {
                    "bool": {
                        "must": [{"range": {
                            "dateHarvested": {"gte": start.strftime("%Y-%m-%d"),
                                              'lte': end.strftime("%Y-%m-%d")}}}
                        ]
                    }
                }
            }
        }
    }
    batch_size = 2000
    scroller = elastic.scroll(
        Elasticsearch(hosts=[ES_HOST], timeout=120, max_retries=10, retry_on_timeout=True),
        index=ES_INDEX,
        body=query,
        scroll='2m',
        clear_scroll=False,
        size=batch_size)
    docs = elastic.scroll_docs_mapped(scroller, mapper)
    for doc_batch in batch(docs, batch_size):
        doc_batch = list(doc_batch)
        suzi_input = [
            {'title': d['title'], 'snip': d['snip']} for d in doc_batch
        ]
        events = score_articles(suzi_input)
        updates = []
        for doc, doc_events in zip(doc_batch, events):
            for company_events in doc_events['events']:
                exploded = doc.copy()
                exploded['company_id'] = company_events['company_id']
                exploded['sdr_scores'] = company_events['scores']
                updates.append(exploded)
        mongo_collection.insert(updates)
        total_docs += len(doc_batch)
        logging.info("docs in day %i" % total_docs)
    return total_docs


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    yesterday_string = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    arg_parser.add_argument("-s", "--start-date", help="Start from date", default=yesterday_string)
    arg_parser.add_argument("-e", "--end-date", help="Run to date", default=yesterday_string)
    args = arg_parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s",
                        filename="log-%s-%s" % (args.start_date, args.end_date), level=logging.INFO)
    arg_start_date = datetime.strptime(args.start_date, '%Y%m%d').date()
    arg_end_date = datetime.strptime(args.end_date, '%Y%m%d').date()
    main(arg_start_date, arg_end_date)
