import requests
import json
import sys
import argparse
from datetime import datetime
from elasticsearch import Elasticsearch, BadRequestError


DEBUG_MODE = False

WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/w/api.php"
ES_CLUSTER_HOST = "http://localhost:9200"
DEFAULT_ES_INDEX = "wikipedia-articles"
ARTICLES_NUMBER = 10
DEFAULT_INDEX_MAPPING = { "mappings" : {
      "properties" : {
        "text" : {
          "type" : "text",
          "term_vector": "with_positions",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "timestamp" : {
          "type" : "date"
        },
        "title" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
      }
    }
}

DEFAULT_OBTENTION_PARAMS = {
    "action": "query",
    "generator":"random",
    "grnnamespace":"0",
    "prop": "revisions",
    "rvprop": "content",
    "rvslots": "*",
    "formatversion": "2",
    "format": "json"
}

def load_n_wikipedia_articles(index_name: str, number_articles:int=ARTICLES_NUMBER):
  '''Load number_articles wikipedia articles (Title and content) in
    the ES cluster under the given index name'''

  params = DEFAULT_OBTENTION_PARAMS
  params["grnlimit"] = str(number_articles)
  response = requests.get(url=WIKIPEDIA_BASE_URL, params=params)
  json_response = response.json()

  for article in json_response["query"]["pages"]:

    content_id = article["pageid"]
    title =  article["title"]
    if DEBUG_MODE: print(title)
    content = article["revisions"][0]["slots"]["main"]["content"]
    
    es = Elasticsearch(ES_CLUSTER_HOST)
    # Datetimes will be serialized:
    es.index(index=index_name, id=content_id, document={"text": content, "title": title, "timestamp": datetime.now()})

    if DEBUG_MODE: print(es.get(index=index_name, id=content_id)['_source']) 



def create_es_index(index_name:str, mapping:dict):
    es = Elasticsearch(ES_CLUSTER_HOST)
    try:
      es.indices.create(index=index_name, body=mapping)
    except BadRequestError as error:
      if 'resource_already_exists_exception' in error.body:
          return


def obtain_ids_from_index(index_name:str):
    es = Elasticsearch(ES_CLUSTER_HOST)
    response = es.search(index=DEFAULT_ES_INDEX, query={"match_all": {}})
    ids = []
    for value in response['hits']['hits']:
      ids.append(value['_id'])
    return ids


def get_total_index_token_count(index_name:str=DEFAULT_ES_INDEX, 
sort_response:bool=False, desc_order:bool=False):
    es = Elasticsearch(ES_CLUSTER_HOST)
    ids = obtain_ids_from_index(index_name)
    response = es.mtermvectors(index=DEFAULT_ES_INDEX,
                                   term_statistics=True, 
                                   fields=['text'], 
                                   ids=ids)
    global_dict = {}
    for doc_wordcount in response['docs']:
          tokens = doc_wordcount['term_vectors']['text']['terms']
          word_counts = doc_wordcount['term_vectors']['text']
          for token in tokens:
                if token not in global_dict:
                  global_dict[token] = tokens[token]['term_freq']
                else:
                  global_dict[token] += tokens[token]['term_freq']

    desc_order = True if desc_order else False
    return {k: v for k, v in sorted(global_dict.items(), key=lambda item: item[1], reverse=desc_order)} if sort_response else global_dict




if __name__ == "__main__":
    create_es_index(index_name=DEFAULT_ES_INDEX, mapping=DEFAULT_INDEX_MAPPING)
    load_n_wikipedia_articles(index_name=DEFAULT_ES_INDEX)
    parser = argparse.ArgumentParser(description='Wordcount using ES local cluster')
    #parser.add_argument('--sort', action="store", dest='sort_wordcount', default=False)
    #parser.add_argument('--desc', action="store", dest='desc', default=False)
    parser.add_argument('--sort', action=argparse.BooleanOptionalAction)
    parser.add_argument('--desc', action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    print(args.desc)
    print(get_total_index_token_count(sort_response=args.sort, desc_order=args.desc))