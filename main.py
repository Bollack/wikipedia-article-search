import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch


URL = "https://en.wikipedia.org/w/api.php"
ARTICLES_NUMBER = 10
PARAMS = {
    "action": "query",
    "generator":"random",
    "grnnamespace":"0",
    "prop": "revisions",
    "rvprop": "content",
    "rvslots": "*",
    "formatversion": "2",
    "format": "json",
    "grnlimit": str(ARTICLES_NUMBER)
}



response = requests.get(url=URL, params=PARAMS)
json_response = response.json()

for article in json_response["query"]["pages"]:

  content_id = article["pageid"]
  title =  article["title"]
  print(title)
  content = article["revisions"][0]["slots"]["main"]["content"]
  #print(content)

  
  # Connect to 'http://localhost:9200'
  es = Elasticsearch("http://localhost:9200")
  # Datetimes will be serialized:
  es.index(index="wikipedia-articles", id=content_id, document={"content": content, "title": title, "timestamp": datetime.now()})
  #{'_id': 'content_id', '_index': 'wikipedia-articles', '_type': 'test-type', '_version': 1, 'ok': True}

  print(es.get(index="wikipedia-articles", id=content_id)['_source'])
