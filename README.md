# wikipedia-article-search
An implementation for solving the use case of obtaining a word count out of Wikipedia articles. It uses a local ES cluster within one docker container to store the wikipedia articles and process them there.

# Requirements
 - Docker.
 - Set up RAM usage of Docker resources to at least 4 GB.
 - Python 3.7+

 # Usage
Within the repo, execute the following command:
```
docker-compose up
```

Once the ES cluster is up, go to http://localhost:5601 to access Kibana. You can click on `Configure Manually` when asked to enter the token and it should autofill to the ES address (localhost).

To run the application, run the command:

```
python3 main.py --sort --desc
```
The flags `sort` and `desc` indicate if the word count should be sorted and if such sort should be in descending order. `desc` without `sort` has no effect.

The application will create a ES index  called `wikipeda-articles`, crawl 10 wikipedia articles, store them in such index and then call a word count from ElasticSearch. A possible bottleneck with this approach is that the aggregation of the word count (Since the word count is only obtained per each article from ES) is done in the Python client. Either way, ES addds the value by providing native analyzers that allows for a better parsing of the tokens. The usage of Spark could solve the possible bottleneck, but the UDF would have to be defined so that the `m_term_vectors` of each article are obtained and processed all within the spark cluster nodes.