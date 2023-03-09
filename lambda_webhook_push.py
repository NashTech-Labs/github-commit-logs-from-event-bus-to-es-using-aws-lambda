import os
from elasticsearch import Elasticsearch

print("Shipping to Elasticsearch")

es_url = os.environ.get('ES_HOST')
es_username = os.environ.get('ES_USERNAME')
es_password = os.environ.get('ES_PASSWORD')
es = Elasticsearch(es_url, http_auth=(es_username, es_password))

index = "github_commit_details"

body = {
    "mappings": {
        "properties": {
            "repository_name": {"type": "keyword"},
            "branch_name": {"type": "keyword"},
            "commit_sha":  {"type": "text"},
            "commit_message":  {"type": "text"},
            "author_name":    {"type": "keyword"},
            "commit_url":   {"type": "text"},
            "added_files":   {"type": "text"},
            "removed_files":   {"type": "text"},
            "modified_files":   {"type": "text"},
            "date":   {"type": "date"}
        }
    },
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    }
}


def check_null(value):
    '''    This function checks if a value is null or empty'''
    if value == "":
        value = "None"
        return value
    else:
        return value


def list_to_string(value):
    '''This function converts a list to a string'''
    value = ", ".join(value)
    return value


def es_reachable():
    '''This function checks if Elasticsearch is reachable'''
    return es.ping()


def index_exists():
    '''This function checks if the index exists in Elasticsearch'''
    return es.indices.exists(index=index)


def shipping_to_es(data):
    '''This function sends the data to Elasticsearch'''
    if not es_reachable:
        print("Elasticsearch is not reachable")
        return False
    if not index_exists():
        print("Index does not exist. Creating an index...")
        respose = es.indices.create(index=index, body=body)
        if not respose["acknowledged"]:
            print("Index creation failed")
            return False
    print("Shipping data to Elasticsearch")
    es.bulk(index=index, body=data)
    return True


def lambda_handler(event, context):
    '''This function is the handler for the Lambda function'''
    event = event['detail']
    repository_name = event['repository']['name']
    branch_name = event['ref'].split('/')[-1]
    try:
        bulk_api_body = []
        print(f"Total number of commits in this push: {len(event['commits'])}")
        for commit in event['commits']:
            action = {
                "index": {
                    "_index": index,
                    "_id": commit['id']
                }
            }
            eachCommit = {
                "repository_name": repository_name,
                "branch_name": branch_name,
                "commit_sha": commit['id'],
                "commit_message": check_null(commit['message']),
                "author_name": commit['author']['name'],
                "commit_url": commit['url'],
                "added_files": check_null(list_to_string(commit['added'])),
                "removed_files": check_null(list_to_string(commit['removed'])),
                "modified_files": check_null(list_to_string(commit['modified'])),
                "date": commit["timestamp"]
            }
            bulk_api_body.append(action)
            bulk_api_body.append(eachCommit)
        print(bulk_api_body)
        response = shipping_to_es(bulk_api_body)
        if not response:
            print("Failed to ship data to Elasticsearch")
            return "Shipping failed"
        return "Data shipped to Elasticsearch"
    except Exception as e:
        print(f"The Exception Occurred: {e}")
        raise e

