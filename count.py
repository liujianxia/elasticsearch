#!/usr/bin/env python
# encoding=utf-8

import connect


def status_code(index, status, interval):
    # establish connection to ES server
    es = connect.connect()

    # Query DSL api . Match http status code check the past time
    result = es.count(
        index="%s-*" % index,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"status": "%s" % status}}
                    ],
                    "filter": [
                        {"range": {"@timestamp": {"gte": "now-%s" % interval}}}
                    ]
                }
            }
        })
    # return the count of http status code
    return result['count']


def keyword_count(index, keyword, interval):
    es = connect.connect()

    # keyword query
    result = es.count(
        index="%s" % index,
        body={
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "message": "%s" % keyword
                        }
                    },
                    "filter": {
                        "range": {
                            "@timestamp": {
                                "gt": "now-%s" % interval,
                                "lt": "now"
                            }
                        }
                    }
                }
            }
        }
    )
    return result['count']


def avg_resopnse_time(index,hostname,interval):
    es = connect.connect()

    avg = es.search(
        index="%s" % index,
        size=0,
        body={
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "beat.hostname": "%s" % hostname
                        }
                    },
                    "filter": {
                        "range": {
                            "@timestamp": {
                                "gt": "now-%s" % interval,
                                "lt": "now"
                            }
                        }
                    }
                }
            },
            "aggs": {
                "upstream_response_time": {
                    "avg": {
                        "field": "upstream_response_time"
                    }
                }
            }

        }
    )
    return avg

if __name__ == "__main__":
    #count = status_code("logstash", 200, "POST", "30h")
    #print count
    result = keyword_count("logstash-*","stuAppApi","4h")
    print result
    #result2 = avg_resopnse_time("logstash-*", "iZbp1gfnfrgfa947vey7zhZ","5h")
    #print result2
