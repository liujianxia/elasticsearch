#!/usr/bin/env python
# encoding=utf-8

import connect
import json
import sys


def status_code(index, status, hostname, interval):
    # establish connection to ES server
    es = connect.connect()

    # Query DSL api . Match http status code check the past time
    result = es.count(
        index="%s-*" % index,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"status": "%s" % status}},
                        {"natch": {"beat.hostname": "%s" % hostname}}
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


def get_response_time(index, hostname, interval):
    es = connect.connect()

    result = es.search(
        index="%s" % index,
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

            "_source": "upstream_response_time"
        })
    time_sum = 0
    k = 0
    if len(result['hits']['hits']) == 0:
        return 0
    else:
        for i in range(len(result['hits']['hits'])):

            if 'upstream_response_time' in result['hits']['hits'][i]['_source'].keys():
                time_sum = time_sum + result['hits']['hits'][i]['_source']['upstream_response_time']
                k = k+1
        return time_sum/k


if __name__ == "__main__":
    #count = status_code("logstash", 200, "POST", "30h")
    #print count
    #result = keyword_count("logstash-*","stuAppApi","4h")
    #print result
    #result2 = avg_resopnse_time("logstash-*", "iZbp1gfnfrgfa947vey7zhZ","5h")
    #print result2
    avg_time = get_response_time("logstash-*","iZbp1gfnfrgfa947vey7zhZ","800m")
    print avg_time
    if sys.argv[1] == "avg_resopnse_time":
        avg_time = get_response_time("nginx-access*",sys.argv[2],"1m")
        print avg_time
    else:
        if sys.argv[1] == "keyword_partten":
            count_keyword = keyword_count(sys.argv[2], sys.argv[3], "1m")
            print count_keyword
        else:
            if sys.argv[1] == "status":
                status_count = status_code("nginx-access*", sys.argv[2],sys.argv[3], "1m")
                print status_count

