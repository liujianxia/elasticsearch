#!/usr/bin/env python
# coding: utf-8

import connect


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


def keyword_count(index, field, keyword, interval="1m"):
    es = connect.connect()

    result = es.count(
        index="%s" % index,
        body={
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "%s" % field: "%s" % keyword
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


def avg_response_time(index, hostname, interval):
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


def get_response_time(index, hostname, interval="1m"):
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





