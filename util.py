#!/usr/bin/env python
# encoding=utf-8

import requests
import json


def keyword_search(url, index, keyword, interval):
    header = {'Content-Type': 'application/json'}

    body = {
        "query": {
            "range": {"@timestamp": "now-%s" % interval}
        }
    }
    message = {
        "q": "%s" % keyword
    }
    match = {
        "query": {
            "match": {"status": "200"}
        }
    }
    url_s = "%s%s/_search" % (url, index)
    print url_s
    print keyword
    print interval
    print body

    r = requests.get("%s" % url_s, headers=header, params=match)
    return r.text


if __name__ == "__main__":
    url = "http://116.62.148.72:9200/"
    index = "logstash-*"
    keyword = "*"
    interval = "1h"
    r = keyword_search(url, index, keyword, interval)
    print r
