#!/usr/bin/env python
# coding: utf-8

from count import get_response_time
from count import keyword_count


if __name__ == "__main__":
    # avg_time = get_response_time("logstash-*", "iZbp1gfnfrgfa947vey7zhZ")
    # print avg_time
    count = keyword_count("logstash-*", "method", "POST", "800h")
    print count
