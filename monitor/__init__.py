#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# @Time    : 16/11/2 14:17

"""
curl -XPOST '10.78.174.67:9200/heart-2016.11.02/_search?pretty=true' -d'
{
  "fields": [
    "t_Log_type",
    "t_host_ip"
  ],
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "*"
        }
      },
      "filter": {
        "bool": {
          "must": {
            "range": {
              "@timestamp": {
                "from": "now-10m"
              }
            }
          }
        }
      }
    }
  },
  "size": 0,
  "aggs": {
    "types": {
      "terms": {
        "field": "t_Log_type",
        "size": 200
      },
      "aggs": {
        "errortypes": {
          "terms": {
            "field": "t_host_ip",
            "size": 200
          }
        }
      }
    }
  }
}'
"""
