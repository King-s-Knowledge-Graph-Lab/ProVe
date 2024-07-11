import json
import random
import uuid
import numpy as np
import time
import requests
import traceback
import pdb
import math
import ast
import pandas as pd
import pickle
from qwikidata.linked_data_interface import get_entity_dict_from_api
from qwikidata.sparql import return_sparql_query_results

from urllib3.exceptions import MaxRetryError, ConnectionError
from qwikidata.linked_data_interface import LdiResponseNotOk

import hashlib

class CachedWikidataAPI():
    
    def __init__(self, cache_path = 'entity_cache.p', save_every_x_queries=1):
        self.save_every_x_queries = save_every_x_queries
        self.x_queries_passed = 0
        self.languages = ['en','fr','es','pt','pt-br','it','de']
        self.cache_path = cache_path
        try:
            with open(self.cache_path,'rb') as f:
                self.entity_cache = pickle.load(f)
        except FileNotFoundError:
            self.entity_cache = {}
            
    def get_unique_id_from_str(self, my_str):
        return hashlib.md5(str.encode(my_str)).hexdigest()
        
    def save_entity_cache(self, force=False):
        if force:
            self.x_queries_passed = self.save_every_x_queries
        self.x_queries_passed = self.x_queries_passed+1
        if self.x_queries_passed >= self.save_every_x_queries:
            with open(self.cache_path,'wb') as f:
                pickle.dump(self.entity_cache,f)
            self.x_queries_passed = 0

    def get_entity(self, item_id):
        if item_id in self.entity_cache:
            return self.entity_cache[item_id]
        while True:
            try:
                entity = get_entity_dict_from_api(item_id)
                self.entity_cache[item_id] = entity
                self.save_entity_cache()
                return entity
            except (ConnectionError, MaxRetryError) as e:
                #traceback.print_exc()
                time.sleep(1)
                continue
            except LdiResponseNotOk:
                #traceback.print_exc()
                self.entity_cache[item_id] = 'deleted'
                self.save_entity_cache()
                return 'deleted'

    def get_label(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return (entity, 'none')
            labels = entity['labels' if 'labels' in entity else 'lemmas']
        elif type(item) == dict:
            if 'labels' in item:
                labels = item['labels']
            elif 'lemmas' in item:        
                labels = item['lemmas']
        for l in self.languages:
            if l in labels:
                return (labels[l]['value'], l)
        if non_language_set:
            all_labels = list(labels.keys())
            if len(all_labels)>0:
                return (labels[all_labels[0]]['value'], all_labels[0])
        return ('no-label', 'none')
    
    def get_desc(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return (entity, 'none')
            descriptions = entity['descriptions']
        elif type(item) == dict:
            if 'descriptions' in item:
                descriptions = item['descriptions']
        for l in self.languages:
            if l in descriptions:
                return (descriptions[l]['value'], l)
        if non_language_set:
            all_descriptions = list(descriptions.keys())
            if len(all_descriptions)>0:
                return (descriptions[all_descriptions[0]]['value'], all_descriptions[0])
        return ('no-desc', 'none')
    
    def get_alias(self, item, non_language_set=False):
        if type(item) == str:        
            entity = self.get_entity(item)
            if entity == 'deleted':
                return ([entity], 'none')
            aliases = entity['aliases']
        elif type(item) == dict:
            if 'aliases' in item:
                aliases = item['aliases']
        for l in self.languages:
            if l in aliases:
                return ([alias['value'] for alias in aliases[l]], l)
        if non_language_set:
            all_aliases = list(aliases.keys())
            if len(all_aliases)>0:            
                return (aliases[all_aliases[0]]['value'], all_aliases[0])
                return ([alias['value'] for alias in aliases[all_aliases[0]]], all_aliases[0])
        return ('no-alias', 'none')

    def get_datatype(self, item):
        try:
            if type(item) == str:
                entity = self.get_entity(item)
                if entity == 'deleted':
                    return entity
                datatype = entity['datatype']
            elif type(item) == dict:
                datatype = item['datatype']
            return datatype
        except KeyError:
            return 'none'

    def get_claim_values_of(self, item, property_id):
        if type(item) == str:
            entity = self.get_entity(item)
            if entity == 'deleted':
                return entity
            claims = entity['claims']
        elif type(item) == dict:
            claims = item['claims']
        if property_id in claims:
            instance_of_claims = claims[property_id]
            return [i['mainsnak']['datavalue']['value']['id'] for i in instance_of_claims]
        else:
            return []

    def query_sparql_endpoint(self, sparql_query):
        sparql_query_id = self.get_unique_id_from_str(sparql_query)
        if sparql_query_id in self.entity_cache:
            return self.entity_cache[sparql_query_id]
        else:
            wikidata_sparql_url = 'https://query.wikidata.org/sparql'
            try:
                while True:
                    res = requests.get(wikidata_sparql_url, params={"query": sparql_query, "format": "json"})
                    if res.status_code in (429,504):
                        time.sleep(1)
                        continue
                    elif res.status_code == 200:
                        res = res.json()
                        self.entity_cache[sparql_query_id] = res
                        self.save_entity_cache()
                        return res
                    else:
                        print(res.status_code)
                        raise Exception
            except json.JSONDecodeError as e:
                #pdb.set_trace()
                print(res, res.__dict__)
                raise e
    
    def custom_sparql_query(self, sparql_query):
        wikidata_sparql_url = 'https://query.wikidata.org/sparql'
        res = requests.get(wikidata_sparql_url, params={"query": sparql_query, "format": "json"})
        return res