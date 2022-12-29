'''
Created on Dec 23, 2022

@author: leo
'''
import numpy as np
from urllib3.util.url import parse_url

from .util import trace

# Which fields to store, when statuses are fetched
STATUS_FIELDS = ["account","id", "url", "created_at", "spoiler_text", "content", "in_reply_to_account_id",
                 "tags", "mentions", "reblogs_count", "favourites_count", "replies_count",
                 "language", "media_attachments.description", "fetched"]
                # "media_attachments.text_url"]
                        
class StatusesData(object):
    '''
    Object holding sequential information on a sequence of statuses
    '''
    
    @staticmethod
    def _append(data_dict, post):
        if post["id"] in data_dict["id"]:
            trace("Post-id '%d' already in data. Skipping post."%post["id"])
        for k in STATUS_FIELDS:
            a = data_dict[k]
            e = post.get(k, None)
            if k == "account":
                acct = e["acct"]
                if len(acct.split("@")) == 1:
                    # User from same server as query
                    host = parse_url(e["url"]).hostname
                    acct = "@".join((acct, host))
                e = acct
            elif k == "tags":
                e = [x["name"] for x in e]
            elif k == "mentions":
                e = [x["acct"] for x in e]
            elif k == "media_attachments.description":
                e = [x["description"] for x in e]
            a.append(e)
            

    def __init__(self, acct, response=None):
        self.acct = acct
        self.posts = dict([(k, []) for k in STATUS_FIELDS])
        self.reblogs = dict([(k, []) for k in STATUS_FIELDS])
        self.min_id, self.max_id = np.inf, 0
        if response is not None:
            for status in response:
                self.append(status)
                
                
    def size(self):
        return self.nr_posts() + self.nr_reblogs()

    def nr_reblogs(self):
        return len(self.reblogs["id"]) 

    def nr_posts(self):
        return len(self.posts["id"]) 
        
        
    def append(self, status):
        self.min_id = min(self.min_id, status.get["id"])
        self.max_id = max(self.max_id, status.get["id"])
        is_reblog = status.get("reblog", None)
        if is_reblog:
            self._append(self.reblogs, status["reblog"])
        else:
            # TODO: Check that this post is from self.acct
            self._append(self.posts, status)


    def __add__(self, sdata):
        if self.acct != sdata.acct:
            raise Exception("StatusesData is tied to a unique account. Cannot merge data for '%s' and '%s'."%(self.acct, sdata.acct))
        for k in STATUS_FIELDS:
            self.posts[k] += sdata.posts[k]
            self.reblogs[k] += sdata.reblogs[k]
        self.max_id = max(self.max_id, sdata.max_id)
        self.max_id = min(self.min_id, sdata.min_id)
            
        
        