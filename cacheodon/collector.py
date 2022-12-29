'''
Created on Dec 23, 2022

@author: leo
'''
import os
import pickle
from functools import reduce
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta

from flipton import MastodonInstanceSwitcher
from cacheodon.util import trace
from cacheodon.config import SKIP_HOSTS, MAX_STATUS_FETCH_PER_QUERY
from cacheodon.statusesdata import StatusesData
from warnings import warn

class Collector(object):
    '''
    Various methods for collecting data from the fediverse
    '''
    
    @staticmethod
    def _acct(user, host):
        return "@".join((user, host))


    def __init__(self, home_dir, use_app_tokens=False):
        '''
        home_dir - base directory for the data storage
        '''
        trace("Constructing cacheodon.Collector...", 1)
        self.home = Path(home_dir)
        if not self.home.exists():
            trace("cacheodon.Collector(): Creating home directory '%s'."%self.home)
            os.makedirs(self.home)
        else:
            trace("Using home directory '%s', initializing cache."%self.home)
            self._init_cache()
        
        self.mis = MastodonInstanceSwitcher(self.home, use_app_tokens=use_app_tokens)
        
            
    def _init_cache(self):
        self.cache_dir_base = self.home / "cache"
        trace("Initializing cache in %s'"%self.cache_dir_base)
        
        os.makedirs(self.cache_dir_base, exist_ok=True)
        self.cache_dir_follows = self.cache_dir_base / "follows"
        os.makedirs(self.cache_dir_follows, exist_ok=True)
        self.cache_dir_statuses = self.cache_dir_base / "statuses"
        os.makedirs(self.cache_dir_statuses, exist_ok=True)
        self.cache_dir_accts = self.cache_dir_base / "accounts"
        os.makedirs(self.cache_dir_accts, exist_ok=True)
        
        
    def get_account(self, user, host, update=False):
        """
        Try retrieving account info for the given user from cache.
        If cache doesn't exist, from server
        """
        cache = self._account_cache_file(user, host)
        if update or not cache.exists():
            acct_info = self._fetch_account(user, host)
            retrieved = datetime.now(timezone.utc)
            self._set_account_cache(user, host, (acct_info, retrieved))
        else:
            acct_info, retrieved = self._get_account_cache(user, host)
        return acct_info, retrieved
    
    
    def get_follows(self, user, host, update=False):
        """
        Try retrieving follows for the given user from cache.
        If cache doesn't exist, from server
        """
        cache = self._follows_cache_file(user, host)
        if update or not cache.exists():
            follows = self._fetch_follows(user, host)
            retrieved = datetime.now(timezone.utc)
            self._set_follows_cache(user, host, (follows, retrieved))
        else:
            follows, retrieved = self._get_follows_cache(user, host)
        return follows, retrieved
    
    
    def get_followers(self, user, host, update=False):
        """
        Try retrieving follows for the given user from cache.
        If cache doesn't exist, from server
        """
        cache = self._followers_cache_file(user, host)
        if update or not cache.exists():
            followers = self._fetch_followers(user, host)
            retrieved = datetime.now(timezone.utc)
            self._set_followers_cache(user, host, (followers, retrieved))
        else:
            followers, retrieved = self._get_followers_cache(user, host)
        return followers, retrieved
    
    
    def get_statuses(self, user, host, age_limit_hours=np.inf, update=False, discard_old=False):
        # Note: Does never update edits to posts once fetched
        acct = self._acct(user, host)
        cache = self._statuses_cache_file(user, host)
        if cache.exists() and not discard_old:
            old = self._get_statuses_cache(user, host)
        else:
            old = StatusesData(acct)
            
        if update or not cache.exists():
            # Create list of args for subsequent calls to 
            # self._fetch_statuses(user, host, **args)
            if old.size() > 0:
                min_id = old.max_id+1
            else:
                min_id = 0
                
            if age_limit_hours < np.inf():
                # Set min_id such that only newer posts than now()-age_limit are retrieved
                # TODO: Test!
                start = datetime.now(timezone.utc) - timedelta(hours=age_limit_hours)
                # Time to id conversion (copied from mastodon.internals.__unpack_id())
                start_id = (int(start.timestamp()) << 16) * 1000
                min_id = max(min_id, start_id)
            fetch_args = [dict(min_id=min_id, limit=MAX_STATUS_FETCH_PER_QUERY)]
            new = []
            for args in fetch_args:
                fetched = self._fetch_statuses(user, host, **args)
                new.append(StatusesData(acct, fetched))
            new = reduce(lambda x,y: x+y, fetched, initial=StatusesData(acct))
        else:
            new = StatusesData(acct)
        if new.size() == 0:
            trace("No new statuses for '%s'."%acct)
            return old

        merged = new+old
        if update and len(new) > 0:
            trace("New: %d posts, %d reblogs."%(new.nr_posts(), new.nr_reblogs()))
            trace("Total: %d posts, %d reblogs."%(merged.nr_posts(), merged.nr_reblogs()))
            trace("Saving statuses for '%s'"%(acct))
            self._set_statuses_cache(user, host, merged)
            trace("  done.")
        
        return merged
    

    
    def get_follows_of_follows(self, user, host, update=False):
        """
        Provides all follows of follows of the given account in a map acct->follows.
        """
        acct = self._acct(user, host)
        follows, oldest_retrieved = self.get_follows(user, host, update=update)
        if follows is None:
            trace("Couldn't retrieve follows for '%s'"%acct, 0)
            return None
        
        follows_of_follows = {}
        for i, facct in enumerate(follows):
            fuser, fhost = facct.split("@")
            trace("  acct: %s (%d/%d)"%(facct, i, len((follows["accts"]))), 2)
            
            if fhost in SKIP_HOSTS:
                trace("  Skipping host '%s' for '%s'"%(fhost, facct), 2)
                continue
            
            ffs, retrieved = self.get_follows(fuser, fhost, update=update)
            
            if ffs is not None:
                oldest_retrieved = min(retrieved, oldest_retrieved)
                follows_of_follows[facct] = ffs
            else:
                trace("Couldn't retrieve any follows information for '%s'"%facct, 1)
                continue
        return follows_of_follows, oldest_retrieved

    
    def _account_cache_file(self, user, host):
        return self.cache_dir_accts / ("acct_info_%s_%s.pickle"%(user, host))
    
    def _follows_cache_file(self, user, host):
        return self.cache_dir_follows / ("follows_%s_%s.pickle"%(user, host))
    
    def _followers_cache_file(self, user, host):
        return self.cache_dir_follows / ("followers_%s_%s.pickle"%(user, host))
    
    def _statuses_cache_file(self, user, host):
        return self.cache_dir_statuses / ("statuses_%s_%s.pickle"%(user, host))
    
    def _fetch_account(self, user, host):
        acct = self._acct(user, host)
        acct_info = self.mis.account_lookup(acct)
        return acct_info
    
    def _fetch_follows(self, user, host, update_accounts=True):
        """
        Try retrieving follows for the given user from their home server
        """
        acct = self._acct(user, host)
        try:
            trace("   Fetching all follows for '%s'"%(acct))
            acct_info = self.get_account(user, host)
            trace("   Number of follows: %d"%(acct_info["following_count"]))
            follows = self.mis.account_following(acct)
            follows = self.mis.fetch_remaining(follows)
            trace("   fetched: %d"%(len(follows)))
        except Exception as e:
            trace("Error fetching follows of %s: '%s'"%(acct, str(e)), 0)
            return None
        
        if update_accounts:
            retrieved = datetime.now(timezone.utc)
            
        follows_account_list = []
        for f_info in follows:
            f_acct_split = f_info["acct"].split("@")
            if len(f_acct_split) == 2:
                f_user, f_host = f_acct_split
            else:
                # User from same instance
                f_user, f_host = f_acct_split[0], host
            if update_accounts:
                self._set_account_cache(f_user, f_host, (f_info, retrieved))
            follows_account_list.append(self._acct(f_user, f_host))
        return follows_account_list
    
    
    def _fetch_followers(self, user, host, update_accounts=True):
        """
        Try retrieving followers for the given user from their home server
        """
        acct = self._acct(user, host)
        try:
            trace("   Fetching all followers for '%s'"%(acct))
            acct_info, retrieved = self.get_account(user, host)
            trace("   Number of followers: %d"%(acct_info["followers_count"]))
            followers = self.mis.account_followers(acct)
            followers = self.mis.fetch_remaining(followers)
            trace("   fetched: %d"%(len(followers)))
        except Exception as e:
            warn("Error fetching followers of %s: '%s'"%(acct, str(e)))
            raise e
            return None
        
        if update_accounts:
            retrieved = datetime.now(timezone.utc)
            
        followers_account_list = []
        for f_info in followers:
            f_acct_split = f_info["acct"].split("@")
            if len(f_acct_split) == 2:
                f_user, f_host = f_acct_split
            else:
                # User from same instance
                f_user, f_host = f_acct_split[0], host
            if update_accounts:
                self._set_account_cache(f_user, f_host, (f_info, retrieved))
            followers_account_list.append(self._acct(f_user, f_host))
        return followers_account_list
    
        
    def _get_follows_cache(self, user, host):
        cache = self._follow_cache_file(user, host)
        if cache.exists():
            with open(cache, "rb") as f:
                data = pickle.load(f)
        else:
            return None, None
        trace("  Loaded %d accounts followed by '%s' from cache '%s'."%(len(data["follows"]["accts"]), self._acct(user, host), cache))
        return data
    
    
    def _set_followers_cache(self, user, host, data):
        cache = self._followers_cache_file(user, host)
        with open(cache, "wb") as f:
            pickle.dump(data, f)
        trace("  Saved %d accounts following '%s' to cache '%s'."%(len(data[0]), self._acct(user, host), cache))
        
        
    def _get_followers_cache(self, user, host):
        cache = self._followers_cache_file(user, host)
        if cache.exists():
            with open(cache, "rb") as f:
                data = pickle.load(f)
        else:
            return None, None
        followers, retrieved = data
        if followers is None:
            trace("  Cached followers is None for '%s'."%self._acct(user, host))
        else:
            trace("  Loaded %d accounts following '%s' from cache '%s'."%(len(followers), self._acct(user, host), cache))
            trace(followers, pretty=True, msg_level=2)
        return data
    
    
    def _set_follows_cache(self, user, host, data):
        cache = self._follow_cache_file(user, host)
        with open(cache, "wb") as f:
            pickle.dump(data, f)
        trace("  Saved %d accounts followed by '%s' to cache '%s'."%(len(data["follows"]["accts"]), self._acct(user, host), cache))
        
        
    def _get_account_cache(self, user, host):
        cache = self._account_cache_file(user, host)
        if cache.exists():
            with open(cache, "rb") as f:
                data = pickle.load(f)
        else:
            return None, None
        trace("  Loaded account info for '%s' from cache '%s'."%(self._acct(user, host), cache))
        return data
    
    
    def _set_account_cache(self, user, host, data):
        cache = self._account_cache_file(user, host)
        with open(cache, "wb") as f:
            pickle.dump(data, f)
        trace("  Saved account dict for '%s' to cache '%s'."%(self._acct(user, host), cache))
        
        
    def _set_statuses_cache(self, user, host, data):
        cache = self._statuses_cache_file(user, host)
        with open(cache, "wb") as f:
            pickle.dump(data, f)
        trace("  Saved statuses for '%s' to cache '%s'."%(self._acct(user, host), cache))

    # def remove_duplicates(self, statuses, verb=1):
    #     trace("   Removing duplicates in fetched", 1, verb) 
    #     cleaned = []
    #     seen = set()
    #     for s in statuses:
    #         if s["url"] in seen:
    #             trace("      skipping duplicate status '%s'"%s["url"], 2, verb)
    #         else:
    #             cleaned.append(s)
    #     nr_removed = len(statuses) - len(cleaned)
    #     trace("   Removed %d duplicates."%nr_removed, 1, verb)
    #     return cleaned
            
    def _fetch_statuses(self, user, host, **fetch_args):
        acct = self._acct(user, host)
        trace("  fetch_statuses() for '%s'"%acct)

        acctinfo = self.mis.account_lookup(acct)
        nr_statuses = acctinfo["statuses_count"]
        trace("  total status count: %d"%nr_statuses)
        if nr_statuses == 0:
            trace("  No statuses.")
            return None
        limit = fetch_args["limit"]
        trace("  fetch limit: %d, min_id=%d"%(limit, fetch_args["min_id"]))
        statuses = self.mis.account_statuses(acct, **fetch_args)
        if statuses is None:
            trace("  Didn't fetch any statuses.")
            return None
        fetchtime = datetime.now(tz=timezone.utc)
        for s in statuses:
            s["fetched"] = fetchtime
        nr_received = nr_received_last = len(statuses)
        response = statuses
        no_more_newer_statuses = False
        while nr_received_last != 0 and nr_received < limit:
            if no_more_newer_statuses:
                response = self.msi.fetch_previous(response)
            else:
                response = self.msi.fetch_next(response)
            if response:
                fetchtime = datetime.now(tz=timezone.utc)
                for s in response:
                    s["fetched"] = fetchtime
                statuses += response
            else:
                # Empty response 
                if no_more_newer_statuses:
                    # fetch_previous() did not return any older, 
                    # thus we seem to have found all
                    nr_received_last = 0
                else:
                    # fetch_next() didn't find any newer,
                    # try fetching older instead
                    no_more_newer_statuses = True
                    # NOTE: this might fetch more than intended, 
                    #        perhaps because the limit is not set appropriately.
                    #        Would need: limit = limit - nr_received
                    response = statuses
                    continue
            nr_received_last = len(response)
            nr_received += nr_received_last
        trace("  fetched %d"%len(statuses))
        if len(statuses) == 0:
            return None
        else:
            return statuses
        
        
        
        