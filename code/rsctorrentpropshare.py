#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging
import math
import numpy as np 
import pandas as pd

from messages import Upload, Request
from util import even_split
from peer import Peer

class RSCTorrentPropShare(Peer):
    def post_init(self):
        print "post_init(): %s here!" % self.id

        self.state = dict()
        self.state["round"] = 0
        self.state["optimistic_spot"] = None
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        np_set = set(needed_pieces)  # sets support fast intersection ops.


        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

        requests = []   # We'll put all the things we want here
        # Symmetry breaking is good...
        random.shuffle(needed_pieces)
        
        # Sort peers by id.  This is probably not a useful sort, but other 
        # sorts might be useful
        peers.sort(key=lambda p: p.id)
        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        counts = pd.DataFrame(data=np.zeros(len(self.pieces)))
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            for piece_index in isect:
                counts.iloc[piece_index, 0] = counts.iloc[piece_index, 0] + 1


        filtered_counts = counts[counts.index.isin(np_set)]
        sorted_counts = filtered_counts.sort_values(0)
        n = min(self.max_requests, sorted_counts.size)

        for piece_id in sorted_counts.index.values[:n]:
             for peer in peers:
                if piece_id in peer.available_pieces:     
                    start_block = self.pieces[piece_id]
                    r = Request(self.id, peer.id, piece_id, start_block)
                    requests.append(r)
        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))

        if len(requests) == 0:
            requesters_with_pos_upload = []
            requesters = []
            peer_blocks_downloaded = pd.DataFrame()


            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            logging.debug("Still here: uploading to a random peer")

            prev_rounds = history.downloads[-1:]
            peer_blocks_downloaded = pd.DataFrame(data=np.zeros(len(peers)), index=[peer.id for peer in peers])
            for prev_round in prev_rounds:
                for download in prev_round:
                    peer_blocks_downloaded.loc[download.from_id] = peer_blocks_downloaded.loc[download.from_id] + download.blocks


            peers_with_pos_upload = peer_blocks_downloaded[peer_blocks_downloaded[0] > 0].index.values
            requesters = [request.requester_id for request in requests]

            requesters_with_pos_upload = [peer for peer in peers_with_pos_upload if peer in requesters]
            filtered_peer_blocks_downloaded = peer_blocks_downloaded[peer_blocks_downloaded.index.isin(requesters_with_pos_upload)]        
            filtered_peer_blocks_downloaded_percentage = filtered_peer_blocks_downloaded / filtered_peer_blocks_downloaded.sum()

            # TODO: update to only give .1 when there are no propshares
            optimistic_bw_percentage = .1 
            propshare_bw  = self.up_bw * (1-optimistic_bw_percentage)
            propshare_peer_bw = (filtered_peer_blocks_downloaded_percentage * propshare_bw).round()
            rounded_propshare_bw = propshare_peer_bw.sum().values[0]

            optimistic_bw = self.up_bw - rounded_propshare_bw

            # filtered candidates for optimistic unchocking
            other_requesters = list(set([requester for requester in requesters if requester not in requesters_with_pos_upload]))

            # OPTIMISTIC UNCHOKING: unchoke randomly every 3 stages
            if len(other_requesters) > 0:
                if self.state["optimistic_spot"] is not None:
                    if self.state["round"] % 3 == 0 or self.state["optimistic_spot"] in requesters_with_pos_upload:            
                        optimistic_spot = [random.choice(other_requesters)]
                    else:
                        optimistic_spot = [self.state["optimistic_spot"]]
                else:
                    optimistic_spot = [random.choice(other_requesters)]

                self.state["optimistic_spot"] = optimistic_spot[0]

            else:
                optimistic_spot = []

            # TODO: handle case where you are reciprocating with all players
            chosen = requesters_with_pos_upload + optimistic_spot

            # Evenly "split" my upload bandwidth among the one chosen requester
            if(len(chosen) > 0):
                bws = list(propshare_peer_bw.values.T[0]) + [optimistic_bw]
            else:
                bws = []

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]


        self.state["round"] += 1
            
        return uploads
