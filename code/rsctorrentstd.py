#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging
import numpy as np 
import pandas as pd

from messages import Upload, Request
from util import even_split
from peer import Peer

class RSCTorrentStd(Peer):
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

        print("peers coming hot \n \n")
        print([peer.id for peer in peers])
        print("\n\n")

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
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            logging.debug("Still here: uploading to a random peer")

            # HISTORY: check previous 2 rounds 
            prev_two_round_downloads = history.downloads[-2:]
            peer_blocks_downloaded = pd.DataFrame(data=np.zeros(len(peers)), index=[peer.id for peer in peers])
            for round in prev_two_round_downloads:
                for download in round:
                    peer_blocks_downloaded.loc[download.from_id] = peer_blocks_downloaded.loc[download.from_id] + download.blocks

            # RECIPROCATION: compute top 3 uploaders who also requested
            sorted_peers = peer_blocks_downloaded.sort_values(0)
            sorted_peers_with_pos_upload = sorted_peers[sorted_peers[0] > 0].index.values
            requesters = [request.requester_id for request in requests]
            sorted_requesters_with_pos_upload = set(sorted_peers_with_pos_upload).intersection(set(requesters))

            # find top 3 and list of possible candidates for optimistic unchoking
            top_3_requesters = list(sorted_requesters_with_pos_upload)[:3]
            other_requesters = [requester for requester in requesters if requester not in top_3_requesters]

            # OPTIMISTIC UNCHOKING: unchoke randomly every 3 stages
            if len(other_requesters) > 0:
                if self.state["optimistic_spot"] is not None:
                    if self.state["round"] % 3 == 0:            
                        optimistic_spot = [random.choice(other_requesters)]
                    else:
                        optimistic_spot = [self.state["optimistic_spot"]]
                else:
                    optimistic_spot = [random.choice(other_requesters)]

                self.state["optimistic_spot"] = optimistic_spot[0]

            else:
                optimistic_spot = []

            chosen = top_3_requesters + optimistic_spot

            # Evenly "split" my upload bandwidth among the one chosen requester
            if(len(chosen) > 0):
                bws = even_split(self.up_bw, len(chosen))
            else:
                bws = []

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        self.state["round"] += 1
            
        return uploads
