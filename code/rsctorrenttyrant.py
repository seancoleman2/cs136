#!/usr/bin/python

# This is a dummy peer that just illustrates the available information your peers 
# have available.

# You'll want to copy this file to AgentNameXXX.py for various versions of XXX,
# probably get rid of the silly logging messages, and then add more logic.

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer
import numpy as np 
import pandas as pd

class RSCTorrentTyrant(Peer):
    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.state = dict()

        # to adjust flow in and expected upload rate to attain unchoking
        self.state["conseq_rounds_unchoked_by"] = dict()

        # to conlude a peers download rate
        self.state["previously_available_pieces"] = dict()
        self.state["blocks_downloaded_last_round"] = dict() # represents total download_in 

        # used for ratios
        self.state["expected_download_rates"] = dict()
        self.state["expected_upload_rates"] = dict()    
    
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

        # initially assume 4 upload spots
        init_spots, bw_cap, = 4, self.up_bw
        initializer = float(even_split(bw_cap, init_spots)[0])

        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))

        # used for calculating the download rate 
        for peer in peers:
            if round == 0:
                # initialize dictionaries
                self.state["blocks_downloaded_last_round"][peer.id] = 0
                self.state["conseq_rounds_unchoked_by"][peer.id] = 0

                # we are estimating the other peers have the same number 
                # of upload spots and bw in the first round
                self.state["expected_download_rates"][peer.id] = initializer
                self.state["expected_upload_rates"][peer.id] = initializer

            else: 
                piece_delta = len(peer.available_pieces) - self.state["previously_available_pieces"][peer.id]
                self.state["blocks_downloaded_last_round"][peer.id] = piece_delta*self.conf.blocks_per_piece

            self.state["previously_available_pieces"][peer.id] = len(peer.available_pieces)

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            
            if round != 0:
                prev_round_downloads = history.downloads[-1:]
                prev_round_unchokers = []
                for round in prev_round_downloads:
                    for download in round:
                        # expected download rate is the number of blocks you downloaded
                        # from this person last round
                        prev_round_unchokers.append(download.from_id)

                        self.state["expected_download_rates"][download.from_id] = download.blocks
                        self.state["conseq_rounds_unchoked_by"][download.from_id] += 1 

                        # if you have been unchoked for more than 2 rounds
                        # decrease your expected upload rate
                        if self.state["conseq_rounds_unchoked_by"][download.from_id] > 2: 
                            u_j = self.state["expected_upload_rates"][download.from_id]
                            self.state["expected_upload_rates"][download.from_id] = 0.9 * float(u_j)

                # those not unchoking us
                choked_peers = [x for x in [peer.id for peer in peers] if x not in prev_round_unchokers]
                for peer in choked_peers:
                    # from book, expect flow to be download_in/4
                    self.state["expected_download_rates"][peer] = float(even_split(self.state["blocks_downloaded_last_round"][peer], init_spots)[0])

                    # increase T_j for those who are not unchoking us
                    self.state["expected_upload_rates"][peer] = 1.2*float(self.state["expected_upload_rates"][peer])
                    self.state["conseq_rounds_unchoked_by"][peer] = 0

            ratios = dict()
            f_ji, t_j = self.state["expected_download_rates"], self.state["expected_upload_rates"]
            for peer in peers:
                if t_j[peer.id] == 0: 
                    # is this correct???
                    ratios[peer.id] = 0.0
                else:
                    ratios[peer.id] = float(f_ji[peer.id])/float(t_j[peer.id])

            sorted_ratios = sorted(ratios, key=ratios.get, reverse=True)

            
            requesters = [request.requester_id for request in requests]
            chosen, bws = [], [] 
            total_t_j = 0.0
            for peer in sorted_ratios: 
                if peer in requesters:
                    # keep track of total expected upload bw
                    total_t_j = float(self.state["expected_upload_rates"][peer]) + total_t_j

                    # check to see we don't exceed the upload capacity, and upload approapriate bandwidth to person
                    if total_t_j < bw_cap:
                        chosen.append(peer)
                        bws.append(float(self.state["expected_upload_rates"][peer]))

            if len(chosen) == 0:
                bws = []


        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
