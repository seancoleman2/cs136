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
        self.state["exp_down_bw"] = dict()
        self.state["exp_min_up_bw"] = dict()    
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))

        np_set = set(needed_pieces)

        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

        requests = [] 

        # COMPUTE RARITY - number of instances of a given piece
        counts = pd.DataFrame(data=np.zeros(len(self.pieces)))
        for peer in peers:  
            for piece_index in set(peer.available_pieces):
                counts.iloc[piece_index, 0] = counts.iloc[piece_index, 0] + 1

        # RAREST FIRST - request the pieces held by the fewest people
        # shuffle counts to break symmetry, then sort
        counts = counts.sample(frac=1)
        sorted_counts = counts.sort_values(0)

        # filter to only request elements we need
        filtered_sorted_counts = sorted_counts[sorted_counts.index.isin(np_set)]
        max_pieces_to_download = min(self.max_requests, filtered_sorted_counts.size)

        # iterate peers, requesting pieces in order of rarity
        for peer in peers:
            pieces_downloaded_from_peer = 0
            for piece_id in filtered_sorted_counts.index.values:
                if piece_id in peer.available_pieces:     
                    start_block = self.pieces[piece_id]
                    r = Request(self.id, peer.id, piece_id, start_block)
                    requests.append(r)

                    # check that we do not exceed maximum possible downloads
                    pieces_downloaded_from_peer = pieces_downloaded_from_peer + 1
                    if pieces_downloaded_from_peer == max_pieces_to_download:
                        break

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        ### CAPPED Version of Tyrant because we have Max Bandwidth ## 

        # initially assume 4 upload spots, with same bw_cap 
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
                self.state["exp_down_bw"][peer.id] = initializer
                self.state["exp_min_up_bw"][peer.id] = initializer

            else: 
                piece_delta = len(peer.available_pieces) - self.state["previously_available_pieces"][peer.id]
                self.state["blocks_downloaded_last_round"][peer.id] = piece_delta * self.conf.blocks_per_piece

            # update previously available pieces at "end of round" - this will not be adjusted after this
            self.state["previously_available_pieces"][peer.id] = len(peer.available_pieces)

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
        else:
            if round != 0:
                prev_round_downloads = history.downloads[-1:]
                prev_round_unchokers = set([])

                # compute how many blocks you have downloaded form each peer j last round
                blocks_downloaded_last_round = pd.DataFrame(data=np.zeros(len(peers)), index=[peer.id for peer in peers])
                for round in prev_round_downloads:
                    for download in round: 
                        blocks_downloaded_last_round.loc[download.from_id] += download.blocks
                        prev_round_unchokers.add(download.from_id)

                # if currently unchoked, expected download rate is the number of blocks you downloaded
                for prev_round_unchoker in prev_round_unchokers:
                    self.state["exp_down_bw"][prev_round_unchoker] = blocks_downloaded_last_round.loc[prev_round_unchoker].values[0]
                    self.state["conseq_rounds_unchoked_by"][prev_round_unchoker] += 1 

                    # if you have been unchoked for more than 2 rounds, decrease your expected required minimum upload rate for reciprocation
                    if self.state["conseq_rounds_unchoked_by"][prev_round_unchoker] > 2: 
                        u_j = self.state["exp_min_up_bw"][prev_round_unchoker]
                        gamma = .1
                        self.state["exp_min_up_bw"][prev_round_unchoker] = (1 - gamma) * u_j

                # those not unchoking us
                prev_round_chokers = [x for x in [peer.id for peer in peers] if x not in prev_round_unchokers]
                for prev_round_choker in prev_round_chokers:
                    # expect flow to be download_in/4
                    self.state["exp_down_bw"][prev_round_choker] = self.state["blocks_downloaded_last_round"][prev_round_choker] / float(init_spots)

                    # increase T_j for those who are choking us
                    alpha = .2
                    self.state["exp_min_up_bw"][prev_round_choker] = (1 + alpha) * self.state["exp_min_up_bw"][prev_round_choker]
                    self.state["conseq_rounds_unchoked_by"][prev_round_choker] = 0

            # ratios
            ratios = dict()
            f_ji, t_j = self.state["exp_down_bw"], self.state["exp_min_up_bw"]
            for peer in peers:
                # if they have been doing no downloading, do not share
                if t_j[peer.id] == 0: 
                    ratios[peer.id] = 0.0
                # otherwise, look at the return on investment
                else:
                    ratios[peer.id] = float(f_ji[peer.id])/float(t_j[peer.id])

            sorted_ratios = sorted(ratios, key=ratios.get, reverse=True)

            # iterate requesters in order or sorted ratios
            requesters = [request.requester_id for request in requests]
            chosen, bws = [], [] 
            total_t_j = 0.0
            for peer in sorted_ratios: 
                if peer in requesters:
                    # round up -> going below expected min threshold will waste BW
                    peer_exp_min_up_bw = math.ceil(self.state["exp_min_up_bw"][peer])

                    # check to see we don't exceed the upload capacity
                    if peer_exp_min_up_bw + total_t_j < bw_cap:
                        chosen.append(peer)
                        bws.append(peer_exp_min_up_bw)

                        # increment
                        total_t_j += peer_exp_min_up_bw

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
