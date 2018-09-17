#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 20:18:06 2018

Search API for the common crawl index files.

@author: zeyi
"""
import os, glob
import random
import gzip
import argparse

import pandas as pd

# def process_wrapper(lineByte):
#     with open("input.txt") as f:
#         f.seek(lineByte)
#         line = f.readline()
#         process(line)

# #create jobs
# with open("input.txt") as f:
#     nextLineByte = f.tell()
#     for line in f:
#         jobs.append( pool.apply_async(process_wrapper,(nextLineByte)) )
#         nextLineByte = f.tell()


class CCIdxSearcher:
    def __init__(self, summary_idx_pattern, idx_pattern):
        self.summary_idx_pattern = summary_idx_pattern
        self.idx_pattern = idx_pattern
        self.summary_idx_files = sorted(glob.glob(self.summary_idx_pattern))
        self.idx_files = sorted(glob.glob(self.idx_pattern))

        self._compile_summary_idx()

    def _compile_summary_idx(self):
        """Helper function for preparing summary index"""

        def _read_csv(path, f_idx):
            """path is the file name, and f_idx allows referencing to self.files for subsequent search"""
            df = pd.read_csv(path, header=None, names=('rdomain', 'start_idx', 'end_idx'))
            df['file_idx'] = f_idx
            return df
        random.sample
        self.summary_idx = pd.concat((_read_csv(path, i) for i, path in enumerate(self.summary_idx_files)), ignore_index=True)

    @staticmethod
    def _retrieve_lines(filename, start_idx, end_idx, start_byte, num_samples=None):
        """Retrieves lines from specified filenames. Can optionally sample as well
        This jumps to the byte location in the file (specified by start_byte), and starts performing a full scan
        Unfortunately, no reliable way of jumping directly to file number without loading data into memory 
        """
        idxs = range(0, end_idx-start_idx+1)
        if num_samples:
            idxs = sorted(random.sample(idxs, num_samples), reverse=False)
        else:
            idxs = list(idxs)[::-1] #Inversed
        res = []

        with gzip.open(filename, 'rt') as f:
            f.seek(start_byte)
            cur_idx = 0
            find_idx = idxs.pop()
            while res or cur_idx < end_idx + 1:
                line = f.readline()
                if cur_idx == find_idx:
                    res.append(line)
                    try:
                        find_idx = idxs.pop()
                    except KeyError: #No more to find
                        break
                cur_idx += 1

        return res


    def _search(self, pattern, num_samples=None):
        """Main search API
        Args:
            pattern: (str) Search pattern. e.g. com.google
        """
        first_file = self.summary_idx['rdomain'].searchsorted(pattern, side='left')
        last_file = self.summary_idx['rdomain'].searchsorted(pattern, side='right')
        files_df = self.summary_idx.iloc[first_file:last_file, :].copy()

        files_df['num_lines'] = files_df['end_idx'] - files_df['start_idx'] + 1
        #Divide up number of samples from each entry proportionately
        if num_samples:
            files_df['num_lines'] = (files_df['num_lines'] * num_samples / files_df['num_lines'].sum()).astype(int)

        res = []
        for _, row in files_df.iterrows():
            res += self._retrieve_lines(self.idx_files[row.file_idx], row.start_idx, row.end_idx, row.start_byte)

        return res

    def sample(self, pattern, num_samples=None):
        """Sample commoncrawl index based on specific patterns"""
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--summary_idx_pattern', default='summary_idx/*.gz',
        help='Search path to summary index files (e.g. summary_idx/*.gz). Supports reading gzip file')
    parser.add_argument('--idx_pattern', default='parsed/*.gz',
        help='Path to parsed index files (contains URLs). Supports reading gzip file')

    args = parser.parse_args()

    # Unpack arguments and dump to CCSearcher
    
    kwargs = {k:v for k, v in vars(args).items()}
    #searcher = CCSearcher(**kwargs)

