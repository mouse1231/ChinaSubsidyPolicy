#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_merge_file_list
# @Date: 2021/1/17
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python -m DataCollection.PKULawData.step01_merge_file_list
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const


if __name__ == '__main__':
    file_list = os.listdir(os.path.join(const.ROOT_PATH, 'data', 'pkulaw'))

    data_dfs = list()
    for f in file_list:
        if not f.endswith('.xls'):
            continue

        df: DataFrame = pd.read_excel(os.path.join(const.ROOT_PATH, 'data', 'pkulaw', f), skiprows=2).iloc[:-2]
        df.loc[:, 'source'] = f.split('.')[0]
        data_dfs.append(df)

    result_df: DataFrame = pd.concat(data_dfs, ignore_index=True)

    agency_df: DataFrame = result_df.loc[:, '发布部门'].drop_duplicates()
    result_df.to_pickle(os.path.join(const.ROOT_PATH, 'temp', '20210117_all_file_list.pkl'))
    agency_df.to_excel(os.path.join(const.ROOT_PATH, 'temp', '20210117_agency_list.pkl'))
