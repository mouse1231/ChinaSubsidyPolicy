#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step04_append_anti_corruption_timeline
# @Date: 2021/4/16
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
https://zh.wikipedia.org/wiki/%E4%B8%AD%E5%A4%AE%E5%B7%A1%E8%A7%86%E7%BB%84#cite_ref-41

python -m DataOrganization.RegressionDataset.step04_append_anti_corruption_timeline
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

INSPECTION_DICT = {'440000': 2013, '110000': 2014, '310000': 2014, '220000': 2013, '320000': 2014, '210000': 2014,
                   '340000': 2013, '510000': 2014, '430000': 2013, '330000': 2014, '130000': 2014, '650000': 2014,
                   '370000': 2014, '410000': 2014, '360000': 2013, '140000': 2013, '420000': 2013, '460000': 2014,
                   '500000': 2013, '450000': 2014, '350000': 2014, '120000': 2014, '530000': 2013, '620000': 2014,
                   '640000': 2014, '610000': 2014, '630000': 2014, '230000': 2014, '520000': 2013, '150000': 2013,
                   '540000': 2014}

if __name__ == '__main__':
    reg_df: DataFrame = pd.read_stata(os.path.join(const.OUTPUT_PATH, '20210401_china_policy_regression_data2.dta'))
    event_df = DataFrame(columns=['ProvinceCode', 'CIYear'])
    for p_code in INSPECTION_DICT:
        event_df.loc[event_df.shape[0]] = {'ProvinceCode': p_code, 'CIYear': INSPECTION_DICT[p_code]}

    reg_df2: DataFrame = reg_df.merge(event_df, on=['ProvinceCode'], how='left')
    reg_df2.loc[:, 'PostCI'] = (reg_df2[const.YEAR] > reg_df2['CIYear']).astype(int)
    reg_df2.to_pickle(os.path.join(const.TEMP_PATH, '20210416_china_policy_regression_data.pkl'))
