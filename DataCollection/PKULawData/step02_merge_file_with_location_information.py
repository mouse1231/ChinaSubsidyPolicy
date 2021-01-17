#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_merge_file_with_location_information
# @Date: 2021/1/17
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python -m DataCollection.PKULawData.step01_merge_file_list
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import ConstantsWSL as const

if __name__ == '__main__':
    list_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210117_all_file_list.pkl'))
    agency_location: DataFrame = pd.read_excel(os.path.join(const.TEMP_PATH, '20210117_agency_list.xlsx'), index_col=0)
    list_location_df: DataFrame = list_df.merge(agency_location, on=['发布部门'])
    list_location_df.loc[999, const.CITY_NAME] = np.nan

    for key in ['发布日期', '实施日期']:
        list_location_df.loc[:, key] = pd.to_datetime(list_location_df[key])

    list_location_df.index.name = 'file_id'
    list_location_df2: DataFrame = list_location_df.reset_index(drop=False)
    list_location_df.to_pickle(os.path.join(const.TEMP_PATH, '20210117_policy_file_with_file_id.pkl'))
