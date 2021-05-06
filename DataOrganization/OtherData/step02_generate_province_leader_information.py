#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_generate_province_leader_information
# @Date: 2021/5/6
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.OtherData.step01_generate_important_industry
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import ConstantsWIN10 as const

if __name__ == '__main__':
    data_path = r'D:\wyatc\GoogleDrive\Projects\ChineseGovernmentSubsidies\data\from_scnu'
    governor_df: DataFrame = pd.read_excel(os.path.join(data_path, '省长-全.xlsx'),
                                           usecols=['start_year', 'ProvinceCode', 'EDU', 'Work']).rename(
        columns={'start_year': const.YEAR, 'EDU': 'isTechEduGovernor', 'Work': 'isTechWorkGovernor'})
    secretary_df: DataFrame = pd.read_excel(os.path.join(data_path, '省委书记-全.xlsx'),
                                            usecols=['start_year', 'ProvinceCode', 'EDU', 'Work']).rename(
        columns={'start_year': const.YEAR, 'EDU': 'isTechEduSec', 'Work': 'isTechWorkSec'})
    merged_df: DataFrame = governor_df.merge(secretary_df, on=['ProvinceCode', const.YEAR], how='outer')
    merged_df.loc[:, 'ProvinceCode'] = (merged_df['ProvinceCode'] * 10000).astype(str)
    merged_df.to_pickle(os.path.join(const.TEMP_PATH, 'province_leader_background_1995_2015.pkl'))
