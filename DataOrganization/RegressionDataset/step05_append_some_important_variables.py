#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step05_append_some_important_variables
# @Date: 2021/5/12
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.RegressionDataset.step04_append_anti_corruption_timeline
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

if __name__ == '__main__':
    reg_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210416_china_policy_regression_data.pkl'))
    reg_df.loc[:, 'CIYear'] = reg_df['CIYear'].astype(int)
    crime_df: DataFrame = pd.read_pickle(os.path.join(const.DATA_PATH, 'crime_data_2007_2015.pkl'))
    governor_df: DataFrame = pd.read_pickle(
        os.path.join(const.DATA_PATH, 'province_leader_background_1995_2015.pkl')).drop(404)
    important_ind: DataFrame = pd.read_pickle(os.path.join(const.DATA_PATH, 'crucial_industry_list_2007_2020.pkl'))
    important_ind.loc[:, const.YEAR] = important_ind['year'].astype(int)
    crime_df.loc[:, 'highCrime'] = 0
    crime_dfs = list()
    for year in crime_df.year.drop_duplicates():
        year_df = crime_df.loc[crime_df[const.YEAR] == year].copy()
        middle_value = year_df['crimeRatio'].median()
        year_df.loc[:, 'highCrime'] = (year_df['crimeRatio'] > middle_value).astype(int)
        crime_dfs.append(year_df)

    crime_df2: DataFrame = pd.concat(crime_dfs, ignore_index=True)
    reg_df2: DataFrame = reg_df.merge(crime_df2, on=['ProvinceCode', const.YEAR], how='left').merge(
        governor_df, on=['ProvinceCode', const.YEAR], how='left').merge(
        important_ind, on=['IndustryCode', const.YEAR], how='left')
    for key in ['isTechEduGovernor', 'isTechWorkGovernor', 'isTechEduSec', 'isTechWorkSec']:
        reg_df2.loc[:, key] = reg_df2[key].fillna(0)
        reg_df2.loc[reg_df2[const.YEAR] > 2015, key] = np.nan

    reg_df2.loc[reg_df2[const.YEAR] >= 2007, 'isCrucialInd'] = reg_df2.loc[
        reg_df2[const.YEAR] >= 2007, 'isCrucialInd'].fillna(0)
    reg_df2.loc[reg_df2[const.YEAR] < 2007, 'isCrucialInd'] = np.nan

    reg_df2.to_stata(os.path.join(const.OUTPUT_PATH, '20210512_china_policy_analysis.dta'), write_index=False)
