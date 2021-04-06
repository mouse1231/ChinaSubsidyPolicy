#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_check_original_data
# @Date: 2021/1/10
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python -m DataOrganization.step01_check_original_data
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

if __name__ == '__main__':
    data_df: DataFrame = pd.read_excel(
        os.path.join(const.DATA_PATH, '2020 Innovation_subsidies_policies_template.xlsx'), index_col=0,
        header=None).T.rename(
        columns={'省份': const.PROVINCE_NAME, '城市': const.CITY_NAME, '年份': const.YEAR, '是否有政策': 'has_policy'})
    data_df.loc[:, const.CITY_NAME] = data_df[const.CITY_NAME].replace([0, '上海市', '天津市', '重庆市', '北京市'], np.nan)
    province_level_df: DataFrame = data_df.loc[data_df[const.CITY_NAME].isnull()].copy()
    city_level_df: DataFrame = data_df.loc[data_df[const.CITY_NAME].notnull()].copy()

