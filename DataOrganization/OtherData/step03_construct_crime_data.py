#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step03_construct_crime_data
# @Date: 2021/5/6
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.OtherData.step01_generate_important_industry
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import ConstantsWIN10 as const

if __name__ == '__main__':
    data_path = r'D:\wyatc\GoogleDrive\Projects\ChineseGovernmentSubsidies\data\from_scnu'
    crime_df: DataFrame = pd.read_excel(os.path.join(data_path, 'crime_data_2007-2015.xlsx')).rename(
        columns={'province code': 'ProvinceCode', 'Number of Job-related crimes ': 'crimeNum',
                 'Public official number（10000）': 'officialNum',
                 'ratio of crimes over number of public officials (number of crimes each 10000 persons)':
                     'crimeRatio'}).dropna(subset=['crimeNum'])
    crime_df.loc[:, 'ProvinceCode'] = (crime_df['ProvinceCode'] * 10000).astype(str)
    crime_df.loc[:, 'crimeNumLn'] = crime_df['crimeNum'].apply(lambda x: np.log(x + 1))
    crime_df.loc[:, 'officialNumLn'] = crime_df['officialNum'].apply(lambda x: np.log(x + 1))
    crime_df.drop(['province name'], axis=1).to_pickle(os.path.join(const.TEMP_PATH, 'crime_data_2007_2015.pkl'))
