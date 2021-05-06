#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_generate_important_industry
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
    important_ind: DataFrame = pd.read_excel(os.path.join(data_path, '重点行业.xlsx'),
                                             usecols=['csmar_cod', 'start_year', 'end_year'])

    result_imp_industry = DataFrame(columns=['IndustryCode', const.YEAR, 'isCrucialInd'])

    for ind_code in important_ind['csmar_cod'].drop_duplicates():
        code_df = important_ind.loc[important_ind['csmar_cod'] == ind_code].copy()
        year_set = set()
        for i in code_df.index:
            start_year = code_df.loc[i, 'start_year']
            end_year = code_df.loc[i, 'end_year']
            year_set.update(range(start_year, end_year + 1))

        for year in year_set:
            index = result_imp_industry.shape[0]
            result_imp_industry.loc[index, const.YEAR] = year
            result_imp_industry.loc[index, 'IndustryCode'] = ind_code

    result_imp_industry.loc[:, 'isCrucialInd'] = 1
    result_imp_industry.to_pickle(os.path.join(const.TEMP_PATH, 'crucial_industry_list_2007_2020.pkl'))
