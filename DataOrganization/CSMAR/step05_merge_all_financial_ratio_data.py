#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step05_merge_all_financial_ratio_data
# @Date: 2021/3/28
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
财务指标分析

python -m DataOrganization.CSMAR.step05_merge_all_financial_ratio_data
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file

if __name__ == '__main__':
    CSMAR_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '财务指标分析')

    file_list = os.listdir(CSMAR_PATH)
    ratio_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '比率结构211512208.zip'))
    debt_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '偿债能力211423212.zip'))
    develop_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '发展能力211732539.zip'))
    dividend_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '股利分配211829952.zip'))
    operation_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '经营能力211620335.zip'))
    share_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '每股指标211808551.zip'))
    disclosure_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '披露财务指标211412279.zip'))
    cashflow_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '现金流分析211634585.zip'))
    relative_value: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '相对价值指标211831919.zip'))
    profit_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '盈利能力211642176.zip'))
    risk_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '风险水平211956512.zip'))

    result_df = DataFrame()

    for file_name in file_list:
        data_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, file_name))

        data_df.loc[:, const.YEAR] = data_df['Accper'].str[:4].astype(int)
        data_df.loc[:, 'Stkcd'] = data_df['Stkcd'].astype(str).str.zfill(6)

        drop_keys = ['Accper']

        if 'Typrep' in data_df.keys():
            drop_keys.append('Typrep')

        data_df2: DataFrame = data_df.drop(drop_keys, axis=1).drop_duplicates(subset=['Stkcd', const.YEAR], keep='last')
        if result_df.empty:
            result_df: DataFrame = data_df2.copy()
        else:
            result_df: DataFrame = result_df.merge(data_df2, on=['Stkcd', const.YEAR], how='outer')

        y_end_keys = [i for i in result_df.keys() if i.endswith('_y')]
        if y_end_keys:
            drop_key_list = list()
            for y_key in y_end_keys:
                key = y_key[:-2]
                x_key = '{}_x'.format(key)

                result_df.loc[:, key] = result_df[x_key].fillna(result_df[y_key])
                drop_key_list.extend([x_key, y_key])
            result_df = result_df.drop(drop_key_list, axis=1)

    result_df.to_pickle(os.path.join(const.TEMP_PATH, '20210328_csmar_financial_analysis_dataset.pkl'))
