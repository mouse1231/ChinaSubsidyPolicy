#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_merge_stock_return_and_province_level_data
# @Date: 2021/3/29
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.RegressionDataset.step02_merge_stock_return_and_province_level_data
"""

import os

import numpy as np
import pandas as pd
from tqdm import tqdm
from pandas import DataFrame

from Constants import Constants as const

if __name__ == '__main__':
    reg_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210329_china_policy_reg_file.pkl'))
    csmar_pro_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210401_csmar_provincial_data.pkl'))
    csmar_stock_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210401_csmar_stock_return_data.pkl'))

    log_keys = ['Epwa0401', 'Epwa0402', 'Epwa0403', 'Epwa0404', 'Gdp0101', 'Gdp0102', 'Gdp0103', 'Gdp0104', 'Gdp0105',
                'Gdp0106', 'Gdp0107', 'Gdp0108', 'Gdp0109', 'Gdp0110', 'Gdp0111', 'Gdp0112', 'Gdp0116', 'Gove0101',
                'Gove0102', 'Gove0103', 'Gove0104', 'Gove0105', 'Gove0106', 'Gove0107', 'Gove0108', 'Gove0109',
                'Gove0110', 'Gove0111', 'Gove0112', 'Gove0113', 'Gove0114', 'Gove0115', 'Gove0117', 'Gove0118',
                'Gove0119', 'Gove0120', 'Gove0121', 'Gove0122', 'Gove0123', 'Gove0124', 'Gove0125', 'Gove0126',
                'Gove0127', 'Gove0128', 'Gove0131', 'Gove0132', 'Gove0133', 'Gove0134', 'Gove0135', 'Gove0136',
                'Gove0137', 'Gove0138', 'Gove0139', 'Gove0140', 'Gove0141', 'Gove0142', 'Gove0143', 'Gove0144',
                'Gove0145', 'Gove0146', 'Gove0147', 'Gove0148', 'Gove0149', 'Gove0150', 'Gove0151', 'Gove0152',
                'Gove0153', 'Gove0154', 'Gove0155', 'Gove0156', 'Gove0157', 'Gove0158', 'Gove0159', 'Gove0160',
                'Gove0161', 'Gove0162', 'Gove0201', 'Gove0202', 'Gove0203', 'Gove0204', 'Gove0205', 'Gove0206',
                'Gove0207', 'Gove0208', 'Gove0209', 'Gove0210', 'Gove0211', 'Gove0212', 'Gove0213', 'Gove0214',
                'Gove0215', 'Gove0216', 'Gove0217', 'Gove0218', 'Gove0219', 'Gove0220', 'Gove0221', 'Gove0222',
                'Gove0223', 'Gove0224', 'Gove0225', 'Gove0226', 'Gove0227', 'Gove0228', 'Gove0229', 'Gove0230',
                'Gove0231', 'Gove0232', 'Gove0233', 'Gove0234', 'Gove0235', 'Gove0236', 'Gove0238', 'Indu0101',
                'Indu0102', 'Indu0103', 'Indu0104', 'Indu0105', 'Indu0106', 'Indu0107', 'Indu0108', 'Indu0109',
                'Indu0110', 'Pop01', 'Pop06', 'Pop07', 'Pop08', 'Pop09', 'Pop10', 'Pop11', 'Pop12', 'Pop13', 'Pop14',
                'Pop15', 'Pop16', 'Pop17', 'Pop18', 'Pop19', 'Pop20', 'Pop21', 'Pop22', 'Pop23', 'Pop24', 'Pop25',
                'Pop26', 'Pop27', 'Pop28', 'Pop29', 'Pop30', 'Pop31', 'Pop32', 'Ifa0501', 'Ifa0502', 'Ifa0503',
                'Ifa0504', 'Ifa0701', 'Ifa0702', 'Ifa0703', 'Ifa0704', 'Ifa0705', 'Ifa0706', 'Ifa0707', 'Ifa0601',
                'Ifa0602', 'Ifa0603', 'Ifa0604', 'Ifa0605', 'Ifa0606', 'Ifa0607', 'Ifa0101', 'Ifa0102', 'Ifa0103',
                'Ifa0104', 'Ifa0105', 'Ifa0106']

    csmar_pro_df2: DataFrame = csmar_pro_df.rename(columns={'Prvcnm_id': 'ProvinceCode', 'Sgnyea': const.YEAR,
                                                            'Prvcnm': 'Province'})
    for key in tqdm(log_keys):
        csmar_pro_df2.loc[:, '{}_ln'.format(key)] = csmar_pro_df2[key].apply(lambda x: np.log(x))

    csmar_stock_df2: DataFrame = csmar_stock_df.copy()
    csmar_stock_df2.loc[:, const.YEAR] -= 1
    reg_df2: DataFrame = reg_df.merge(csmar_pro_df2, on=['ProvinceCode', const.YEAR], how='left').merge(
        csmar_stock_df, on=['Stkcd', const.YEAR], how='left').merge(
        csmar_stock_df2, on=['Stkcd', const.YEAR], how='left', suffixes=['', '_1'])
    reg_df2.loc[:, 'Province'] = reg_df2['Province_x'].fillna(reg_df2['Province_y'])
    reg_df3: DataFrame = reg_df2.loc[reg_df2[const.YEAR].apply(lambda x: 1999 < x < 2021)].drop(
        ['Province_x', 'Province_y'], axis=1).replace([np.inf, -np.inf], np.nan)

    reg_df3.to_pickle(os.path.join(const.TEMP_PATH, '20210401_china_policy_regression_data.pkl'))
    reg_df3.to_stata(os.path.join(const.OUTPUT_PATH, '20210401_china_policy_regression_data.dta'), version=119,
                     write_index=False)
