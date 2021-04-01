#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step03_winsorize_some_ratio_variables
# @Date: 2021/4/1
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.RegressionDataset.step03_winsorize_some_ratio_variables
"""

import os

import pandas as pd
from tqdm import tqdm
from pandas import DataFrame
from scipy.stats.mstats import winsorize

from Constants import Constants as const

if __name__ == '__main__':
    reg_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210401_china_policy_regression_data.pkl'))

    # winsorize keys
    win_keys = ['T40401_1', 'F100901A_1', 'F053401B_1', 'F053201B_1', 'RDInvestRatio_1',
                'RDInvestNetprofitRatio_1', 'RDSpendSumRatio_1', 'RDPersonRatio_1', 'F101001A_1', 'F011201A_1']
    new_win_keys = list()

    for key in tqdm(win_keys):
        reg_df.loc[:, '{}_win'.format(key)] = winsorize(reg_df[key], limits=(0.01, 0.01))
        reg_df.loc[:, '{}_win'.format(key[:-2])] = winsorize(reg_df[key[:-2]], limits=(0.01, 0.01))

    # fillin missing values
    miss_keys = ['F053401B_1', 'Apply_ln_1', 'ApplyGrant_ln_1', 'ApplyTermination_ln_1', 'ApplyPending_ln_1',
                 'ApplyUnGrantDecided_ln_1', 'IApply_ln_1', 'IApplyGrant_ln_1', 'IApplyTermination_ln_1',
                 'IApplyPending_ln_1', 'IApplyUnGrantDecided_ln_1', 'UApply_ln_1', 'UApplyGrant_ln_1',
                 'UApplyTermination_ln_1', 'DApply_ln_1', 'DApplyGrant_ln_1', 'DApplyTermination_ln_1',
                 'pat_apply_num_ln_1', 'pat_grant_num_ln_1', 'invpat_apply_num_ln_1', 'invpat_grant_num_ln_1',
                 'outpat_apply_num_ln_1', 'outpat_grant_num_ln_1', 'utipat_apply_num_ln_1', 'utipat_grant_num_ln_1',
                 'RDPerson_ln_1', 'RDSpendSum_ln_1', 'RDExpenses_ln_1', 'RDInvest_ln_1', 'RDInvestRatio_1',
                 'RDInvestNetprofitRatio_1', 'RDSpendSumRatio_1', 'RDPersonRatio_1']

    for key in tqdm(miss_keys):
        reg_df.loc[:, '{}_f0'.format(key)] = reg_df[key].fillna(0)
        reg_df.loc[:, '{}_f0'.format(key[:-2])] = reg_df[key[:-2]].fillna(0)
        if '{}_win'.format(key) in reg_df.keys():
            reg_df.loc[:, '{}_win_f0'.format(key)] = reg_df['{}_win'.format(key)].fillna(0)
        if '{}_win'.format(key[:-2]) in reg_df.keys():
            reg_df.loc[:, '{}_win_f0'.format(key[:-2])] = reg_df['{}_win'.format(key[:-2])].fillna(0)

    reg_df.to_pickle(os.path.join(const.TEMP_PATH, '20210401_china_policy_regression_data2.pkl'))
    reg_df.to_stata(os.path.join(const.OUTPUT_PATH, '20210401_china_policy_regression_data2.dta'), version=119,
                    write_index=False)
