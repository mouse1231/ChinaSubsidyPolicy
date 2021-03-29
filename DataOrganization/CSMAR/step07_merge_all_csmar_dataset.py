#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step07_merge_all_csmar_dataset
# @Date: 2021/3/29
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

import os

from tqdm import tqdm
import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

if __name__ == '__main__':
    CSMAR_PICKLE_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', 'pickle_file')
    financial_indicators: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'financial_indicators.pkl'))
    financial_analysis: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20210328_csmar_financial_analysis_dataset.pkl'))
    firm_inno_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210329_firm_innovation_data.pkl'))
    innovation_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20210328_csmar_china_innovation_dataset.pkl'))

    financial_indicators.loc[:, 'Stkcd'] = financial_indicators['Stkcd'].astype(str).str.zfill(6)
    financial_indicators2: DataFrame = financial_indicators.drop(['Accper', 'Annodt'], axis=1)

    # merge two innovation related database
    firm_df: DataFrame = firm_inno_df.merge(innovation_df, on=['Stkcd', const.YEAR], how='outer')
    for key in ['IndustryCode', 'IndustryName']:
        firm_df.loc[:, key] = firm_df['{}_x'.format(key)].fillna(firm_df['{}_y'.format(key)])
        firm_df.loc[:, key] = firm_df.groupby('Stkcd')[key].ffill().bfill()
        firm_df: DataFrame = firm_df.drop(['{}_x'.format(key), '{}_y'.format(key)], axis=1)

    for key in ['Province', 'ProvinceCode']:
        firm_df.loc[:, key] = firm_df.groupby('Stkcd')[key].ffill().bfill()

    firm_df2: DataFrame = firm_df.merge(financial_indicators2, on=['Stkcd', const.YEAR], how='left').merge(
        financial_analysis, on=['Stkcd', const.YEAR], how='left')

    firm_df2.loc[:, 'Indcd'] = firm_df2['Indcd'].fillna(firm_df2['IndustryCode'])
    firm_df2.loc[49310, 'Province'] = '广东省'
    firm_df2.loc[49310, 'ProvinceCode'] = '440000'

    # prepare some log values
    log_key = ['A100000', 'Capexp', 'Nstaff', 'F100801A', 'F100802A', 'Apply', 'Apply1stYr', 'Apply2ndYr', 'Apply3rdYr',
               'Apply4thYr', 'ApplyGrant', 'ApplyTermination', 'ApplyPending', 'ApplyUnGrantDecided', 'IApply',
               'IApply1stYr', 'IApply2ndYr', 'IApply3rdYr', 'IApply4thYr', 'IApplyGrant', 'IApplyTermination',
               'IApplyPending', 'IApplyUnGrantDecided', 'UApply', 'UApply1stYr', 'UApply2ndYr', 'UApply3rdYr',
               'UApply4thYr', 'UApplyGrant', 'UApplyTermination', 'UApplyPending', 'UApplyUnGrantDecided', 'DApply',
               'DApply1stYr', 'DApply2ndYr', 'DApply3rdYr', 'DApply4thYr', 'DApplyGrant', 'DApplyTermination',
               'DApplyPending', 'DApplyUnGrantDecided', 'pat_apply_num', 'pat_grant_num', 'invpat_apply_num',
               'invpat_grant_num', 'outpat_apply_num', 'outpat_grant_num', 'utipat_apply_num', 'utipat_grant_num',
               'RDPerson', 'RDSpendSum', 'RDExpenses', 'RDInvest']

    lead_key = ['T30100', 'T40401', 'T60200', 'F100901A', 'F100902A', 'F100903A', 'F100904A', 'F101001A', 'F101002A',
                'F011201A', 'F011301A', 'F020108', 'F020109', 'F032801B', 'F030801A', 'F030901A', 'F031001A',
                'F032601B', 'F032701B', 'F050101B', 'F050102B', 'F050103B', 'F050201B', 'F050202B', 'F050203B',
                'F053202B', 'F053401B', 'F061701B', 'RDPersonRatio', 'RDSpendSumRatio', 'RDInvestRatio',
                'RDInvestNetprofitRatio']
    for key in tqdm(log_key):
        firm_df2.loc[:, '{}_ln'.format(key)] = firm_df2[key].apply(lambda x: np.log(x + 1))
        lead_key.append('{}_ln'.format(key))

    firm_df3: DataFrame = firm_df2.sort_values(by=['Stkcd', const.YEAR], ascending=True)
    for key in tqdm(lead_key):
        firm_df3.loc[:, '{}_1'.format(key)] = firm_df3.groupby('Stkcd')[key].shift(-1)

    firm_df3.to_pickle(os.path.join(const.TEMP_PATH, '20210329_csmar_dep_ctrl_dataset.pkl'))
