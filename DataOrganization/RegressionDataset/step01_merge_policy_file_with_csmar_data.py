#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_merge_policy_file_with_csmar_data
# @Date: 2021/3/29
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.RegressionDataset.step01_merge_policy_file_with_csmar_data
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

if __name__ == '__main__':
    policy_df: DataFrame = pd.read_excel(
        os.path.join(const.DATA_PATH, '20210324_innovation_subsidies_policies_template.xlsx'), skiprows=1)
    policy_df.loc[:, 'ProvinceCode'] = policy_df['ProvinceCode'].astype(str)
    province_policy_df: DataFrame = policy_df.loc[policy_df['city_name'].isnull()].copy()

    ignore_keys = ['help_sme_prereq_detail', 'private_firm_prereq_detail', 'industrial_prereq_detail',
                   'cash_subsidy_prereq_detail', 'specific_fund_prereq_detail', 'pm_subsidy_prereq_detail',
                   'pm_subsidy_amount_detail', 'ProvinceCode', 'Province', 'city_name', 'year', 'file_id', 'url',
                   'cash_subsidy_amount_lower', 'cash_subsidy_amount_upper', 'innovate_subsidy_amount',
                   'finance_subsidy_amount', 'public_finance_support_amount', 'gov_invest_amount']


    def combine_same_year_policy(df):
        if df.shape[0] == 1:
            return df

        tmp_df = df.copy()
        first_index = df.index[0]
        for key in df.keys():
            if key in ignore_keys:
                continue
            tmp_df.loc[first_index, key] = int(df[key].sum() > 0.5)

        return tmp_df.iloc[:1]


    province_group = province_policy_df.groupby(['ProvinceCode', const.YEAR])
    result_dfs = list()
    for key in province_group.groups.keys():
        result_dfs.append(combine_same_year_policy(province_group.get_group(key)))

    result_df: DataFrame = pd.concat(result_dfs, ignore_index=True)

    ctrl_dep_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210329_csmar_dep_ctrl_dataset.pkl'))
    reg_df: DataFrame = ctrl_dep_df.merge(result_df.drop(['Province'], axis=1), on=['ProvinceCode', const.YEAR],
                                          how='left')
    for key in result_df.keys():
        if key in ignore_keys:
            continue

        reg_df.loc[:, key] = reg_df[key].fillna(0)

    drop_keys = ['help_sme_prereq_detail', 'private_firm_prereq_detail', 'industrial_prereq_detail',
                 'cash_subsidy_prereq_detail', 'specific_fund_prereq_detail', 'pm_subsidy_prereq_detail',
                 'pm_subsidy_amount_detail', 'city_name', 'file_id', 'url', 'RegisterAddress', 'FullName', 'CityCode',
                 'cash_subsidy_amount_lower', 'cash_subsidy_amount_upper', 'innovate_subsidy_amount', 'City',
                 'finance_subsidy_amount', 'public_finance_support_amount', 'gov_invest_amount', 'prereq_useless',
                 'ValidityPeriod', 'Currency', '']
    reg_df2: DataFrame = reg_df.drop(drop_keys, axis=1)
    for key in ['ShareholdingRatio', 'Apply', 'Apply1stYr', 'Apply2ndYr', 'Apply3rdYr', 'Apply4thYr', 'ApplyGrant',
                'ApplyTermination', 'ApplyPending', 'ApplyUnGrantDecided', 'IApply', 'IApply1stYr', 'IApply2ndYr',
                'IApply3rdYr', 'IApply4thYr', 'IApplyGrant', 'IApplyTermination', 'IApplyPending', 'IsQualification',
                'IApplyUnGrantDecided', 'UApply', 'UApply1stYr', 'UApply2ndYr', 'UApply3rdYr', 'UApply4thYr',
                'UApplyGrant', 'UApplyTermination', 'UApplyPending', 'UApplyUnGrantDecided', 'DApply', 'DApply1stYr',
                'DApply2ndYr', 'DApply3rdYr', 'DApply4thYr', 'DApplyGrant', 'DApplyTermination', 'DApplyPending',
                'DApplyUnGrantDecided', 'pat_apply_num', 'pat_grant_num',
                'invpat_apply_num', 'invpat_grant_num', 'outpat_apply_num', 'outpat_grant_num', 'utipat_apply_num',
                'utipat_grant_num']:
        reg_df2.loc[:, key] = pd.to_numeric(reg_df2[key])

    for key in ['Province', 'ProvinceCode', 'Stkcd', 'IndustryCode', 'IndustryName', 'Indcd', 'IApplyIPC', 'UApplyIPC',
                'DApplyLocarno']:
        reg_df2.loc[:, key] = reg_df2[key].astype(str)

    reg_df2.to_pickle(os.path.join(const.TEMP_PATH, '20210329_china_policy_reg_file.pkl'))
    reg_df2.to_stata(os.path.join(const.OUTPUT_PATH, '20210329_china_policy_reg_file.dta'), write_index=False,
                     version=119)
