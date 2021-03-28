#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step04_append_patent_information
# @Date: 2021/3/26
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
上市公司研发创新 database
2007-

python -m DataOrganization.CSMAR.step04_append_patent_information
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file
from .step02_merge_all_csmar_files import reformat_government_subsidy_data


def construct_patent_count_number(df):
    start_year = int(min(df['pat_apply_year'].min(), df['pat_grant_year'].min(), df['EndDate'].dt.year.min()))
    end_year = int(max(df['pat_apply_year'].max(), df['pat_grant_year'].max(), df['EndDate'].dt.year.max()))

    result_df = DataFrame(columns=[const.YEAR, 'pat_apply_num', 'pat_grant_num',
                                   'invpat_apply_num', 'invpat_grant_num', 'outpat_apply_num', 'outpat_grant_num',
                                   'utipat_apply_num', 'utipat_grant_num'])
    for year in range(start_year, end_year + 1):
        result_dict = {const.YEAR: year}
        for target_year in ['pat_apply_year', 'pat_grant_year']:
            if 'apply' in target_year:
                mid_key = 'apply'
            else:
                mid_key = 'grant'

            tmp_df: DataFrame = df.loc[df[target_year] == year].copy()
            result_dict['pat_{}_num'.format(mid_key)] = tmp_df.shape[0]
            inv_tmp_df: DataFrame = tmp_df.loc[tmp_df['PatentTypeCode'] == 'S4901'].copy()
            result_dict['invpat_{}_num'.format(mid_key)] = inv_tmp_df.shape[0]
            out_tmp_df: DataFrame = tmp_df.loc[tmp_df['PatentTypeCode'] == 'S4902'].copy()
            result_dict['outpat_{}_num'.format(mid_key)] = out_tmp_df.shape[0]
            uti_tmp_df: DataFrame = tmp_df.loc[tmp_df['PatentTypeCode'] == 'S4903'].copy()
            result_dict['utipat_{}_num'.format(mid_key)] = uti_tmp_df.shape[0]

        result_df: DataFrame = result_df.append(result_dict, ignore_index=True)

    result_df.loc[:, 'Stkcd'] = df['Stkcd'].iloc[0]
    return result_df


if __name__ == '__main__':
    # Load data
    RD_INNOVATION_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '上市公司研发创新')
    major_financial_indiators: DataFrame = read_CSMAR_data_file(
        os.path.join(RD_INNOVATION_PATH, '上市公司主要财务指标161627241.zip'))
    # dev_expense_df: DataFrame = read_CSMAR_data_file(
    #     os.path.join(RD_INNOVATION_PATH, '上市公司开发支出162116784.zip'))
    intan_assets: DataFrame = read_CSMAR_data_file(
        os.path.join(RD_INNOVATION_PATH, '上市公司无形资产161921508.zip'))
    patent_info: DataFrame = read_CSMAR_data_file(os.path.join(RD_INNOVATION_PATH, '专利明细情况161319699.zip'))
    # patent_apply_info: DataFrame = read_CSMAR_data_file(
    #     os.path.join(RD_INNOVATION_PATH, '国内外专利申请获得情况表161202517.zip'))
    # cash_output: DataFrame = read_CSMAR_data_file(os.path.join(RD_INNOVATION_PATH,
    #                                                            '支付的其他与经营活动有关的现金161719753.zip'))
    # gov_subsidy: DataFrame = read_CSMAR_data_file(os.path.join(RD_INNOVATION_PATH, '政府补助161743594.zip'))
    rd_expense: DataFrame = read_CSMAR_data_file(os.path.join(RD_INNOVATION_PATH, '研发投入情况表162054225.zip'))

    # organize firm information file
    major_financial_indiators.loc[:, 'EndDate'] = pd.to_datetime(major_financial_indiators['EndDate'],
                                                                 format='%Y-%m-%d')
    major_financial_indiators.loc[:, const.YEAR] = major_financial_indiators['EndDate'].dt.year

    firm_info: DataFrame = major_financial_indiators.rename(columns={'Symbol': 'Stkcd'}).drop(['EndDate'], axis=1)
    firm_info.loc[:, 'Stkcd'] = firm_info['Stkcd'].astype(str).str.zfill(6)

    # organize firm development expense file
    # dev_expense_df2: DataFrame = dev_expense_df.loc[dev_expense_df['DevItem'] == '合计'].copy()
    # dev_expense_df2.loc[:, const.YEAR] = dev_expense_df2['Accper'].str[:4].astype(int)

    # organize intangible assets information
    intan_assets2: DataFrame = intan_assets.drop([26176, 26177, 26178, 61216, 61217, 80746, 80747], axis=0).dropna(
        subset=['Accper'], how='any')
    intan_assets2.loc[:, 'Stkcd'] = intan_assets2['Stkcd'].astype(str).str.zfill(6)
    intan_assets2.loc[:, const.YEAR] = intan_assets2['Accper'].str[:4].astype(int)
    intan_assets3: DataFrame = intan_assets2.loc[
        (intan_assets2['InvAsectItm'] == '合计') & (intan_assets2['Category'] == 4)].drop(
        ['Source', 'Typrep', 'Category', 'Explanation', 'Accper', 'InvAsectItm'], axis=1)

    intan_assets3.loc[:, 'NetIntGain'] = intan_assets3['EndValue'] - intan_assets3['BeginValue']

    intan_assets4: DataFrame = intan_assets3.groupby('Stkcd').apply(
        reformat_government_subsidy_data, current_name='EndValue', new_name='IntAT',
        last_name='BeginValue').reset_index(drop=False)
    intan_assets5: DataFrame = intan_assets4.merge(
        intan_assets3.loc[:, ['Stkcd', const.YEAR, 'NetIntGain']].drop_duplicates(subset=['Stkcd', const.YEAR]),
        on=['Stkcd', const.YEAR], how='left')

    # construct patent information data
    patent_info.loc[:, 'ApplicationDate'] = pd.to_datetime(patent_info['ApplicationDate'], format='%Y-%m-%d')
    patent_info.loc[:, 'GrantDate'] = pd.to_datetime(patent_info['GrantDate'], format='%Y-%m-%d')
    patent_info.loc[:, 'EndDate'] = pd.to_datetime(patent_info['EndDate'], format='%Y-%m-%d')
    patent_info.loc[:, 'pat_apply_year'] = patent_info['ApplicationDate'].dt.year.fillna(
        patent_info.loc[:, 'EndDate'].dt.year)
    patent_info.loc[:, 'pat_grant_year'] = patent_info['GrantDate'].dt.year
    patent_info2: DataFrame = patent_info.rename(columns={'Symbol': 'Stkcd'})
    patent_info2.loc[:, 'Stkcd'] = patent_info2.loc[:, 'Stkcd'].astype(str).str.zfill(6)

    patent_info3: DataFrame = patent_info2.groupby('Stkcd').apply(construct_patent_count_number).reset_index(drop=True)

    rd_expense.loc[:, const.YEAR] = rd_expense['EndDate'].str[:4].astype(int)
    rd_expense2: DataFrame = rd_expense.rename(columns={'Symbol': 'Stkcd'}).drop(
        ['EndDate', 'StateTypeCode', 'Explanation'], axis=1).drop_duplicates(subset=['Stkcd', const.YEAR])
    rd_expense2.loc[:, 'Stkcd'] = rd_expense2['Stkcd'].astype(str).str.zfill(6)

    innovation_df: DataFrame = firm_info.merge(intan_assets5, on=['Stkcd', const.YEAR], how='outer').merge(
        patent_info3, on=['Stkcd', const.YEAR], how='outer').merge(rd_expense2, on=['Stkcd', const.YEAR], how='outer')
    innovation_df.to_pickle(os.path.join(const.TEMP_PATH, '20210328_csmar_china_innovation_dataset.pkl'))
    csmar_df: DataFrame = pd.read_pickle(os.path.join(os.path.join(const.TEMP_PATH, '20210326_csmsr_6_database.pkl')))
    csmar_df2: DataFrame = csmar_df.merge(innovation_df.drop(['Currency'], axis=1), on=['Stkcd', const.YEAR],
                                          how='left')
    csmar_df2.loc[:, 'IndustryCode'] = csmar_df2['IndustryCode_x'].fillna(csmar_df2['IndustryCode_y'])
    csmar_df2.loc[:, 'IndustryName'] = csmar_df2['IndustryName_x'].fillna(csmar_df2['IndustryName_y'])
    csmar_df2.drop(['IndustryCode_x', 'IndustryCode_y', 'IndustryName_x', 'IndustryName_y'], axis=1).to_pickle(
        os.path.join(const.TEMP_PATH, '20210328_csmar_10_database.pkl'))
