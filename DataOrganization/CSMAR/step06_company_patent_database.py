#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step06_company_patent_database
# @Date: 2021/3/29
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.CSMAR.step06_company_patent_database
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file

if __name__ == '__main__':
    CSMAR_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '上市公司与子公司专利')
    application_info: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '专利申请情况表212058512.zip'))
    firm_information: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '公司基本信息表212024408.zip'))
    application_info.loc[:, const.YEAR] = application_info['EndDate'].str[:4].astype(int)
    application_info.loc[:, 'Stkcd'] = application_info['Symbol'].astype(str).str.zfill(6)
    application_info2: DataFrame = application_info.loc[application_info['ClassifySign'] == 7].drop(
        ['EndDate', 'FullNameOrType', 'ClassifySign', 'NameAttribute', 'IApplyCityCode', 'IApplyCity', 'Symbol',
         'UApplyCityCode', 'UApplyCity', 'DApplyCityCode', 'DApplyCity', 'MergeSign'], axis=1).drop_duplicates(
        subset=['Stkcd', const.YEAR], keep='last')

    firm_information.loc[:, const.YEAR] = firm_information['EndDate'].str[:4].astype(int)
    firm_information.loc[:, 'Stkcd'] = firm_information['Symbol'].astype(str).str.zfill(6)
    firm_information2: DataFrame = firm_information.drop(
        ['Symbol', 'EndDate', 'ClassifySign', 'Board'], axis=1).drop_duplicates(
        subset=['Stkcd', const.YEAR], keep='last')

    firm_type_ids = firm_information2['ProjectTypeID'].dropna().drop_duplicates()
    firm_type_id_set = set()
    for i in firm_type_ids:
        firm_type_id_set.update(i.split(','))

    for key in firm_type_id_set:
        firm_information2.loc[:, 'is_{}'.format(key)] = 0
        firm_information2.loc[
            firm_information2['ProjectTypeID'].apply(lambda x: isinstance(x, str) and key in x), 'is_{}'.format(
                key)] = 1

    firm_information3: DataFrame = firm_information2.drop(['ProjectTypeID', 'ProjectType'], axis=1)
    firm_inno_df: DataFrame = firm_information3.merge(application_info2, on=['Stkcd', const.YEAR], how='outer')
    for key in ['ProvinceCode', 'Province']:
        firm_inno_df.loc[:, key] = firm_inno_df.groupby('Stkcd')[key].bfill().ffill()

    firm_inno_df.to_pickle(os.path.join(const.TEMP_PATH, '20210329_firm_innovation_data.pkl'))
