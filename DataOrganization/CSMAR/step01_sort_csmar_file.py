#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_sort_csmar_file
# @Date: 2021/1/10
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python -m DataOrganization.CSMAR.step01_sort_csmar_file
"""

import os

import zipfile
import numpy as np
import pandas as pd
from pandas import DataFrame

from Constants import Constants as const

rename_dict = {
    '研发费用160230988.zip': {
        'column_name': {'FN_Fn06002': 'current_rd_expense', 'FN_Fn06003': 'last_rd_expense'},
        'data_keys': ['current_rd_expense', 'last_rd_expense'],
        'save_filename': 'rd_expense.pkl', 'sum_key': 'FN_Fn06001'},
    '资产负债表160530050.zip': {
        'column_name': dict(),
        'data_keys': np.nan,
        'save_filename': 'balance_sheet.pkl', 'sum_key': np.nan},
    '利润表160748301.zip': {
        'column_name': dict(),
        'data_keys': np.nan,
        'save_filename': 'income_statement.pkl', 'sum_key': np.nan}}

if __name__ == '__main__':
    # organize subsidize file
    z = zipfile.ZipFile(os.path.join(const.DATABASE_PATH, 'CSMAR', '政府补助160121870.zip'))
    data_df = DataFrame()
    for file_info in z.filelist:
        if file_info.filename.endswith('.csv'):
            data_df: DataFrame = pd.read_csv(z.open(file_info.filename), error_bad_lines=False)
            break

    data_df2: DataFrame = data_df.rename(columns={data_df.keys()[0]: 'Stkcd', 'Fn05602': 'current_subsidy',
                                                  'Fn05603': 'last_subsidy'}).loc[data_df['Fn05601'] == '合计'].copy()
    data_df2.loc[:, 'Accper'] = pd.to_datetime(data_df2['Accper'], format='%Y-%m-%d')
    for key in ['current_subsidy', 'last_subsidy']:
        data_df2.loc[:, key] = pd.to_numeric(data_df2[key])
    data_df3: DataFrame = data_df2.groupby(['Stkcd', 'Accper']).sum()[['current_subsidy', 'last_subsidy']].reset_index(
        drop=False)
    data_df3.to_pickle(os.path.join(const.DATABASE_PATH, 'CSMAR', 'pickle_file', 'government_subsidy.pkl'))

    for root, dirs, files in os.walk(os.path.join(const.DATABASE_PATH, 'CSMAR')):
        for file_name in files:
            if file_name not in rename_dict:
                continue

            # organize rd expense file
            z = zipfile.ZipFile(os.path.join(root, file_name))
            data_df = DataFrame()
            for file_info in z.filelist:
                if file_info.filename.endswith('.csv'):
                    data_df: DataFrame = pd.read_csv(z.open(file_info.filename),
                                                     error_bad_lines=False, warn_bad_lines=False).rename(
                        columns=rename_dict[file_name]['column_name'])
                    break

            if not np.isnan(rename_dict[file_name]['sum_key']):
                data_df: DataFrame = data_df.loc[data_df[rename_dict[file_name]['sum_key']] == '合计'].copy()

            data_df.loc[:, 'Accper'] = pd.to_datetime(data_df['Accper'], format='%Y-%m-%d')
            data_df.loc[:, const.YEAR] = data_df['Accper'].dt.year
            if not np.isnan(rename_dict[file_name]['data_keys']):
                data_df: DataFrame = data_df.groupby(['Stkcd', 'Accper']).sum()[
                    rename_dict[file_name]['data_keys']].reset_index(drop=False)
            else:
                data_df: DataFrame = data_df.drop_duplicates(subset=['Stkcd', const.YEAR], keep='last')

            data_df.to_pickle(
                os.path.join(const.DATABASE_PATH, 'CSMAR', 'pickle_file', rename_dict[file_name]['save_filename']))

    z = zipfile.ZipFile(os.path.join(const.DATABASE_PATH, 'CSMAR', '上市公司基本信息', '上市公司基本信息年度表212011796.zip'))
    data_df = DataFrame()
    for file_info in z.filelist:
        if file_info.filename.endswith('.csv'):
            data_df: DataFrame = pd.read_csv(z.open(file_info.filename),
                                             error_bad_lines=False, warn_bad_lines=False)
            break

    key_zip_dict = {16843: '518014', 16845: '518031', 16847: '518029', 16848: '130011', 21534: '100031',
                    21699: '510403', 29584: '519000', 36404: '200120', 38551: '200051', 38571: '200233',
                    38609: '421005', 19145: '518103', 23043: '404000', 33082: '850000', 11304: '541004',
                    11614: '226500', 12835: '518057', 27524: '113001', 27525: '113001', 27526: '113001',
                    27527: '113001', 270: '518001', 271: '518001', 272: '518001', 1544: '816000', 5471: '570203',
                    17943: '618400', 22421: '200063', 22422: '200063', 26364: '610207', 26972: '330096',
                    31301: '610044', 33606: '050031'}

    for index_id in key_zip_dict:
        data_df.loc[index_id, 'Zipcode'] = key_zip_dict[index_id]

    # ZIPCODE to province information
    zip_to_province = {'51': 'GUANGDONG', '10': 'BEIJING', '13': 'JILIN', '22': 'JIANGSU', '12': 'LIAONING',
                       '23': 'ANHUI', '61': 'SICHUAN', '41': 'HUNAN', '31': 'ZHEJIANG', '05': 'HEBEI', '83': 'XINJIANG',
                       '21': 'JIANGSU', '52': 'GUANGDONG', '26': 'SHANDONG', '46': 'HENAN', '06': 'HEBEI',
                       '03': 'SHANXI', '33': 'JIANGXI', '25': 'SHANDONG', '81': 'QINGHAI', '11': 'LIAONING',
                       '44': 'HUBEI', '02': 'NEIMENGGU', '42': 'HUNAN', '43': 'HUBEI', '04': 'SHANXI', '57': 'HAINAN',
                       '20': 'SHANGHAI', '40': 'CHONGQING', '71': 'SHAN3XI', '47': 'HENAN', '36': 'FUJIAN',
                       '54': 'GUANGXI', '35': 'FUJIAN', '30': 'TIANJIN', '65': 'YUNNAN', '55': 'GUIZHOU', '45': 'HENAN',
                       '73': 'GANSU', '27': 'SHANDONG', '75': 'NINGXIA', '72': 'SHAN3XI', '64': 'SICHUAN',
                       '62': 'SICHUAN', '53': 'GUANGXI', '15': 'HEILONGJIANG', '01': 'NEIMENGGU', '24': 'ANHUI',
                       '84': 'XINJIANG', '07': 'HEBEI', '32': 'ZHEJIANG', '85': 'XIZANG', '63': 'SICHUAN',
                       '34': 'JIANGXI', '66': 'YUNNAN', '16': 'HEILONGJIANG', '56': 'GUIZHOU', '67': 'YUNNAN',
                       '74': 'GANSU', '86': 'XIZANG'}

    data_df.loc[:, 'province'] = data_df['Zipcode'].str[:2].replace(zip_to_province)
    data_df.loc[:, const.YEAR] = data_df['EndDate'].str[:4].astype(int)
    data_df.to_pickle(os.path.join(const.DATABASE_PATH, 'pickle_file', 'basic_information.pkl'))
