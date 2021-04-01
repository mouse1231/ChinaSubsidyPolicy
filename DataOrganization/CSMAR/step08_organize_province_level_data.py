#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step08_organize_province_level_data
# @Date: 2021/3/29
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.CSMAR.step08_organize_province_level_data
"""

import os

from tqdm import tqdm
from pandas import DataFrame

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file

if __name__ == '__main__':
    CSMAR_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '区域经济')
    useful_file_list = ['分省份职工平均工资及指数115303179.zip', '分省份按城乡分就业人员数115230217.zip',
                        '分省份按城乡分全社会固定资产投资115420148.zip', '分省份按经济类型分全社会固定资产投资115436186.zip',
                        '分省份按三次产业分就业人员数115218163.zip', '分省份按用途分商品房屋平均销售价格115539152.zip',
                        '分省份按用途分商品房屋销售面积115527143.zip', '分省份财政金融文件（二）130029235.zip',
                        '分省份财政金融文件（一）130105364.zip', '分省份城镇居民家庭平均每人全年收入来源125711164.zip',
                        '分省份城镇居民家庭平均每人全年消费性支出125758299.zip', '分省份大中型工业企业主要经济效益指标132712161.zip',
                        '分省份大中型工业企业主要指标132619169.zip', '分省份固定资产投资价格指数130453120.zip',
                        '分省份国民生产总值115028299.zip', '分省份居民消费价格指数和商品零售价格指数130301199.zip',
                        '分省份居民消费水平130003148.zip', '分省份全部工业企业主要经济指标132026186.zip',
                        '分省份全社会固定资产投资资金来源115445164.zip', '分省份人口情况文件115204295.zip',
                        '分省份商品房屋销售情况115509224.zip', '分省份社会消费品零售总额134053172.zip',
                        '分省份最终消费支出及构成125946200.zip']

    result_df = DataFrame()
    for file_name in tqdm(useful_file_list):
        csmar_df = read_CSMAR_data_file(os.path.join(CSMAR_PATH, file_name))
        csmar_df.loc[:, 'Prvcnm_id'] = csmar_df['Prvcnm_id'].astype(str).str.zfill(6)
        csmar_df.loc[:, 'Sgnyea'] = csmar_df['Sgnyea'].astype(int)
        if result_df.empty:
            result_df = csmar_df.copy()
        else:
            result_df: DataFrame = result_df.merge(csmar_df, on=['Prvcnm_id', 'Sgnyea'], how='outer')

            duplicated_keys = [i for i in result_df.keys() if i.endswith('_y')]
            key_to_drop = duplicated_keys[:]
            for key in duplicated_keys:
                x_key = '{}x'.format(key[:-1])
                result_df.loc[:, key[:-2]] = result_df[key].fillna(result_df[x_key])
                key_to_drop.append(x_key)

            result_df: DataFrame = result_df.drop(key_to_drop, axis=1)

    result_df.to_pickle(os.path.join(const.TEMP_PATH, '20210401_csmar_provincial_data.pkl'))
