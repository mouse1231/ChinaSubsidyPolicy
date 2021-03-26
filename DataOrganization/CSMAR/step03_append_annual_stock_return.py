#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step03_append_annual_stock_return
# @Date: 2021/3/26
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.CSMAR.step03_append_annual_stock_return
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file

if __name__ == '__main__':
    annual_stock_return: DataFrame = read_CSMAR_data_file(os.path.join(const.DATABASE_PATH, 'CSMAR', '股票市场交易',
                                                                       '年个股回报率文件183517048.zip'))
    annual_sr_df: DataFrame = annual_stock_return.loc[:, ['Stkcd', 'Trdynt', 'Yretnd']].rename(
        columns={'Trdynt': const.YEAR})
    annual_sr_df.loc[:, 'year'] = annual_sr_df[const.YEAR].astype(int)

    csmar_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20210326_csmsr_5_database.pkl'))
    csmar_df.loc[:, 'Stkcd'] = csmar_df['Stkcd'].astype(str).str.zfill(6)
    csmar_df2: DataFrame = csmar_df.merge(annual_sr_df, on=['Stkcd', const.YEAR], how='left')
    csmar_df2.to_pickle(os.path.join(os.path.join(const.TEMP_PATH, '20210326_csmsr_6_database.pkl')))
