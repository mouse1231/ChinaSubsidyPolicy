#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_merge_all_csmar_files
# @Date: 2021/3/24
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
Merge useful files and create some control variables

balance_sheet
basic_information
income_statement
financial_indicators
government_subsidy

python -m DataOrganization.CSMAR.step02_merge_all_csmar_files
"""

import os

import pandas as pd
from pandas import DataFrame

from Constants import Constants as const


def reformat_government_subsidy_data(df, new_name='government_subsidy', last_name='last_subsidy',
                                     current_name='current_subsidy'):
    result_series = pd.Series(name=new_name, dtype=float)

    start_year = df.year.min()
    end_year = df.year.max()

    result_series.loc[start_year - 1] = df.loc[df[const.YEAR] == start_year, last_name].iloc[0]

    for year in range(start_year, end_year + 1):
        if not df.loc[df[const.YEAR] == year].empty:
            result_series.loc[year] = df.loc[df[const.YEAR] == year, current_name].iloc[0]

        elif not df.loc[df[const.YEAR] == year + 1].empty:
            result_series.loc[year] = df.loc[df[const.YEAR] == year + 1, last_name].iloc[0]

    result_series.index.name = const.YEAR
    return result_series


if __name__ == '__main__':
    CSMAR_PICKLE_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', 'pickle_file')
    balance_sheet: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'balance_sheet.pkl'))
    basic_information: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'basic_information.pkl'))
    government_subsidy: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'government_subsidy.pkl'))
    income_statement: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'income_statement.pkl'))
    financial_indicators: DataFrame = pd.read_pickle(os.path.join(CSMAR_PICKLE_PATH, 'financial_indicators.pkl'))

    # organize government_subsidy information
    government_subsidy.loc[:, const.YEAR] = government_subsidy['Accper'].dt.year
    government_subsidy2: DataFrame = government_subsidy.groupby('Stkcd').apply(
        reformat_government_subsidy_data).reset_index(drop=False)

    csmar_df: DataFrame = balance_sheet.drop(['Accper', 'Typrep'], axis=1).merge(
        basic_information.rename(columns={'Symbol': 'Stkcd'}), on=['Stkcd', const.YEAR], how='outer').merge(
        government_subsidy2, on=['Stkcd', const.YEAR], how='outer').merge(
        income_statement.drop(['Accper', 'Typrep'], axis=1), on=['Stkcd', const.YEAR], how='outer').merge(
        financial_indicators.drop(['Accper', 'Annodt'], axis=1), on=['Stkcd', const.YEAR], how='outer')

    csmar_df.to_pickle(os.path.join(const.TEMP_PATH, '20210326_csmsr_5_database.pkl'))
