#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: csmar_related
# @Date: 2021/3/26
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

import zipfile
import pandas as pd
from pandas import DataFrame


def read_CSMAR_data_file(file_path):
    z = zipfile.ZipFile(file_path)

    data_file_list = list()
    for file_info in z.filelist:
        if file_info.filename.endswith('.csv'):
            data_file_list.append(pd.read_csv(z.open(file_info.filename), error_bad_lines=False))

        elif file_info.filename.endswith('.xlsx'):
            data_file_list.append(pd.read_excel(z.open(file_info.filename)).iloc[2:].copy())

    if data_file_list:
        return pd.concat(data_file_list, ignore_index=True, sort=False)
    else:
        return DataFrame()
