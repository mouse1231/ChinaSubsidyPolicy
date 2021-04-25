#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step09_construct_emp_distribution_data
# @Date: 2021/4/20
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m DataOrganization.CSMAR.step09_construct_emp_distribution_data
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from Constants import Constants as const
from Utilities.csmar_related import read_CSMAR_data_file

if __name__ == '__main__':
    CSMAR_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '治理结构')
    cor_govern_df: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '治理综合信息文件161445206.zip'))
    cor_govern_df.loc[:, 'Stkcd'] = cor_govern_df['Stkcd'].astype(str).str.zfill(6)
    cor_govern_df.loc[:, const.YEAR] = cor_govern_df['Annodt'].str[:4].astype(int)
    useful_vars = ['Stkcd', const.YEAR, 'Y0601b']
    emp_num: DataFrame = cor_govern_df[useful_vars].copy()

    CSMAR_PATH = os.path.join(const.DATABASE_PATH, 'CSMAR', '社会责任')
    emp_distribution: DataFrame = read_CSMAR_data_file(os.path.join(CSMAR_PATH, '上市公司公司人员分布情况表211755229.zip'))
    emp_deg_df: DataFrame = emp_distribution.loc[emp_distribution['EmployStructureID'].isin({'P5703'})].copy()
    emp_deg_df.loc[emp_deg_df['Unit'] == '万人', 'Amount'] *= 10000
    emp_deg_df.loc[:, const.YEAR] = emp_deg_df['EndDate'].str[:4].astype(int)
    emp_deg_df.loc[:, 'Stkcd'] = emp_deg_df['Symbol'].astype(str).str.zfill(6)
    possible_keys = ['研究生', '博士', '硕士']
    detail_keys = emp_deg_df['EmployDetail'].drop_duplicates()
    valid_keys = set()
    for i in tqdm(detail_keys):
        for j in possible_keys:
            if j in i:
                valid_keys.update(i)
                break
    emp_deg_df_high: DataFrame = emp_deg_df.loc[emp_deg_df['EmployDetail'].isin(valid_keys)].copy()
    emp_deg_df_high2: DataFrame = emp_deg_df_high.groupby(['Stkcd', const.YEAR])['Amount'].sum().reset_index(drop=False)

    # calculate total employee number
    total_key = ['总计', '合计', '汇总', '人员总数', '总数', '人数合计', '总人数', '员工总数', '合计\x7f', '合计人数',
                 '在职员工总数']
    total_emp_df = DataFrame(columns=['Stkcd', const.YEAR, 'EmployeeNum'])
    for stk_cd in emp_deg_df['Stkcd'].drop_duplicates():
        tmp_df: DataFrame = emp_deg_df.loc[emp_deg_df['Stkcd'] == stk_cd].copy()
        for year in tmp_df[const.YEAR].drop_duplicates():
            tmp_df2: DataFrame = tmp_df.loc[tmp_df[const.YEAR] == year].copy()
            if tmp_df2.loc[tmp_df2['EmployDetail'].isin(total_key)].empty:
                total_emp = tmp_df2['Amount'].sum()
            else:
                total_emp = tmp_df2.loc[tmp_df2['EmployDetail'].isin(total_key)].iloc[0]['Amount']
            total_emp_df.loc[total_emp_df.shape[0]] = pd.Series(
                {'Stkcd': stk_cd, const.YEAR: year, 'EmployeeNum': total_emp})

    # information modify
    modified_dict = {21494: 1401, 21493: 1303, }
    for index in modified_dict:
        emp_deg_df_high2.loc[index, 'Amount'] = modified_dict[index]

    emp_high_df: DataFrame = emp_deg_df_high2.merge(total_emp_df, on=['Stkcd', const.YEAR], suffixes=['', '_total'])
    emp_high_df2: DataFrame = emp_high_df.rename(
        columns={'Amount': 'PMEmployeeNum', 'Amount_total': 'EmployeeNum'})
    emp_high_df2.loc[:, 'EmployeeNum'] = emp_high_df2['EmployeeNum'].replace({0: np.nan})
    emp_high_df2.loc[:, 'PM_Ratio'] = emp_high_df2['PMEmployeeNum'] / emp_high_df2['EmployeeNum']

    modify_list = ['640\t189\t832', '641\t173\t837', '642\t170\t679', '643\t136\t728', '644\t169\t691', '645\t114\t371',
                   '1441\t304\t498', '1442\t375\t599', '2841\t9\t2047', '3198\t36\t11325', '4227\t56\t134',
                   '5153\t521\t4057', '5154\t573\t4776', '5155\t731\t5329', '5156\t927\t6310', '6965\t32\t565',
                   '9819\t152\t934', '14550\t19\t278', '14551\t17\t342', '14552\t27\t466', '17590\t236\t5048',
                   '18059\t1203\t1642', '18060\t1473\t1976', '18061\t1410\t1915', '18063\t988\t1443',
                   '18064\t1082\t1462', '19242\t12\t2426', '20022\t11\t173', '20940\t271\t1414', '21616\t4573\t9784',
                   '21819\t33\t136', '21883\t10\t1640', '22315\t41\t2574', '523\t23319\t74773', '524\t21954\t68240',
                   '22084\t12535\t57045', '22474\t6585\t17730', '22475\t7472\t21744', '13758\t5800\t15908',
                   '21418\t10567\t285405', '21780\t13827\t548355', '21779\t12982\t552810', '21768\t6742\t45618',
                   '21733\t13365\t124457', '21730\t9714\t118765', '21729\t8506\t115179', '21728\t7289\t103357',
                   '21727\t6199\t100874', '21726\t5353\t94629', '21676\t12289\t168600', '21293\t27093\t473691',
                   '21294\t28936\t464011', '21615\t3440\t8826', '21614\t2698\t7591']

    for i in modify_list:
        index, pm, emp = (int(j) for j in i.split('\t'))
        emp_high_df2.loc[index, 'PMEmployeeNum'] = pm
        emp_high_df2.loc[index, 'EmployeeNum'] = emp
    emp_high_df2.loc[:, 'PM_Ratio'] = emp_high_df2['PMEmployeeNum'] / emp_high_df2['EmployeeNum']

    emp_high_df2.to_pickle(os.path.join(const.TEMP_PATH, '20210420_employee_distribution_info.pkl'))
