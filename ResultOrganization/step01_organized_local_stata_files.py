#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_organized_local_stata_files
# @Date: 2021/4/6
# @Author: Mark Wang
# @Email: markwang@connect.hku.hk

"""
python -m ResultOrganization.step01_organized_local_stata_files
"""

import os

from tqdm import tqdm

from Constants import ConstantsWIN10 as const

if __name__ == '__main__':
    result_path = os.path.join(const.REG_RESULT_PATH, '20210401')

    file_info_list = ['has_policy', 'high_tech_firm', 'high_tech_firm_clear', 'high_tech_product',
                      'high_tech_product_clear', 'top_firm', 'top_firm_clear', 'innovate_firm', 'innovate_firm_clear',
                      'help_sme', 'help_sme_prereq', 'help_sme_pre_clear', 'help_sme_favor', 'help_sme_clear',
                      'help_sme_prereq_detail', 'private_firm', 'private_firm_prereq', 'private_firm_prereq_detail',
                      'industrial', 'industrial_prereq', 'industrial_prereq_detail', 'innovate_subsidy',
                      'innovate_subsidy_prereq', 'innovate_subsidy_prereq_clear', 'innovate_subsidy_amount',
                      'cash_subsidy', 'cash_subsidy_prereq', 'cash_subsidy_prereq_clear', 'cash_subsidy_amount_clear',
                      'cash_subsidy_prereq_detail', 'cash_subsidy_amount_lower', 'cash_subsidy_amount_upper',
                      'RD_subsidy', 'RD_subsidy_prereq', 'RD_subsidy_prereq_clear', 'RD_subsidy_amount_clear',
                      'pre_RD_subsidy', 'num_tech_firm', 'tax_favor', 'corp_tax_favor', 'val_tax_favor',
                      'bus_tax_favor', 'tax_favor_amount_clear', 'social_care_subsidy',
                      'social_care_subsidy_amount_clear', 'pretax_deduct', 'pretax_deduct_prereq',
                      'pretax_deduct_prereq_clear', 'benefit_policy_clear', 'finance_subsidy', 'finance_subsidy_amount',
                      'public_finance_support', 'prereq_useless', 'public_finance_support_amount', 'ppn_subsidy',
                      'ppn_subsidy_amount_clear', 'loan_subsidy', 'loan_subsidy_prereq', 'loan_subsidy_prereq_clear',
                      'loan_subsidy_amount_clear', 'secured_fina_subsidy', 'secured_fina_subsidy_amount_clear',
                      'interest_subsidy', 'interest_subsidy_prereq', 'interest_subsidy_prereq_clear',
                      'interest_subsidy_amount_clear', 'gov_invest_amount', 'gov_invest_ddl', 'ip_support',
                      'ip_support_amount_clear', 'tech_incubator', 'tech_incubator_amount_clear',
                      'pub_service_platform', 'pub_service_platform_prereq', 'pub_service_platform_prereq_clear',
                      'pub_service_platform_amount_clear', 'country_project_support', 'patent_support',
                      'patent_support_prereq', 'patent_support_prereq_clear', 'patent_support_amount_clear',
                      'specific_fund', 'specific_fund_prereq', 'specific_fund_prereq_clear',
                      'specific_fund_amount_clear', 'specific_fund_prereq_detail', 'pm_subsidy', 'pm_subsidy_prereq',
                      'pm_subsidy_prereq_clear', 'pm_subsidy_prereq_detail', 'pm_subsidy_amount_detail']

    result_file_path = os.path.join(const.REG_RESULT_PATH, '20210401_regression_results.txt')
    with open(result_file_path, 'w') as f:
        for f_name in tqdm(file_info_list):
            file_full_path = os.path.join(result_path, '{}.txt'.format(f_name))
            if not os.path.isfile(file_full_path):
                continue
            result_f = open(os.path.join(result_path, '{}.txt'.format(f_name)))
            lines = result_f.readlines()

            if f_name == file_info_list[0]:
                for line in lines[:5]:
                    f.write(line)
            else:
                for line in lines[3:5]:
                    f.write(line)
            result_f.close()
