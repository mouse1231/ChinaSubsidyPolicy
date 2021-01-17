#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: __init__.py
# @Date: 2021/1/10
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

from .path_info import PathInfo


class Constants(PathInfo):
    PROVINCE_NAME = 'province_name'
    CITY_NAME = 'city_name'
    YEAR = 'year'

    HAS_POLICY = 'has_policy'

    PUBLISH_DATE = 'publish_date'
    PUBLISH_YEAR = 'publish_year'
    PUBLISH_MONTH = 'publish_month'
    PUBLISH_DAY = 'publish_day'
    ENFORCEMENT_DATE = 'enforcement_date'
    ENFORCEMENT_YEAR = 'enforcement_year'
    ENFORCEMENT_MONTH = 'enforcement_month'
    ENFORCEMENT_DAY = 'enforcement_day'


class ConstantsWSL(Constants):
    ROOT_PATH = '/mnt/d/wyatc/GoogleDrive/Projects/NewProjects/ChineseGovernmentSubsidies'
    DATA_PATH = os.path.join(ROOT_PATH, 'data')
    TEMP_PATH = os.path.join(ROOT_PATH, 'temp')
    OUTPUT_PATH = os.path.join(ROOT_PATH, 'result')
