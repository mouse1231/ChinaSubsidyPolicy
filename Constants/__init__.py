#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: __init__.py
# @Date: 2021/1/10
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

from .path_info import PathInfo


class Constants(PathInfo):
    PROVINCE_NAME = 'province_name'
    CITY_NAME = 'city_name'
    YEAR = 'year'

    HAS_POLICY = 'has_policy'
