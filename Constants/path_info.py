#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: path_info
# @Date: 2021/1/10
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os


class PathInfo(object):
    ROOT_PATH = '/home/zigan/Documents/wangyouan/research/ChinaPolicyAnalysis'
    DATA_PATH = os.path.join(ROOT_PATH, 'data')
    TEMP_PATH = os.path.join(ROOT_PATH, 'temp')
    OUTPUT_PATH = os.path.join(ROOT_PATH, 'output')

    DATABASE_PATH = '/home/zigan/Documents/wangyouan/database'
