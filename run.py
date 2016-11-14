#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import logging
from datetime import datetime

from hdx.facades.scraperwiki import facade

from worldpop import generate_datasets

logger = logging.getLogger(__name__)


def main(configuration):
    '''Generate dataset and create it in HDX'''

    generate_datasets(configuration)

if __name__ == '__main__':
    facade(main, hdx_site='test')
