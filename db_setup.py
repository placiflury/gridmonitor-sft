#!/usr/bin/env python
# -*- coding: utf-8  -*-

"""
Create SFT dababase tables. To be run after a fresh installation.
"""
import logging
import logging.config

from sqlalchemy import engine_from_config

from sft.utils import init_config
import sft.utils.config_parser as config_parser
from sft.db import init_model

if __name__ == "__main__":
    
    logging.config.fileConfig("./logging.conf")
    log = logging.getLogger(__name__)
    init_config('./config.ini') 

    try:
        sft_engine = engine_from_config(config_parser.config.get(), 'sqlalchemy_sft.')
        init_model(sft_engine)
        log.info("Session object to local database created")
    except Exception, e:
        log.error("Session object to local database failed: %r", e)
    
    
