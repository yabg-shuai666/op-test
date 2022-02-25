#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import liblemon as lemon
import libtables
from pygdbt.common import mem_info

if __name__ == '__main__':
    lemon.inner_main(sys.argv)
    mem_info("pserver end")
