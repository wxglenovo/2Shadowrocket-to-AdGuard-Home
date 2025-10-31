#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 单分片验证调用 split_and_check_16.py 的 process_part

import sys
from split_and_check_16 import process_part

if __name__=="__main__":
    if len(sys.argv)<2:
        print("Usage: python validate.py <part_number>")
        sys.exit(1)
    part=int(sys.argv[1])
    process_part(part)
