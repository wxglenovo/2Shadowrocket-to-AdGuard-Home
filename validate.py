#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate.py
快速调用： python validate.py <part_number>
会调用 split_and_check_16.py 的 process_part 功能（通过 subprocess 风格运行）
"""

import sys
import os
import subprocess

if len(sys.argv) != 2:
    print("Usage: python validate.py <part_number>")
    sys.exit(1)

part = sys.argv[1]

# call split_and_check_16.py --part <part>
cmd = [sys.executable, "split_and_check_16.py", "--part", str(part)]
print("Running:", " ".join(cmd))
ret = subprocess.run(cmd)
sys.exit(ret.returncode)
