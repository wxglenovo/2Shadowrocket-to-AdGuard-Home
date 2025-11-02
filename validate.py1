#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

import os
import json
import dns.resolver
import sys

DELETE_THRESHOLD = 4
DELETE_COUNTER_FILE = "dist/delete_counter.json"

def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines, workers=50):
    valid = []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        total = len(lines)
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    sys.stdout.flush()
    return valid
