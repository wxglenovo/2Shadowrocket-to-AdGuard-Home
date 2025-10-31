#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.resolver

DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4
DELETE_COUNTER_FILE = "dist/delete_counter.json"

if len(sys.argv) != 4:
    print("Usage: python validate.py <part_X.txt> <validated_part_X.txt> <log_file>")
    sys.exit(1)

part_file = sys.argv[1]
validated_file = sys.argv[2]
log_file = sys.argv[3]

if not os.path.exists(part_file):
    print(f"❌ 分片文件不存在: {part_file}")
    sys.exit(1)

with open(part_file, "r", encoding="utf-8") as f:
    rules = [l.strip() for l in f if l.strip()]

delete_counter = {}
if os.path.exists(DELETE_COUNTER_FILE):
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        delete_counter = json.load(f)

def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*","")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(rules):
    valid=[]
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures={executor.submit(check_domain,r):r for r in rules}
        done=0
        for future in as_completed(futures):
            done+=1
            r=future.result()
            if r: valid.append(r)
    return valid

valid_rules = set(dns_validate(rules))

old_rules=set()
if os.path.exists(validated_file):
    with open(validated_file,"r",encoding="utf-8") as f:
        old_rules=set([l.strip() for l in f if l.strip()])

final_rules=set()
new_delete_counter={}
added_count=0
removed_count=0

for rule in old_rules|set(rules):
    if rule in valid_rules:
        final_rules.add(rule)
        new_delete_counter[rule]=0
    else:
        count=delete_counter.get(rule,0)+1
        new_delete_counter[rule]=count
        if count>=DELETE_THRESHOLD:
            removed_count+=1
        else:
            final_rules.add(rule)
    if rule not in old_rules and rule in valid_rules:
        added_count+=1

os.makedirs(os.path.dirname(DELETE_COUNTER_FILE),exist_ok=True)
with open(DELETE_COUNTER_FILE,"w",encoding="utf-8") as f:
    json.dump(new_delete_counter,f,indent=2,ensure_ascii=False)

with open(validated_file,"w",encoding="utf-8") as f:
    f.write("\n".join(sorted(final_rules)))

with open(log_file,"w",encoding="utf-8") as f:
    f.write(f"总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}\n")

print(f"✅ 验证完成: 总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}")
