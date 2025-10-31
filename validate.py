#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os
from split_and_check_16 import dns_validate, load_delete_counter, save_delete_counter

if len(sys.argv)<4:
    print("Usage: python validate.py <part_X.txt> <validated_part_X.txt> <log_file>")
    sys.exit(1)

part_file=sys.argv[1]
validated_file=sys.argv[2]
log_file=sys.argv[3]

lines=open(part_file,"r",encoding="utf-8").read().splitlines()
valid_rules=set(dns_validate(lines,batch_size=500))
old_rules=set()
if os.path.exists(validated_file):
    with open(validated_file,"r",encoding="utf-8") as f:
        old_rules=set([l.strip() for l in f if l.strip()])
delete_counter=load_delete_counter()
new_delete_counter={}
final_rules=set()
removed_count=0
added_count=0

for rule in old_rules | set(lines):
    if rule in valid_rules:
        final_rules.add(rule)
        new_delete_counter[rule]=0
    else:
        count=delete_counter.get(rule,0)+1
        new_delete_counter[rule]=count
        if count>=4:
            removed_count+=1
        else:
            final_rules.add(rule)
    if rule not in old_rules and rule in valid_rules:
        added_count+=1

os.makedirs(os.path.dirname(validated_file),exist_ok=True)
save_delete_counter(new_delete_counter)

with open(validated_file,"w",encoding="utf-8") as f:
    f.write("\n".join(sorted(final_rules)))
with open(log_file,"w",encoding="utf-8") as f:
    f.write(f"总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}\n")
print(f"✅ 验证完成: 总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}")
