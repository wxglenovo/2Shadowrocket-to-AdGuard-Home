#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

import os
import json
import dns.resolver
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置
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

# 删除计数管理
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

def process_part(part):
    part_file = os.path.join("tmp", f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")
    valid = set(dns_validate(lines))
    out_file = os.path.join("dist", f"validated_part_{part}.txt")

    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            if rule in delete_counter:
                print(f"🔄 验证成功，清零删除计数: {rule}")
            new_delete_counter[rule] = 0
        else:
            count = delete_counter.get(rule, 0) + 1
            new_delete_counter[rule] = count
            print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
            if count >= DELETE_THRESHOLD:
                removed_count += 1
            else:
                final_rules.add(rule)
        if rule not in old_rules and rule in valid:
            added_count += 1

    save_delete_counter(new_delete_counter)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    # 💾 输出给 workflow 用作 commit 信息
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
