#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.resolver

# ===============================
# 配置
# ===============================
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4

# delete_counter.json 保存在 dist/
DIST_DIR = "dist"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

# ===============================
# 函数
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines):
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        total = len(lines)
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0 or done == total:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    return set(valid)

def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

def validate_part(part_file, validated_file, log_file):
    if not os.path.exists(part_file):
        print(f"❌ 分片文件不存在: {part_file}")
        return

    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8") if l.strip()]
    print(f"⏱ 验证分片 {part_file}，共 {len(lines)} 条规则")

    valid = dns_validate(lines)

    old_rules = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            new_delete_counter[rule] = 0
            if rule in delete_counter:
                print(f"🔄 验证成功，清零删除计数: {rule}")
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

    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片验证完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

    # 输出 commit 用的统计
    commit_stats = f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}"
    print(commit_stats)

    # 写入 log
    with open(log_file, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")
        f.write("\n")
        f.write(commit_stats + "\n")

# ===============================
# 主程序
# ===============================
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python validate.py <part_X.txt> <validated_part_X.txt> <log_file>")
        sys.exit(1)

    part_file = sys.argv[1]
    validated_file = sys.argv[2]
    log_file = sys.argv[3]

    validate_part(part_file, validated_file, log_file)
