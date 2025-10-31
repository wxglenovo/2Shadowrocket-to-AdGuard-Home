#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

DIST_DIR = "dist"
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

os.makedirs(DIST_DIR, exist_ok=True)

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
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
        futures = {ex.submit(check_domain, r): r for r in lines}
        done = 0
        for f in as_completed(futures):
            done += 1
            r = f.result()
            if r:
                valid.append(r)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{len(lines)} 条，有效 {len(valid)} 条")
    return valid

def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

def process_part(part):
    part_file = f"tmp/part_{int(part):02d}.txt"
    if not os.path.exists(part_file):
        print(f"❌ 分片缺失: {part_file}")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8") if l.strip()]
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")

    valid = set(dns_validate(lines))
    out_file = f"dist/validated_part_{int(part):02d}.txt"

    old_rules = set()
    if os.path.exists(out_file):
        old_rules = {l.strip() for l in open(out_file, "r", encoding="utf-8") if l.strip()}

    # ✅ FIX #3: 加载旧计数，不覆盖其他分片的数据
    delete_counter = load_delete_counter()
    new_delete_counter = delete_counter.copy()

    final_rules = set()
    added = 0
    removed = 0

    # ✅ old_rules ∪ 新规则，这里是完整候选集
    all_rules = old_rules | set(lines)

    for rule in all_rules:
        if rule in valid:
            final_rules.add(rule)
            new_delete_counter[rule] = 0  # 验证成功归零
        else:
            old_cnt = delete_counter.get(rule, 0)
            # ✅ FIX #2: 删除计数封顶 4，不得超过
            new_cnt = min(old_cnt + 1, DELETE_THRESHOLD)
            new_delete_counter[rule] = new_cnt

            print(f"⚠ 连续删除计数 {new_cnt}/{DELETE_THRESHOLD}: {rule}")

            # ✅ FIX #1: 达到4次 → 真正删除，不写入文件
            if new_cnt >= DELETE_THRESHOLD:
                removed += 1
            else:
                final_rules.add(rule)

        # 统计新增
        if rule not in old_rules and rule in valid:
            added += 1

    # ✅ 保存完整计数（不会覆盖其他分片的记录）
    save_delete_counter(new_delete_counter)

    # ✅ 写入新的 validated 文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total = len(final_rules)
    print(f"validated part {part} → 总 {total}, 新增 {added}, 删除 {removed}")
    print(f"COMMIT_STATS: 总 {total}, 新增 {added}, 删除 {removed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片")
    args = parser.parse_args()

    if args.part:
        process_part(args.part)
