#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

TMP_DIR = "tmp"
DIST_DIR = "dist"
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


def load_not_written():
    if os.path.exists(NOT_WRITTEN_FILE):
        with open(NOT_WRITTEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_not_written(data):
    with open(NOT_WRITTEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def dns_check(domain):
    try:
        dns.resolver.resolve(domain, "A")
        return True
    except:
        return False


def validate_rules(rules):
    valid = []
    failed_counter = {}

    batch = 500
    total = len(rules)

    for i in range(0, total, batch):
        chunk = rules[i:i+batch]
        with ThreadPoolExecutor(max_workers=50) as executor:
            future_to_rule = {executor.submit(dns_check, r): r for r in chunk}
            for future in as_completed(future_to_rule):
                rule = future_to_rule[future]
                try:
                    ok = future.result()
                except:
                    ok = False

                if ok:
                    valid.append(rule)
                else:
                    failed_counter[rule] = failed_counter.get(rule, 0) + 1
                    print(f"⚠ 连续失败 +1 → {failed_counter[rule]}/4 ：{rule}")

    return valid, failed_counter


def process_part(part_num):
    part_file = os.path.join(TMP_DIR, f"part_{part_num:02d}.txt")
    validated_file = os.path.join(DIST_DIR, f"validated_part_{part_num}.txt")
    tmp_valid_file = os.path.join(TMP_DIR, f"vpart_{part_num}.tmp")

    if not os.path.exists(part_file):
        print(f"❌ 分片 {part_num} 不存在：{part_file}")
        return

    not_written = load_not_written()

    old_valid = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            old_valid = set([line.strip() for line in f if line.strip()])

    with open(part_file, "r", encoding="utf-8") as f:
        part_rules = [line.strip() for line in f if line.strip()]

    total_rules = len(part_rules)
    valid_rules, failed_counter = validate_rules(part_rules)

    with open(tmp_valid_file, "w", encoding="utf-8") as f:
        for rule in valid_rules:
            f.write(rule + "\n")

    for rule in valid_rules:
        not_written[rule] = {"write_counter": 4}

    removed_from_current = old_valid - set(valid_rules)
    for rule in removed_from_current:
        if rule in not_written:
            not_written[rule]["write_counter"] = not_written[rule].get("write_counter", 1) - 1
        else:
            not_written[rule] = {"write_counter": 2}

    updated_validated = []
    for rule, info in not_written.items():
        wc = info.get("write_counter", 0)
        if wc > 0:
            updated_validated.append(rule)

    with open(validated_file, "w", encoding="utf-8") as f:
        for r in updated_validated:
            f.write(r + "\n")

    new_added = len(set(valid_rules) - old_valid)
    new_deleted = len(old_valid - set(valid_rules))

    print(f"🤖 分片 {part_num} | 总 {total_rules}, 新增 {new_added}, 删除 {new_deleted}")

    save_not_written(not_written)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("part", type=int, help="分片编号 1~16")
    args = parser.parse_args()

    process_part(args.part)
