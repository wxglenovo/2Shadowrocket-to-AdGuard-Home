#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import dns.resolver
import argparse

DIST_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_TIMEOUT = 1

resolver = dns.resolver.Resolver()
resolver.timeout = DNS_TIMEOUT
resolver.lifetime = DNS_TIMEOUT

def is_valid_domain(domain: str) -> bool:
    try:
        resolver.resolve(domain, "A")
        return True
    except:
        return False

def load_rules(file_path):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return set(line.strip() for line in f if line.strip())

def save_rules(path, rules):
    with open(path, "w", encoding="utf-8") as f:
        for r in sorted(rules):
            f.write(r + "\n")

def process_part(part):
    part_name = f"{int(part):02d}"
    input_file = f"{TMP_DIR}/part_{part_name}.txt"
    output_file = f"{DIST_DIR}/validated_part_{part_name}.txt"

    new_rules = load_rules(input_file)
    old_rules = load_rules(f"{DIST_DIR}/validated_part_{part_name}.txt")

    valid = set()
    removed_count = 0

    print(f"🔍 开始验证分片 {part_name}, {len(new_rules)} 条")

    for line in new_rules:
        domain = line.replace("^", "").replace("||", "").replace("0.0.0.0 ", "").replace(":443", "").strip()

        if is_valid_domain(domain):
            valid.add(line)
        else:
            removed_count += 1

    added = valid - old_rules
    save_rules(output_file, list(valid))

    total_final = len(valid)
    added_count = len(added)

    # ✅ 写入 dist 文件底部
    with open(output_file, "a", encoding="utf-8") as f:
        f.write(f"# 总数: {total_final}\n# 新增: {added_count}\n# 删除: {removed_count}\n")

    # ✅ 输出统计给 GitHub Actions
    stats_msg = f"总数 {total_final}, 新增 {added_count}, 删除 {removed_count}"
    print(f"✅ 分片 {part_name} 完成 → {stats_msg}")
    print(f"::set-output name=stats::{stats_msg}")

    # ✅ 输出到环境变量，给 commit message 使用
    with open(os.environ["GITHUB_ENV"], "a") as env:
        env.write(f"PART_STATS={stats_msg}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", required=True)
    args = parser.parse_args()
    process_part(args.part)
