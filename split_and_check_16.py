#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 500
WRITE_COUNTER_MAX = 6

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠ 读取 {path} 时发生错误: {e}")
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ 已保存 {path}")
    except Exception as e:
        print(f"⚠ 保存 {path} 时发生错误: {e}")

# ===============================
# 下载并合并规则源
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False

    print("📥 下载规则源...")
    merged = set()

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line:
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    filtered_rules, updated_delete_counter = filter_and_update_high_delete_count_rules(merged)
    split_parts(filtered_rules)
    save_json(DELETE_COUNTER_FILE, updated_delete_counter)
    return True

# ===============================
# 处理删除计数 >=7 的规则
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    reset_count = 0
    skipped_count = 0
    skipped_rules = []
    reset_rules = []

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 24:
                updated_delete_counter[rule] = 5
                reset_count += 1
                reset_rules.append(rule)
            skipped_count += 1
            skipped_rules.append(rule)

    for rule in skipped_rules[:20]:
        print(f"⚠ 删除计数达到 7 或以上，跳过规则：{rule}")

    print(f"🔢 共 {skipped_count} 条规则删除计数达到 7 或以上被跳过验证")

    for rule in reset_rules[:20]:
        print(f"🔁 删除计数达到 24，重置：{rule} 的删除计数为 5")

    print(f"🔢 共 {reset_count} 条规则删除计数达到 24 的已重置为 5")

    return low_delete_count_rules, updated_delete_counter

# ===============================
# 分片
# ===============================
def split_parts(merged_rules):
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条规则")

    for i in range(PARTS):
        part_rules = list(merged_rules)[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

# ===============================
# DNS 验证
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
    except Exception:
        return None

def dns_validate(rules, part):
    valid_rules = []
    total_rules = len(rules)
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in rules}
        completed = 0
        start_time = time.time()
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_rules.append(result)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed
                eta = (total_rules - completed)/speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")

    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(valid_rules)))

    return valid_rules

# ===============================
# 更新 not_written_counter.json（修正版）
# ===============================
def update_not_written_counter(part):
    part_key = f"validated_part_{part}"
    counter = load_json(NOT_WRITTEN_FILE)

    for i in range(1, PARTS+1):
        pk = f"validated_part_{i}"
        if pk not in counter:
            counter[pk] = {}

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")

    existing_rules = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as vf:
            existing_rules = set([l.strip() for l in vf if l.strip()])

    tmp_rules = set()
    if os.path.exists(tmp_file):
        with open(tmp_file, "r", encoding="utf-8") as tf:
            tmp_rules = set([l.strip() for l in tf if l.strip()])

    part_counter = counter[part_key]

    # 验证成功规则 write_counter = 6
    for rule in tmp_rules:
        part_counter[rule] = 6

    # 已存在规则在 tmp_rules 中缺失，write_counter -1
    for rule in existing_rules:
        if rule not in tmp_rules:
            if rule in part_counter:
                part_counter[rule] -= 1
                if part_counter[rule] <= 0:
                    del part_counter[rule]
            else:
                part_counter[rule] = 5

    # 删除 validated_file 中 write_counter ≤3 的规则，并写入带 header 文件
    header_w = "! Validated rules"
    cleaned_whitelist = [r for r in existing_rules.union(tmp_rules) if part_counter.get(r, 0) > 3]

    with open(validated_file, "w", encoding="utf-8") as f:
        f.write(header_w + "\n")
        f.write("\n".join(sorted(cleaned_whitelist)) + "\n")

    counter[part_key] = part_counter
    save_json(NOT_WRITTEN_FILE, counter)
