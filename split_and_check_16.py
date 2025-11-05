#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Lock

# ===============================
# 配置区
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
WRITE_COUNTER_MAX = 3

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

lock = Lock()  # 多线程安全

# ===============================
# JSON 读写
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    with lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# 下载与合并规则源
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
# 删除计数处理
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 17:
                updated_delete_counter[rule] = 6
                print(f"🔁 删除计数达到 17，重置规则：{rule} 的删除计数为 6")
    return low_delete_count_rules, updated_delete_counter

# ===============================
# 分片
# ===============================
def split_parts(merged_rules):
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")
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
    except:
        return None

def dns_validate(rules):
    valid_rules = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        for future in as_completed(futures):
            res = future.result()
            if res:
                valid_rules.append(res)
    return valid_rules

# ===============================
# 更新 not_written_counter.json
# ===============================
def update_not_written_counter(part, final_rules):
    counter = load_json(NOT_WRITTEN_FILE)

    # 写入的规则：write_counter = 3，并记录分片号
    def set_write_counter(rule):
        counter[rule] = {"write_counter": WRITE_COUNTER_MAX, "part": f"validated_part_{part}"}

    # 当前分片未出现：write_counter-1，如果=0则删除
    def reduce_counter(rule):
        info = counter.get(rule)
        if info and info.get("part") == f"validated_part_{part}":
            info["write_counter"] -= 1
            if info["write_counter"] <= 0:
                print(f"🔥 write_counter 为0，从 {info['part']} 删除规则：{rule}")
                counter.pop(rule)

    with ThreadPoolExecutor(max_workers=20) as executor:
        # 并行写入
        for rule in final_rules:
            executor.submit(set_write_counter, rule)
        # 并行减少未出现规则的 write_counter
        for rule, info in list(counter.items()):
            if info.get("part") == f"validated_part_{part}" and rule not in final_rules:
                executor.submit(reduce_counter, rule)

    save_json(NOT_WRITTEN_FILE, counter)

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，拉取规则中…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        old_rules = set([l.strip() for l in open(out_file, "r", encoding="utf-8") if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    for r in lines:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = del_cnt + 1
            print(f"⚠ 删除计数达到 7 或以上，跳过规则：{r} | 删除计数={del_cnt}")

    # DNS 并行验证
    valid = dns_validate(rules_to_validate)

    # 写入 final_rules
    for rule in rules_to_validate:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            added_count += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            print(f"⚠ 连续失败 +1 → {delete_counter[rule]}/{DELETE_THRESHOLD} ：{rule}")
            if delete_counter[rule] >= DELETE_THRESHOLD:
                removed_count += 1
                final_rules.discard(rule)

    # 写入 validated_part_X.txt
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    # 更新 not_written_counter.json
    update_not_written_counter(part, final_rules)

    save_json(DELETE_COUNTER_FILE, delete_counter)

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
