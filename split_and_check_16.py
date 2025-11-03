#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

# 删除阈值 = 4
DELETE_THRESHOLD = 4

# 跳过阈值
SKIP_VALIDATE_THRESHOLD = 7
SKIP_ROUNDS = 10   # 跳过验证 10 次
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")

# 创建目录
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 跳过验证计数器
# ===============================
def load_skip_tracker():
    if os.path.exists(SKIP_FILE):
        try:
            with open(SKIP_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        return {}

def save_skip_tracker(data):
    with open(SKIP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# 删除计数器
# ===============================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print(f"⚠ {DELETE_COUNTER_FILE} 解析失败，重建空计数")
            return {}
    else:
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# HOSTS → AdGuard 转换
# ===============================
def hosts_to_adguard(line):
    line = line.strip()
    if line.startswith("0.0.0.0") or line.startswith("127.0.0.1"):
        parts = line.split()
        if len(parts) >= 2:
            domain = parts[1].strip()
            adguard_rule = f"||{domain}^"
            return adguard_rule
    return line

# ===============================
# 下载规则并合并
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
                if line and not line.startswith("#"):
                    merged.add(hosts_to_adguard(line))
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片
# ===============================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")
    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ===============================
# DNS 验证
# ===============================
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
    print(f"🚀 启动 {DNS_WORKERS} 并发验证")
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        total = len(rules)
        done = 0
        for future in as_completed(futures):
            done += 1
            res = future.result()
            if res:
                valid.append(res)
            if done % 500 == 0 or done == total:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片 DNS 验证完成，有效 {len(valid)} 条")
    return set(valid)

# ===============================
# 处理单个分片
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").readlines() if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    # 筛选需要验证的规则
    rules_to_validate = []
    for r in lines:
        c = delete_counter.get(r, 0)
        if c <= SKIP_VALIDATE_THRESHOLD:
            rules_to_validate.append(r)
        else:
            skip_cnt = skip_tracker.get(r, 0) + 1
            skip_tracker[r] = skip_cnt
            print(f"⏩ 跳过验证 {r}（次数 {skip_cnt}/{SKIP_ROUNDS}）")
            if skip_cnt >= SKIP_ROUNDS:
                print(f"🔁 恢复验证：{r}（跳过达到{SKIP_ROUNDS}次 → 重置计数=4）")
                delete_counter[r] = 4
                skip_tracker.pop(r)
                rules_to_validate.append(r)

    valid_rules = dns_validate(rules_to_validate)

    final_rules = set()
    added_count = 0
    removed_count = 0
    all_rules = old_rules | set(lines)
    new_delete_counter = delete_counter.copy()

    for r in all_rules:
        if r in valid_rules:
            final_rules.add(r)
            new_delete_counter[r] = 0
            if r not in old_rules:
                added_count += 1
            continue
        old_count = delete_counter.get(r, None)
        if old_count is None:
            new_count = 4
        else:
            new_count = old_count + 1
        new_delete_counter[r] = new_count
        print(f"⚠ 连续失败计数 = {new_count} ：{r}")
        if new_count >= DELETE_THRESHOLD:
            removed_count += 1
            continue
        final_rules.add(r)

    save_delete_counter(new_delete_counter)
    save_skip_tracker(skip_tracker)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主函数
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
