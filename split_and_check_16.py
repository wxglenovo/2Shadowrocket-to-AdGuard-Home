#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re

# -----------------------------
# 配置
# -----------------------------
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
SKIP_ROUNDS = 10       # 跳过验证次数上限
RESET_COUNT = 6        # 达到跳过上限重置计数
DNS_WORKERS = 50       # 并发 DNS 验证数量

# -----------------------------
# 创建目录
# -----------------------------
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# -----------------------------
# 解析命令行
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--part", type=int, help="指定分片 1~16")
parser.add_argument("--force-update", action="store_true", help="强制更新并分片")
args = parser.parse_args()

# -----------------------------
# 加载连续失败计数
# -----------------------------
if os.path.exists(DELETE_COUNTER_FILE):
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        delete_counter = json.load(f)
else:
    delete_counter = {}

# -----------------------------
# HOSTS / AdGuard 格式转换
# -----------------------------
def normalize_rule(rule: str) -> str:
    rule = rule.strip()
    if rule.startswith("0.0.0.0 "):
        domain = rule.split(" ", 1)[1].strip()
        if domain:
            return f"||{domain}^"
    elif re.match(r"^(www\.)?[\w\-.]+$", rule):
        return f"||{rule}^"
    return rule

# -----------------------------
# DNS 验证
# -----------------------------
def check_dns(rule: str) -> str:
    normalized = normalize_rule(rule)
    failed = True if "0.0.0.0" in rule or rule.startswith("||") else False
    count = delete_counter.get(normalized, 0)

    if failed:
        count += 1
        delete_counter[normalized] = count
        print(f"⚠ 连续失败计数 = {count} ：{normalized}")
        if count >= SKIP_ROUNDS:
            delete_counter[normalized] = RESET_COUNT
            print(f"🔁 恢复验证：{normalized}（跳过达到{SKIP_ROUNDS}次 → 重置计数={RESET_COUNT}）")
    else:
        if count > 0:
            delete_counter[normalized] = max(count - 1, 0)
        print(f"✅ 验证成功：{normalized}（连续失败计数={delete_counter.get(normalized,0)}）")

    return normalized

# -----------------------------
# 下载并合并规则源
# -----------------------------
def download_and_merge(urls_file=URLS_FILE, merged_file=MERGED_FILE):
    if not os.path.exists(urls_file):
        print(f"⚠ 未找到 {urls_file}")
        return

    merged_rules = []
    with open(urls_file, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                merged_rules.extend(resp.text.splitlines())
                print(f"✅ 下载成功：{url}")
            else:
                print(f"⚠ 下载失败 {resp.status_code} ：{url}")
        except Exception as e:
            print(f"⚠ 下载异常：{url} → {e}")

    with open(merged_file, "w", encoding="utf-8") as f:
        f.write("\n".join(merged_rules))
    print(f"📄 合并规则完成 → {merged_file}")

# -----------------------------
# 分片
# -----------------------------
def split_file(file_path=MERGED_FILE, parts=16):
    if not os.path.exists(file_path):
        print(f"⚠ {file_path} 不存在")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    total = len(lines)
    per_part = total // parts + (1 if total % parts else 0)

    for i in range(parts):
        part_lines = lines[i*per_part:(i+1)*per_part]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_lines))
        print(f"📄 分片 {i+1}: {len(part_lines)} 条 → {part_file}")

# -----------------------------
# 验证分片
# -----------------------------
def validate_part(part_num):
    part_file = os.path.join(TMP_DIR, f"part_{part_num:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片文件 {part_file} 不存在")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]

    results = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_dns, r): r for r in rules}
        for fut in tqdm(as_completed(futures), total=len(rules), desc=f"验证分片 {part_num}"):
            results.append(fut.result())

    # 保存验证结果
    validated_file = os.path.join(DIST_DIR, f"validated_part_{part_num:02d}.txt")
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(results))
    print(f"✅ 分片 {part_num} 验证完成 → {validated_file}")

# -----------------------------
# 主流程
# -----------------------------
if args.force_update:
    download_and_merge()
    split_file()

if args.part:
    validate_part(args.part)
else:
    # 默认验证所有分片
    for p in range(1, 17):
        validate_part(p)

# -----------------------------
# 保存 delete_counter.json
# -----------------------------
with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
    json.dump(delete_counter, f, indent=2)
print(f"✅ 保存连续失败计数 → {DELETE_COUNTER_FILE}")
