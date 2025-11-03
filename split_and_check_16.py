#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import argparse
import requests
import dns.resolver
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
VALIDATED_PART_FILE_PATTERN = os.path.join(DIST_DIR, "validated_part_{:02d}.txt")

DNS_WORKERS = 50
SKIP_ROUNDS = 10  # 达到10次跳过

# -----------------------------
# 参数解析
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--part", type=int, help="处理的分片编号 1~16")
parser.add_argument("--force-update", action="store_true", help="强制重新下载和切片")
parser.add_argument("--print-hosts-to-adguard", action="store_true", help="打印 HOSTS 转换为 AdGuard 格式")
args = parser.parse_args()

# -----------------------------
# 加载或初始化 delete_counter
# -----------------------------
if not os.path.exists(DIST_DIR):
    os.makedirs(DIST_DIR)
if os.path.exists(DELETE_COUNTER_FILE):
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        delete_counter = json.load(f)
else:
    delete_counter = {}

# -----------------------------
# HOSTS → AdGuard 转换函数
# -----------------------------
def hosts_to_adguard(line):
    line = line.strip()
    # 处理 HOSTS 形式：0.0.0.0 domain
    if line.startswith("0.0.0.0") or line.startswith("127.0.0.1"):
        parts = line.split()
        if len(parts) >= 2:
            domain = parts[1].strip()
            adguard_rule = f"||{domain}^"
            if args.print_hosts_to_adguard:
                print(f"🔗 HOSTS 转换 → {line} => {adguard_rule}")
            return adguard_rule
    # 保留已有 AdGuard / Regex / CSS 规则
    return line

# -----------------------------
# 更新 delete_counter 并判断是否跳过
# -----------------------------
def check_skip(rule):
    count = delete_counter.get(rule, 0)
    if count >= SKIP_ROUNDS:
        # 超过跳过次数，重置为6
        delete_counter[rule] = 6
        print(f"🔁 恢复验证：{rule}（跳过达到{SKIP_ROUNDS}次 → 重置计数=6）")
        return False
    return count >= SKIP_ROUNDS

def increment_fail(rule, first_fail=1):
    count = delete_counter.get(rule, 0)
    count += 1
    delete_counter[rule] = count
    if count == first_fail:
        print(f"⚠ 第一次失败 = {first_fail} ：{rule}")
    return count

# -----------------------------
# DNS 验证函数
# -----------------------------
def check_dns(rule):
    if rule.startswith("||"):
        domain = rule[2:].rstrip("^")
        try:
            dns.resolver.resolve(domain, 'A')
            return True
        except Exception:
            return False
    return True

# -----------------------------
# 主逻辑
# -----------------------------
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片文件不存在：{part_file}")
        return

    validated_rules = []
    with open(part_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    print(f"📄 分片 {part}: {len(lines)} 条规则 → 正在处理...")

    # HOSTS 转换
    rules = [hosts_to_adguard(line) for line in lines]

    # 并发 DNS 验证
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        future_to_rule = {executor.submit(check_dns, rule): rule for rule in rules}
        for future in tqdm(as_completed(future_to_rule), total=len(future_to_rule), desc=f"分片 {part} DNS 验证"):
            rule = future_to_rule[future]
            try:
                success = future.result()
                if not success:
                    count = increment_fail(rule, first_fail=4)
                    if check_skip(rule):
                        print(f"⏩ 跳过验证 {rule}（次数 {delete_counter[rule]}/{SKIP_ROUNDS}）")
                        validated_rules.append(rule)
                    else:
                        print(f"⚠ 连续失败计数 = {delete_counter[rule]} ：{rule}")
                else:
                    # 验证成功，重置连续失败计数
                    if delete_counter.get(rule, 0) != 0:
                        print(f"✅ 验证成功，重置计数：{rule}")
                    delete_counter[rule] = 0
                    validated_rules.append(rule)
            except Exception as e:
                print(f"⚠ DNS 验证异常：{rule} → {e}")

    # 保存已验证规则
    validated_file = VALIDATED_PART_FILE_PATTERN.format(part)
    with open(validated_file, "w", encoding="utf-8") as f:
        for rule in validated_rules:
            f.write(rule + "\n")
    print(f"✅ 分片 {part} 验证完成 → {validated_file}")

    # 保存 delete_counter.json
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(delete_counter, f, indent=2, ensure_ascii=False)

# -----------------------------
# 执行
# -----------------------------
if args.part:
    process_part(args.part)
elif args.force_update:
    print("⚡ 强制更新模式 → 处理所有分片")
    for part in range(1, 17):
        process_part(part)
else:
    print("ℹ️ 未指定 --part 或 --force-update，仅打印 HOSTS 转换时使用 --print-hosts-to-adguard")
