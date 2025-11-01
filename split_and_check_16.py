#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import re

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"               # 存放规则源地址
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"    # 合并后的规则文件
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# 可随机使用多个 DNS，避免单服务器导致大量 Fails
DNS_SERVERS = [
    "1.1.1.1", "8.8.8.8", "8.8.4.4",
    "9.9.9.9", "208.67.222.222", "208.67.220.220"
]

# ===============================
# 下载与合并规则
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 下载所有规则源...")
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        print(f"🌐 获取：{url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                # 跳过注释或无效字符串
                if not line or line.startswith("#") or len(line) < 3:
                    continue
                merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败：{url} → {e}")
    print(f"✅ 合并完成，共 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片
# ===============================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ merged_rules.txt 缺失")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    if total == 0:
        print("❌ 合并结果为空，无法分片")
        return False

    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 开始分片：共 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        chunk = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(chunk))
        print(f"📄 分片 {i+1}：{len(chunk)} 条 → {filename}")
    return True

# ===============================
# 解析域名
# ===============================
DOMAIN_RE = re.compile(r"([\w\-\.]+\.\w+)$")

def extract_domain(rule):
    rule = rule.replace("@@","").replace("||","").lstrip("|")
    rule = rule.split("^")[0].replace("*","").strip()
    m = DOMAIN_RE.search(rule)
    return m.group(1) if m else None

# ===============================
# DNS 验证
# ===============================
def check_domain(rule):
    domain = extract_domain(rule)
    if not domain:
        return None

    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    resolver.nameservers = [random.choice(DNS_SERVERS)]

    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines):
    print(f"🚀 并发验证 {DNS_WORKERS} 条线程")
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in lines}
        total = len(lines)
        done = 0
        for future in as_completed(futures):
            done += 1
            if future.result():
                valid.append(future.result())
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total}，有效：{len(valid)}")
    print(f"✅ 完成，最终有效：{len(valid)}")
    return valid

# ===============================
# 删除计数
# ===============================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print(f"⚠ {DELETE_COUNTER_FILE} 损坏，已重建")
            return {}
    else:
        print("⚠ delete_counter.json 不存在，创建中…")
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# 分片处理
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失 → 自动下载+分片")
        download_all_sources()
        split_parts()

    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，停止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}：{len(lines)} 条规则")

    valid = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{int(part):02d}.txt")

    # 旧规则载入
    old = set()
    if os.path.exists(out_file):
        old = {l.strip() for l in open(out_file, "r", encoding="utf-8") if l.strip()}

    counter = load_delete_counter()
    new_counter = {}
    final = set()
    removed = added = 0

    all_rules = old | set(lines)

    for rule in all_rules:
        if rule in valid:
            final.add(rule)
            new_counter[rule] = 0
        else:
            c = counter.get(rule, 0) + 1
            new_counter[rule] = c
            if c >= DELETE_THRESHOLD:
                removed += 1
            else:
                final.add(rule)

        if rule not in old and rule in valid:
            added += 1

    save_delete_counter(new_counter)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final)))

    print(f"✅ 分片 {part} 完成 → 保留 {len(final)} 新增 {added} 删除 {removed}")
    print(f"COMMIT_STATS: 总 {len(final)}, 新增 {added}, 删除 {removed}")

# ===============================
# 主函数
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force", action="store_true", help="强制下载+切片（兼容 actions）")
    parser.add_argument("--force-update", action="store_true", help="强制下载+切片")
    args = parser.parse_args()

    # ✅ 两种写法均支持
    if args.force or args.force_update:
        download_all_sources()
        split_parts()

    # ✅ 自动修复缺失
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片 → 自动生成")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
