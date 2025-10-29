#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import dns.resolver
import concurrent.futures
import argparse
from datetime import datetime

# ===============================
# 配置
# ===============================
URLS_FILE = "urls.txt"
OUTPUT_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16
MAX_WORKERS = 80         # DNS 并发线程数
DNS_BATCH_SIZE = 800     # 每批验证规则数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

# ===============================
# 函数
# ===============================
def safe_fetch(url):
    try:
        print(f"📥 下载：{url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except:
        print(f"⚠️ 下载失败：{url}")
        return []

def clean_rule(line):
    l = line.strip()
    if not l or l.startswith("#"):
        return None
    return l

def extract_domain(rule):
    return rule.lstrip("|").lstrip(".").split("^")[0].strip()

def is_valid_domain(domain):
    try:
        resolver.resolve(domain, "A")
        return True
    except:
        return False

def check_rule(rule):
    domain = extract_domain(rule)
    return rule if is_valid_domain(domain) else None

def chunk_rules(rules, parts):
    total = len(rules)
    chunk_size = total // parts
    chunks = []
    for i in range(parts):
        start = i * chunk_size
        end = None if i == parts - 1 else (i + 1) * chunk_size
        chunks.append(rules[start:end])
    return chunks

# ===============================
# 主程序
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动指定分片 0~15")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DIST_DIR, exist_ok=True)
    part_files = [os.path.join(OUTPUT_DIR, f"part_{i+1:02d}.txt") for i in range(PARTS)]
    valid_output = os.path.join(DIST_DIR, "blocklist_valid.txt")

    # -------------------------------
    # 首次切分分片
    # -------------------------------
    if not os.path.exists(part_files[0]):
        print("🧩 首次运行：下载并切片")
        if not os.path.exists(URLS_FILE):
            print(f"❌ 未找到 {URLS_FILE}")
            return

        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            for lines in ex.map(safe_fetch, urls):
                all_rules.extend(lines)

        cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
        print(f"✅ 去重后总计：{len(cleaned):,} 条")

        chunks = chunk_rules(cleaned, PARTS)
        for i, chunk in enumerate(chunks):
            with open(part_files[i], "w", encoding="utf-8") as f:
                f.write("\n".join(chunk))
            print(f"📄 分片 {i+1:02d} 保存 {len(chunk):,} 条规则 → {part_files[i]}")
            print(f"前 10 条示例： {chunk[:10]}")
        return

    # -------------------------------
    # 选择分片
    # -------------------------------
    if args.part is not None:
        if not (0 <= args.part < PARTS):
            print(f"❌ 分片 {args.part} 不合法")
            return
        part_index = args.part
        print(f"🛠 手动触发，验证分片 {part_index}")
    else:
        # 自动轮替：按当前 UTC 时间每 1.5 小时选择分片
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS
        print(f"⏱ 自动轮替，当前处理分片 {part_index}")

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片不存在，跳过：{target_file}")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # -------------------------------
    # 批量 DNS 验证
    # -------------------------------
    valid = []
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, len(rules)):,}/{len(rules):,} 条，本批有效 {len(batch_valid):,} 条")

    # -------------------------------
    # 保存有效规则
    # -------------------------------
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"🎯 本分片完成验证，总有效 {len(valid):,} 条 → 已追加至 {valid_output}")

if __name__ == "__main__":
    main()
