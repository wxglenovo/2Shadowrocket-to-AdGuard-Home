#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import os
import argparse
from math import ceil

# ===============================
# 配置
# ===============================
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
VALID_FILE = os.path.join(DIST_DIR, "blocklist_valid.txt")
DNS_BATCH_SIZE = 800
NUM_PARTS = 16

# ===============================
# 工具函数
# ===============================
def fetch_all_urls(urls_file):
    urls = []
    with open(urls_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls

def download_rules(urls):
    all_rules = []
    for url in urls:
        try:
            print(f"📥 下载 {url} ...")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            rules = [line.strip() for line in resp.text.splitlines() if line.strip()]
            all_rules.extend(rules)
            print(f"✅ 下载 {len(rules)} 条规则")
        except Exception as e:
            print(f"❌ 下载失败 {url}: {e}")
    # 去重
    all_rules = list(dict.fromkeys(all_rules))
    print(f"📊 总规则数: {len(all_rules)}")
    return all_rules

def split_rules(rules, num_parts):
    size = ceil(len(rules) / num_parts)
    parts = []
    for i in range(num_parts):
        part_rules = rules[i*size:(i+1)*size]
        parts.append(part_rules)
    return parts

def save_part(part_rules, index):
    os.makedirs(TMP_DIR, exist_ok=True)
    filename = os.path.join(TMP_DIR, f"part_{index+1:02d}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        for rule in part_rules:
            f.write(rule + "\n")
    print(f"📄 分片 {index+1} 保存 {len(part_rules)} 条规则 → {filename}")
    print(f"前 10 条示例： {part_rules[:10]}")
    return filename

def mock_validate_rules(part_rules):
    # 模拟 DNS 验证
    valid_rules = []
    total = len(part_rules)
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = part_rules[i:i+DNS_BATCH_SIZE]
        # 假设验证成功率高
        valid_rules.extend(batch)
        print(f"⏱ 已验证 {min(i+DNS_BATCH_SIZE,total)}/{total} 条，本批有效 {len(batch)} 条")
    return valid_rules

# ===============================
# 主函数
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片 0~15")
    args = parser.parse_args()

    os.makedirs(DIST_DIR, exist_ok=True)
    urls = fetch_all_urls(URLS_FILE)
    all_rules = download_rules(urls)
    parts = split_rules(all_rules, NUM_PARTS)

    if args.part is not None:
        # 验证指定分片
        part_rules = parts[args.part]
        filename = save_part(part_rules, args.part)
        valid_rules = mock_validate_rules(part_rules)
    else:
        # 验证全部分片
        valid_rules = []
        for idx, part_rules in enumerate(parts):
            filename = save_part(part_rules, idx)
            valid_rules.extend(mock_validate_rules(part_rules))

    # 保存最终有效规则
    with open(VALID_FILE, "w", encoding="utf-8") as f:
        for rule in valid_rules:
            f.write(rule + "\n")
    print(f"🎉 最终有效规则保存至 {VALID_FILE}，共 {len(valid_rules)} 条")

if __name__ == "__main__":
    main()
