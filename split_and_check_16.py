#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import requests
import time

DNS_BATCH_SIZE = 800  # 每批验证数量
TOTAL_PARTS = 16
URLS_FILE = 'urls.txt'
TMP_DIR = 'tmp'
DIST_DIR = 'dist'

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

def update_urls():
    """每天更新 urls.txt"""
    url_source = 'https://raw.githubusercontent.com/wxglenovo/AdGuardHome-Filter/refs/heads/main/dist/blocklist.txt'
    r = requests.get(url_source)
    r.raise_for_status()
    with open(URLS_FILE, 'w', encoding='utf-8') as f:
        f.write(r.text)
    print(f"📄 更新 urls.txt 成功，规则总数: {len(r.text.splitlines())}")

def load_rules():
    with open(URLS_FILE, 'r', encoding='utf-8') as f:
        rules = [line.strip() for line in f if line.strip()]
    return rules

def split_rules(rules):
    """分成16个切片"""
    part_size = (len(rules) + TOTAL_PARTS - 1) // TOTAL_PARTS
    parts = []
    for i in range(TOTAL_PARTS):
        start = i * part_size
        end = start + part_size
        part = rules[start:end]
        parts.append(part)
        part_file = os.path.join(TMP_DIR, f'part_{i+1:02d}.txt')
        with open(part_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(part))
        print(f"📄 分片 {i+1} 保存 {len(part)} 条规则 → {part_file}")
        print(f"前 10 条示例： {part[:10]}")
    return parts

def validate_rules(part_rules):
    """模拟验证，返回有效规则列表"""
    valid_rules = []
    total = len(part_rules)
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = part_rules[i:i+DNS_BATCH_SIZE]
        # 模拟 DNS 验证，这里直接假设偶数条有效
        batch_valid = [rule for idx, rule in enumerate(batch) if idx % 2 == 0]
        valid_rules.extend(batch_valid)
        print(f"⏱ 已验证 {min(i+DNS_BATCH_SIZE, total)}/{total} 条，本批有效 {len(batch_valid)} 条")
        time.sleep(0.1)  # 模拟验证耗时
    return valid_rules

def save_valid_rules(valid_rules):
    out_file = os.path.join(DIST_DIR, 'blocklist_valid.txt')
    with open(out_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(valid_rules))
    print(f"✅ 已保存有效规则，共 {len(valid_rules)} 条 → {out_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--part', type=int, default=-1, help='指定验证的切片编号 0-15')
    args = parser.parse_args()

    # 每天更新 urls.txt
    if not os.path.exists(URLS_FILE) or time.time() - os.path.getmtime(URLS_FILE) > 86400:
        update_urls()

    rules = load_rules()
    parts = split_rules(rules)

    if 0 <= args.part < TOTAL_PARTS:
        # 手动触发验证单个切片
        part_idx = args.part
        print(f"⏱ 当前处理分片：tmp/part_{part_idx+1:02d}.txt, 总规则 {len(parts[part_idx])} 条")
        print(f"前 10 条规则示例： {parts[part_idx][:10]}")
        valid_rules = validate_rules(parts[part_idx])
    else:
        # 自动轮替，按顺序验证每个切片
        valid_rules = []
        for idx, part in enumerate(parts):
            print(f"⏱ 当前处理分片：tmp/part_{idx+1:02d}.txt, 总规则 {len(part)} 条")
            print(f"前 10 条规则示例： {part[:10]}")
            valid_rules.extend(validate_rules(part))

    save_valid_rules(valid_rules)

if __name__ == '__main__':
    main()
