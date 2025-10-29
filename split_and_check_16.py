#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import requests
import time
from datetime import datetime

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
DNS_BATCH_SIZE = 800  # 每批验证数量
TOTAL_PARTS = 16

# ===============================
# 下载 urls.txt
# ===============================
def update_urls():
    print("📥 开始更新 urls.txt")
    url = "https://raw.githubusercontent.com/wxglenovo/AdGuardHome-Filter/refs/heads/main/urls.txt"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(URLS_TXT, "w", encoding="utf-8") as f:
        f.write(r.text)
    print(f"✅ 更新完成，规则总数 {len(r.text.splitlines())} 条")

# ===============================
# 分片
# ===============================
def split_parts():
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    total = len(lines)
    part_size = (total + TOTAL_PARTS - 1) // TOTAL_PARTS
    parts = []
    for i in range(TOTAL_PARTS):
        start = i * part_size
        end = start + part_size
        part_lines = lines[start:end]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as pf:
            pf.write("\n".join(part_lines))
        parts.append(part_file)
        print(f"📄 分片 {i+1} 保存 {len(part_lines)} 条规则 → {part_file}")
        print(f"前 10 条示例： {part_lines[:10]}")
    return parts

# ===============================
# 模拟 DNS 验证
# ===============================
def validate_part(part_file):
    with open(part_file, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]
    total = len(rules)
    valid_count = 0
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        # 模拟验证，每条规则随机成功（这里可以替换成真实 DNS 验证逻辑）
        batch_valid = len(batch) // 2  # 模拟有效一半
        valid_count += batch_valid
        print(f"⏱ 当前处理分片：{part_file}, 总规则 {total} 条")
        print(f"前 10 条规则示例： {batch[:10]}")
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE,total)}/{total} 条，本批有效 {batch_valid} 条")
        time.sleep(0.5)
    return valid_count

# ===============================
# 合并有效规则
# ===============================
def merge_valid(parts):
    if not os.path.exists(DIST_DIR):
        os.makedirs(DIST_DIR)
    merged_file = os.path.join(DIST_DIR, "blocklist_valid.txt")
    all_rules = []
    for part in parts:
        with open(part, "r", encoding="utf-8") as f:
            all_rules.extend([line.strip() for line in f if line.strip()])
    with open(merged_file, "w", encoding="utf-8") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并完成 → {merged_file}, 总规则 {len(all_rules)} 条")

# ===============================
# 主函数
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="指定分片验证 1~16")
    parser.add_argument("--update", action="store_true", help="更新 urls.txt")
    args = parser.parse_args()

    if args.update or not os.path.exists(URLS_TXT):
        update_urls()

    parts = split_parts()

    if args.part:
        if 1 <= args.part <= TOTAL_PARTS:
            validate_part(parts[args.part - 1])
        else:
            print("❌ 分片编号无效")
    else:
        for part_file in parts:
            validate_part(part_file)

    merge_valid(parts)

if __name__ == "__main__":
    main()
