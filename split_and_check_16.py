#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import requests
from pathlib import Path
from datetime import datetime

# ---------------------------
# 配置
# ---------------------------
DNS_BATCH_SIZE = 800  # 每批验证数量
NUM_PARTS = 16        # 分片数量
URLS_FILE = "urls.txt"  # 每天更新一次的源列表
TMP_DIR = Path("tmp")
DIST_DIR = Path("dist")

TMP_DIR.mkdir(exist_ok=True)
DIST_DIR.mkdir(exist_ok=True)

# ---------------------------
# 命令行参数
# ---------------------------
parser = argparse.ArgumentParser(description="Split and validate blocklist")
parser.add_argument("--part", type=int, help="指定分片验证 (0-15)")
args = parser.parse_args()

# ---------------------------
# 更新 urls.txt
# ---------------------------
def update_urls():
    if not os.path.exists(URLS_FILE):
        print(f"⚠️ {URLS_FILE} 不存在，跳过更新")
        return []
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    print(f"📥 urls.txt 已加载 {len(urls)} 条源")
    return urls

# ---------------------------
# 下载规则
# ---------------------------
def download_rules(urls):
    rules = []
    for url in urls:
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
                rules.extend(lines)
        except Exception as e:
            print(f"⚠️ 下载失败 {url} -> {e}")
    rules = list(set(rules))
    print(f"📄 总规则 {len(rules)} 条")
    return rules

# ---------------------------
# 分片
# ---------------------------
def split_rules(rules, num_parts):
    parts = [[] for _ in range(num_parts)]
    for i, rule in enumerate(rules):
        parts[i % num_parts].append(rule)
    return parts

# ---------------------------
# 验证规则（模拟）
# ---------------------------
def validate_rules(rules):
    # 这里可以替换成真正 DNS 验证逻辑
    valid_rules = [r for r in rules if r]  # 模拟全部有效
    return valid_rules

# ---------------------------
# 保存分片
# ---------------------------
def save_part(part_idx, rules):
    filename = TMP_DIR / f"part_{part_idx+1:02d}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(rules))
    print(f"📄 分片 {part_idx+1} 保存 {len(rules)} 条规则 → {filename}")
    print(f"前 10 条示例： {rules[:10]}")

# ---------------------------
# 主逻辑
# ---------------------------
def main():
    urls = update_urls()
    rules = download_rules(urls)
    parts = split_rules(rules, NUM_PARTS)

    # 如果指定分片验证
    if args.part is not None:
        idx = args.part
        if 0 <= idx < NUM_PARTS:
            print(f"⏱ 当前处理分片：tmp/part_{idx+1:02d}.txt, 总规则 {len(parts[idx])} 条")
            valid = validate_rules(parts[idx])
            print(f"✅ 已验证 {len(valid)}/{len(parts[idx])} 条，本批有效 {len(valid)} 条")
            save_part(idx, valid)
        else:
            print(f"⚠️ 指定分片 {idx} 无效")
        return

    # 全部分片验证
    all_valid = []
    for idx, part in enumerate(parts):
        print(f"⏱ 当前处理分片：tmp/part_{idx+1:02d}.txt, 总规则 {len(part)} 条")
        valid = validate_rules(part)
        print(f"✅ 已验证 {len(valid)}/{len(part)} 条，本批有效 {len(valid)} 条")
        save_part(idx, valid)
        all_valid.extend(valid)

    # 合并生成 dist/blocklist_valid.txt
    final_file = DIST_DIR / "blocklist_valid.txt"
    with open(final_file, "w", encoding="utf-8") as f:
        f.write("\n".join(all_valid))
    print(f"🎯 最终有效规则合并 {len(all_valid)} 条 → {final_file}")

if __name__ == "__main__":
    main()
