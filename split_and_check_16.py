#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import requests
from pathlib import Path

DNS_BATCH_SIZE = 800  # 每批处理条数

# ===============================
# 参数解析
# ===============================
parser = argparse.ArgumentParser(description="DNS 分片验证脚本")
parser.add_argument('--part', type=int, help='指定验证分片 1~16', default=None)
args = parser.parse_args()

# ===============================
# 文件与目录准备
# ===============================
urls_file = Path("urls.txt")
tmp_dir = Path("tmp")
dist_dir = Path("dist")
tmp_dir.mkdir(exist_ok=True)
dist_dir.mkdir(exist_ok=True)

# ===============================
# 读取 urls.txt
# ===============================
if not urls_file.exists():
    raise FileNotFoundError(f"{urls_file} 不存在，请先更新")

with open(urls_file, 'r', encoding='utf-8') as f:
    urls = [line.strip() for line in f if line.strip()]

total_count = len(urls)

# ===============================
# 切分为 16 个分片
# ===============================
parts = 16
part_size = (total_count + parts - 1) // parts  # 向上取整

part_files = []
for i in range(parts):
    start = i * part_size
    end = min(start + part_size, total_count)
    part_urls = urls[start:end]
    part_file = tmp_dir / f"part_{i+1:02d}.txt"
    with open(part_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(part_urls))
    print(f"📄 分片 {i+1} 保存 {len(part_urls)} 条规则 → {part_file}")
    print("前 10 条示例：", part_urls[:10])
    part_files.append(part_file)

# ===============================
# 验证函数（示例：请求每条 URL 返回状态码 200）
# ===============================
def validate_dns(url_list):
    valid = []
    for i in range(0, len(url_list), DNS_BATCH_SIZE):
        batch = url_list[i:i+DNS_BATCH_SIZE]
        batch_valid = []
        for u in batch:
            try:
                # 这里只是示例，实际可以做 DNS 查询或请求头验证
                resp = requests.head("http://" + u.lstrip("|^").replace(".*", ""), timeout=3)
                if resp.status_code < 400:
                    batch_valid.append(u)
            except:
                pass
        valid.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, len(url_list))}/{len(url_list)} 条，本批有效 {len(batch_valid)} 条")
    return valid

# ===============================
# 自动或手动分片验证
# ===============================
if args.part:
    part_index = args.part - 1
    if part_index < 0 or part_index >= parts:
        raise ValueError("分片编号必须 1~16")
    current_part_file = part_files[part_index]
    print(f"⏱ 当前处理分片：{current_part_file}, 总规则 {len(open(current_part_file).readlines())} 条")
    with open(current_part_file, 'r', encoding='utf-8') as f:
        urls_to_check = [line.strip() for line in f if line.strip()]
    valid_urls = validate_dns(urls_to_check)
else:
    # 自动轮替验证全部分片（按顺序处理）
    for idx, part_file in enumerate(part_files):
        print(f"⏱ 当前处理分片 {idx+1}: {part_file}, 总规则 {len(open(part_file).readlines())} 条")
        with open(part_file, 'r', encoding='utf-8') as f:
            urls_to_check = [line.strip() for line in f if line.strip()]
        valid_urls = validate_dns(urls_to_check)

# ===============================
# 保存最终有效规则
# ===============================
valid_file = dist_dir / "blocklist_valid.txt"
with open(valid_file, 'w', encoding='utf-8') as f:
    f.write("\n".join(valid_urls))
print(f"🎯 最终有效规则保存到 {valid_file}, 共 {len(valid_urls)} 条")
