#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"               # 存放规则源地址
TMP_DIR = "tmp"
DIST_DIR = "dist"                   # 修改为小写 dist 目录
MASTER_RULE = "merged_rules.txt"    # 合并后的规则文件
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

# 确保 dist 目录存在并且具有写权限
if not os.path.exists(DIST_DIR):
    print(f"⚠ {DIST_DIR} 目录不存在")
else:
    print(f"📂 {DIST_DIR} 目录存在")

# 确保 dist 目录有写权限
if not os.access(DIST_DIR, os.W_OK):
    print(f"❌ 没有写入权限：{DIST_DIR}")
else:
    print(f"✅ 具有写入权限：{DIST_DIR}")

# 创建目录，确保 dist 目录存在
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)  # 确保 dist 目录存在

# ===============================
# 下载与合并规则
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
                    merged.add(line)
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
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证")
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        total = len(lines)
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid

# ===============================
# 删除计数管理
# ===============================
def load_delete_counter():
    """加载删除计数器"""
    if not os.path.exists(DELETE_COUNTER_FILE):
        print(f"🔄 文件不存在：{DELETE_COUNTER_FILE}. 创建新文件。")
        # 强制创建一个空字典文件
        with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        print("📂 delete_counter.json 文件已创建")
        return {}

    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        counter = json.load(f)
        print(f"🔄 加载已有删除计数：{counter}")  # 调试日志，查看计数文件内容
        return counter

def save_delete_counter(counter):
    """保存删除计数器"""
    print(f"💾 正在保存删除计数：{counter}")  # 调试日志，确认保存的计数
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)
    print(f"💾 已保存删除计数：{counter}")  # 确认保存成功

# ===============================
# 分片处理
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

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")
    valid = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            if rule in delete_counter:
                print(f"🔄 验证成功，清零删除计数: {rule}")
            new_delete_counter[rule] = 0
        else:
            # 当前规则的删除计数应累计
            current_count = delete_counter.get(rule, 0)  # 获取当前的删除计数
            count = current_count + 1  # 累加计数
            new_delete_counter[rule] = count  # 更新计数
            print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")  # 调试输出
            if count >= DELETE_THRESHOLD:
                removed_count += 1
            else:
                final_rules.add(rule)

        if rule not in old_rules and rule in valid:
            added_count += 1

    # 保存更新后的计数器
    save_delete_counter(new_delete_counter)

    # 保存验证后的规则
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    # 💾 输出给 workflow 用作 commit 信息
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
