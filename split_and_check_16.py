#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 1️⃣ 配置参数
# ===============================
URLS_TXT = "urls.txt"               # 存放规则源地址
TMP_DIR = "tmp"                      # 临时目录，用于存储分片
DIST_DIR = "dist"                    # 输出目录，用于存储验证后的规则
MASTER_RULE = "merged_rules.txt"     # 合并后的规则文件
PARTS = 16                           # 分片数量
DNS_WORKERS = 50                     # 并发 DNS 查询线程数
DNS_TIMEOUT = 2                      # DNS 查询超时时间（秒）
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # 删除计数文件
DELETE_THRESHOLD = 4                 # 连续验证失败几次才真正删除规则

# 创建必要目录
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 2️⃣ 下载与合并规则源
# ===============================
def download_all_sources():
    """
    下载 urls.txt 中的所有规则源并合并为 MASTER_RULE
    """
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
                if line and not line.startswith("#"):  # 忽略空行和注释
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    return True

# ===============================
# 3️⃣ 分片规则
# ===============================
def split_parts():
    """
    将 MASTER_RULE 拆分为 PARTS 个分片，存储到 TMP_DIR
    """
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
# 4️⃣ DNS 验证
# ===============================
def check_domain(rule):
    """
    验证单条规则的域名是否可解析
    """
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT

    # 提取域名：去掉前导 |、去掉 ^ 和 *
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None

    try:
        resolver.resolve(domain)
        return rule  # DNS 成功，返回规则
    except:
        return None  # DNS 失败，返回 None

def dns_validate(lines):
    """
    并发验证规则列表，返回可用规则列表
    """
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
# 5️⃣ 删除计数管理
# ===============================
def load_delete_counter():
    """
    读取 delete_counter.json 文件，返回字典
    """
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print(f"⚠ {DELETE_COUNTER_FILE} 解析失败，重建空计数")
            return {}
    else:
        print(f"⚠ {DELETE_COUNTER_FILE} 不存在，创建新文件")
        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        return {}

def save_delete_counter(counter):
    """
    保存删除计数字典到 delete_counter.json
    """
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# 6️⃣ 分片处理逻辑
# ===============================
def process_part(part):
    """
    对指定分片进行 DNS 验证，并管理删除计数
    """
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")

    # 分片不存在时，尝试重新下载并切片
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

    # 读取之前验证的规则
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    # 遍历旧规则 + 当前分片所有规则
    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            if rule in delete_counter and delete_counter[rule] > 0:
                print(f"🔄 验证成功，清零删除计数: {rule}")
            new_delete_counter[rule] = 0
        else:
            count = delete_counter.get(rule, 0) + 1
            new_delete_counter[rule] = count
            print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
            if count >= DELETE_THRESHOLD:
                removed_count += 1
                # 不加入 final_rules
            else:
                final_rules.add(rule)
        if rule not in old_rules and rule in valid:
            added_count += 1

    # 保存删除计数
    save_delete_counter(new_delete_counter)

    # 写入最终验证结果
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 7️⃣ 主程序入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    # 强制更新规则源
    if args.force_update:
        download_all_sources()
        split_parts()

    # 确保规则文件和首个分片存在
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        split_parts()

    # 如果指定分片，则处理该分片
    if args.part:
        process_part(args.part)
