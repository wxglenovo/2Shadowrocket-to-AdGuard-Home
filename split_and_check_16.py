#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AdGuard / DNS 规则管理脚本（最终版）
功能：
1. 下载规则源并合并
2. 将合并规则拆分为多个分片（去掉注释行）
3. 使用 DNS 验证规则有效性
4. 自动维护删除计数和跳过验证机制
5. 清理 delete_counter 和 skip_tracker 中已删除规则
"""

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置区
# ===============================
URLS_TXT = "urls.txt"  # 规则源列表文件
TMP_DIR = "tmp"        # 临时分片存放目录
DIST_DIR = "dist"      # 验证后的分片存放目录
MASTER_RULE = "merged_rules.txt"  # 合并后的规则文件
PARTS = 16             # 分片数量
DNS_WORKERS = 50       # DNS 验证并发数量
DNS_TIMEOUT = 2        # DNS 查询超时时间（秒）
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # 删除计数文件
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")  # 跳过验证记录文件

DELETE_THRESHOLD = 4         # 连续失败次数超过此值则从列表中删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过此值则暂时跳过验证
SKIP_ROUNDS = 10             # 跳过验证的最大轮数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# Skip Tracker（跳过验证机制）
# ===============================
def load_skip_tracker():
    """加载跳过验证记录"""
    if os.path.exists(SKIP_FILE):
        try:
            with open(SKIP_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(SKIP_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_skip_tracker(data):
    """保存跳过验证记录"""
    with open(SKIP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# Delete Counter（删除计数机制）
# ===============================
def load_delete_counter():
    """加载规则连续失败计数"""
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_delete_counter(counter):
    """保存规则连续失败计数"""
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# 下载与合并规则（简化版）
# ===============================
def download_all_sources():
    """
    下载 urls.txt 中的所有规则源
    不做 HOSTS -> AdGuard 转换，也不拆分多域名
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
                # 直接跳过注释行
                if not line or line.startswith("#") or line.startswith("!"):
                    continue
                merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片处理（去掉注释）
# ===============================
def split_parts():
    """
    将合并规则拆分为多个分片
    注：分片时已过滤掉注释行（! 或 # 开头）
    """
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip() and not (l.startswith("!") or l.startswith("#"))]

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
# DNS 验证模块
# ===============================
def check_domain(rule):
    """检查单条规则的域名是否可解析"""
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
    """并发 DNS 验证规则有效性"""
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
# 核心处理分片逻辑
# ===============================
def process_part(part):
    """
    处理单个分片：
    1. 加载规则
    2. DNS 验证（跳过规则逻辑）
    3. 更新删除计数
    4. 清理 delete_counter 和 skip_tracker 中已删除规则
    5. 保存验证后的分片（去掉注释行）
    """
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    # 加载当前分片规则（过滤注释）
    lines = [l for l in open(part_file, "r", encoding="utf-8").read().splitlines()
             if not l.startswith("!") and not l.startswith("#")]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（已过滤注释）")

    # 加载已有验证结果
    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if not l.startswith("!") and not l.startswith("#")])

    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    # 构建待验证列表
    rules_to_validate = []
    for r in lines:
        c = delete_counter.get(r, None)
        if c is None or c <= SKIP_VALIDATE_THRESHOLD:
            rules_to_validate.append(r)
            continue

        skip_cnt = skip_tracker.get(r, 0)
        skip_cnt += 1
        skip_tracker[r] = skip_cnt
        print(f"⏩ 跳过验证 {r}（次数 {skip_cnt}/{SKIP_ROUNDS}）")

        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 恢复验证：{r}（跳过达到 {SKIP_ROUNDS} 次 → 重置计数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r)
            rules_to_validate.append(r)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate))

    # 更新规则集和删除计数
    final_rules = set()
    added_count = 0
    removed_count = 0
    all_rules = old_rules | set(lines)
    new_delete_counter = delete_counter.copy()

    for rule in all_rules:
        if rule in valid:
            final_rules.add(rule)
            new_delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
            continue

        old_count = delete_counter.get(rule, None)
        new_count = 4 if old_count is None else old_count + 1
        new_delete_counter[rule] = new_count
        print(f"⚠ 连续失败计数 = {new_count} ：{rule}")

        if new_count >= DELETE_THRESHOLD:
            removed_count += 1
            continue
        final_rules.add(rule)

    # ===============================
    # 清理 delete_counter 和 skip_tracker 中已删除规则
    # ===============================
    all_current_rules = set(lines)
    removed_from_counter = []
    removed_from_skip = []

    for rule in list(new_delete_counter.keys()):
        if rule not in all_current_rules:
            new_delete_counter.pop(rule)
            removed_from_counter.append(rule)

    for rule in list(skip_tracker.keys()):
        if rule not in all_current_rules:
            skip_tracker.pop(rule)
            removed_from_skip.append(rule)

    if removed_from_counter or removed_from_skip:
        print(f"🗑 清理 delete_counter {len(removed_from_counter)} 条，skip_tracker {len(removed_from_skip)} 条已删除的规则")

    # 保存更新后的计数和跳过记录
    save_delete_counter(new_delete_counter)
    save_skip_tracker(skip_tracker)

    # 保存最终分片，去掉注释
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted([r for r in final_rules if not r.startswith("!") and not r.startswith("#")])))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主程序入口
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
