#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")
DELETE_THRESHOLD = 4
SKIP_VALIDATE_THRESHOLD = 7
SKIP_ROUNDS = 10

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 跳过验证计数器模块
# ===============================
def load_skip_tracker():
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

def save_skip_tracker(update_data):
    old_data = {}
    if os.path.exists(SKIP_FILE):
        try:
            with open(SKIP_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
        except:
            old_data = {}
    for k, v in update_data.items():
        old_data[k] = v
    with open(SKIP_FILE, "w", encoding="utf-8") as f:
        json.dump(old_data, f, indent=2, ensure_ascii=False)

# ===============================
# 连续失败计数器模块
# ===============================
def load_delete_counter():
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

def save_delete_counter(update_data):
    old_data = {}
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
        except:
            old_data = {}
    for k, v in update_data.items():
        old_data[k] = v
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(old_data, f, indent=2, ensure_ascii=False)

# ===============================
# 下载与合并规则模块
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
                if not line:
                    continue
                merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 统一剔除跳过验证规则
# ===============================
def unified_skip_remove(rules, delete_counter, skip_tracker):
    remaining_rules = []
    restore_rules = []

    for r in rules:
        old_skip = skip_tracker.get(r, 0)
        skip_cnt = old_skip + 1
        old_del = delete_counter.get(r, 0)
        new_del_cnt = old_del + 1

        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r, None)
            restore_rules.append(r)
            remaining_rules.append(r)  # 放最后一个分片
        else:
            skip_tracker[r] = skip_cnt
            delete_counter[r] = new_del_cnt
            print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_tracker[r]} | 删除计数={delete_counter[r]}")
            remaining_rules.append(r)

    save_delete_counter(delete_counter)
    save_skip_tracker(skip_tracker)
    return remaining_rules, restore_rules

# ===============================
# 分片模块
# ===============================
def split_parts(all_rules, restore_rules):
    total_rules = all_rules + restore_rules  # 恢复验证的规则排在最后
    total = len(total_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条")

    for i in range(PARTS):
        part_rules = total_rules[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

# ===============================
# DNS 验证模块
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
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，批量 500 条规则")
    valid = []
    batch_size = 500
    start_time = time.time()
    for i in range(0, len(lines), batch_size):
        batch = lines[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule): rule for rule in batch}
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result:
                    valid.append(result)
                # 进度日志每 50 条打印一次
                if done % 50 == 0 or done == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + done) / elapsed
                    eta = (len(lines) - (i + done)) / speed
                    print(f"✅ 已验证 {i + done}/{len(lines)} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")
    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ===============================
# 核心处理分片逻辑
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        # 重新剔除跳过规则
        rules = []
        if os.path.exists(MASTER_RULE):
            with open(MASTER_RULE, "r", encoding="utf-8") as f:
                rules = [l.strip() for l in f if l.strip()]
        delete_counter = load_delete_counter()
        skip_tracker = load_skip_tracker()
        rules, restore_rules = unified_skip_remove(rules, delete_counter, skip_tracker)
        split_parts(rules, restore_rules)
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").readlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（增量更新）")

    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    # 先剔除跳过规则
    remaining, restore_rules = unified_skip_remove(lines, delete_counter, skip_tracker)
    rules_to_validate.extend(remaining)
    rules_to_validate.extend(restore_rules)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate))

    # 更新增量 validated_part 文件
    for rule in rules_to_validate:
        if rule in valid or rule in final_rules:
            final_rules.add(rule)
            delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
        else:
            new_count = delete_counter.get(rule, 0) + 1
            delete_counter[rule] = new_count
            print(f"⚠ 连续失败 +1 → {new_count}/{DELETE_THRESHOLD} ：{rule}")
            if new_count >= DELETE_THRESHOLD:
                removed_count += 1
                continue
            final_rules.add(rule)

    # 删除连续三次未写入的规则
    to_remove = []
    for rule in final_rules:
        if delete_counter.get(rule, 0) >= 3 and rule not in valid:
            to_remove.append(rule)
            removed_count += 1
            print(f"🗑 连续三次未写入 → 删除规则：{rule}")
    for r in to_remove:
        final_rules.remove(r)
        delete_counter.pop(r, None)
        skip_tracker.pop(r, None)

    # 保存计数器
    save_delete_counter(delete_counter)
    save_skip_tracker(skip_tracker)

    # 保存增量 validated_part 文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
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
        # 统一剔除跳过验证后再分片
        rules = []
        if os.path.exists(MASTER_RULE):
            with open(MASTER_RULE, "r", encoding="utf-8") as f:
                rules = [l.strip() for l in f if l.strip()]
        delete_counter = load_delete_counter()
        skip_tracker = load_skip_tracker()
        rules, restore_rules = unified_skip_remove(rules, delete_counter, skip_tracker)
        split_parts(rules, restore_rules)

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        rules = []
        if os.path.exists(MASTER_RULE):
            with open(MASTER_RULE, "r", encoding="utf-8") as f:
                rules = [l.strip() for l in f if l.strip()]
        delete_counter = load_delete_counter()
        skip_tracker = load_skip_tracker()
        rules, restore_rules = unified_skip_remove(rules, delete_counter, skip_tracker)
        split_parts(rules, restore_rules)

    if args.part:
        process_part(args.part)
