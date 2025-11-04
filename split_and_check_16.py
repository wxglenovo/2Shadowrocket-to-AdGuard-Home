#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"  # urls.txt 存放所有规则源 URL
TMP_DIR = "tmp"  # 临时分片目录
DIST_DIR = "dist"  # 处理后输出目录
MASTER_RULE = "merged_rules.txt"  # 合并后的主规则文件
PARTS = 16  # 分片总数
DNS_WORKERS = 50  # DNS 并发验证线程数
DNS_TIMEOUT = 2  # DNS 查询超时时间
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # 连续失败计数文件路径
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")  # 跳过验证计数文件路径
DELETE_THRESHOLD = 4  # 规则连续失败多少次后删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过多少次失败跳过 DNS 验证
SKIP_ROUNDS = 10  # 跳过验证的最大轮次
INCREMENTAL_MISS_LIMIT = 3  # 连续未写入次数阈值

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
# 统一剔除跳过验证规则
# ===============================
def unified_skip_remove(rules, delete_counter, skip_tracker):
    """
    统一剔除跳过验证规则，超过 SKIP_ROUNDS 自动恢复验证
    返回恢复验证规则列表，用于最后一个分片
    """
    remaining_rules = []
    restore_rules = []

    for r in rules:
        c = delete_counter.get(r, 0)
        if c <= SKIP_VALIDATE_THRESHOLD:
            remaining_rules.append(r)
            continue

        # 超过跳过阈值，不验证但计数
        skip_cnt = skip_tracker.get(r, 0) + 1
        skip_tracker[r] = skip_cnt

        new_del_cnt = c + 1
        delete_counter[r] = new_del_cnt

        print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={new_del_cnt}")

        # 超过删除阈值就不要加入剩余规则
        if new_del_cnt >= DELETE_THRESHOLD:
            continue

        remaining_rules.append(r)

        # 超过 SKIP_ROUNDS 恢复验证
        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r)
            remaining_rules.remove(r)
            restore_rules.append(r)

    # 更新计数文件
    save_delete_counter(delete_counter)
    save_skip_tracker(skip_tracker)
    return remaining_rules, restore_rules

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
                if line:
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    # 加载计数器
    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    # 统一剔除跳过验证规则
    merged_list = list(merged)
    merged_list, restore_rules = unified_skip_remove(merged_list, delete_counter, skip_tracker)

    # 将恢复验证规则排最后
    merged_list.extend(restore_rules)

    print(f"✅ 合并 {len(merged_list)} 条规则（恢复验证规则已排最后）")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged_list)))

    # 分片处理
    split_parts()
    return True

# ===============================
# 分片模块
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
        with open(filename, "a", encoding="utf-8") as f:  # 增量写入
            f.write("\n".join(part_rules) + "\n")
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

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
    total_lines = len(lines)
    start_time = time.time()
    for i in range(0, total_lines, batch_size):
        batch = lines[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule): rule for rule in batch}
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result:
                    valid.append(result)

                if done % 50 == 0 or done == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + done) / elapsed if elapsed > 0 else 0
                    remaining = total_lines - (i + done)
                    eta = remaining / speed if speed > 0 else 0
                    print(f"✅ 已验证 {i + done}/{total_lines} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")
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
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（不剔除注释）")

    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    rules_to_validate = []
    final_rules = set(old_rules)  # 保持增量更新
    added_count = 0
    removed_count = 0
    incremental_miss = {}

    # 统一剔除跳过验证规则
    lines, restore_rules = unified_skip_remove(lines, delete_counter, skip_tracker)

    # DNS 验证
    valid = set(dns_validate(lines + restore_rules))

    # 处理规则
    for rule in lines + restore_rules:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
            incremental_miss[rule] = 0
            print(f"✅ 验证通过：{rule}")
        else:
            # 连续未写入计数
            miss_cnt = incremental_miss.get(rule, 0) + 1
            incremental_miss[rule] = miss_cnt
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            print(f"⚠ 验证失败：{rule} | 连续未写入 {miss_cnt} 次 | 连续失败 {delete_counter[rule]}/{DELETE_THRESHOLD}")
            if miss_cnt >= INCREMENTAL_MISS_LIMIT or delete_counter[rule] >= DELETE_THRESHOLD:
                removed_count += 1
                if rule in final_rules:
                    final_rules.remove(rule)
                print(f"🔥 删除规则：{rule}")

    # 保存计数器
    save_delete_counter(delete_counter)
    save_skip_tracker(skip_tracker)

    # 增量更新 validated_part 文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)) + "\n")

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

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
