#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict

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
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")  # 连续未写入计数
DELETE_THRESHOLD = 4  # 连续失败多少次后删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过多少次失败跳过 DNS 验证（删除计数 >= 7）
SKIP_ROUNDS = 10  # 跳过验证的最大轮次，超过后恢复验证
DNS_BATCH_SIZE = 500  # 每批验证条数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写封装
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# 下载源并合并
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

    print(f"✅ 合并 {len(merged)} 条规则")

    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    recovered_rules = unified_skip_remove(merged)
    split_parts(recovered_rules)
    return True

# ===============================
# 统一剔除跳过验证模块（核心）
# ===============================
def unified_skip_remove(all_rules_set):
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)
    recovered_rules = []

    log_count = defaultdict(int)  # 记录每个日志出现次数

    for r in list(all_rules_set):
        del_cnt = delete_counter.get(r, 0)
        skip_cnt = skip_tracker.get(r, 0)

        # 只有删除计数 >= SKIP_VALIDATE_THRESHOLD 才跳过验证
        if del_cnt < SKIP_VALIDATE_THRESHOLD:
            continue

        # 累加跳过次数（从文件中读取后 +1）
        skip_cnt += 1
        skip_tracker[r] = skip_cnt

        # 删除计数继续 +1（历史累加）
        del_cnt += 1
        delete_counter[r] = del_cnt

        # 严格日志
        log_msg = f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={del_cnt}"
        if log_count[log_msg] < 20:  # 如果该日志没有超过20次，打印
            print(log_msg)
            log_count[log_msg] += 1
        elif log_count[log_msg] == 20:  # 打印次数达到20次时，显示数量
            print(f"⚠ 日志超出次数限制，显示数量：{log_msg}...")

        # 当跳过 >= SKIP_ROUNDS 时恢复验证
        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            skip_tracker.pop(r)
            delete_counter[r] = 6
            recovered_rules.append(r)

    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written_counter)
    return recovered_rules

# ===============================
# 分片
# ===============================
def split_parts(recovered_rules=None):
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少主规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    # 恢复验证的规则放在最后一个分片
    if recovered_rules:
        for r in recovered_rules:
            if r in rules:
                rules.remove(r)
        rules.extend(recovered_rules)

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
# 通过并行化提升恢复验证效率
# ===============================
def recover_validation(rules_to_recover):
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = [executor.submit(process_recovery, r) for r in rules_to_recover]
        for future in as_completed(futures):
            future.result()  # 等待所有恢复任务完成

def process_recovery(rule):
    # 恢复验证的具体操作
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    skip_tracker.pop(rule, None)
    delete_counter[rule] = 6  # 重置失败次数
    print(f"🔁 恢复验证：{rule}（重置连续失败次数=6）")

    # 这里可以增加其他恢复操作

    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)

# ===============================
# 主入口
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
