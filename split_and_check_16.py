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
# 并行提取规则与更新删除计数
# ===============================
def process_rules_parallel(all_rules_set, delete_counter):
    with ThreadPoolExecutor(max_workers=2) as executor:
        # 提交提取规则任务（删除计数 < 7）
        future_extract = executor.submit(extract_valid_rules, all_rules_set, delete_counter)
        # 提交更新删除计数任务（删除计数 >= 7）
        future_update = executor.submit(update_delete_count, all_rules_set, delete_counter)
        
        # 获取任务执行结果
        valid_rules = future_extract.result()
        update_result = future_update.result()

    return valid_rules, update_result

def extract_valid_rules(all_rules_set, delete_counter):
    valid_rules = []
    for r in all_rules_set:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt >= 7:
            continue
        valid_rules.append(r)
    return valid_rules

def update_delete_count(all_rules_set, delete_counter):
    for r in all_rules_set:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt >= 17:
            print(f"⚠ 删除计数达到 17，重置为 6：{r} | 删除计数={del_cnt}")
            delete_counter[r] = 6
        elif del_cnt >= 7:
            delete_counter[r] = del_cnt + 1
    save_json(DELETE_COUNTER_FILE, delete_counter)

# ===============================
# DNS 验证函数
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

# ===============================
# 验证并打印完整日志
# ===============================
def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，每批 {DNS_BATCH_SIZE} 条规则")
    valid = []
    start_time = time.time()

    # 分批处理
    for i in range(0, len(lines), DNS_BATCH_SIZE):
        batch = lines[i:i + DNS_BATCH_SIZE]

        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, r): r for r in batch}

            completed = 0
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                if result:
                    valid.append(result)

                # 每500条打印一次
                if completed % 500 == 0 or completed == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + completed) / elapsed
                    eta = (len(lines) - (i + completed)) / speed if speed > 0 else 0
                    print(f"✅ 已验证 {i + completed}/{len(lines)} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")

    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ===============================
# 统一剔除删除计数 >= 7 的规则
# ===============================
def unified_skip_remove(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    valid_rules, _ = process_rules_parallel(all_rules_set, delete_counter)  # 并行执行提取和删除计数更新
    save_json(DELETE_COUNTER_FILE, delete_counter)
    return valid_rules

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
