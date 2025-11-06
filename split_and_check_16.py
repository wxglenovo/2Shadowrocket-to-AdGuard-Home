#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading

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
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")  # 未写入计数文件
DELETE_THRESHOLD = 4  # 删除计数阈值
DNS_BATCH_SIZE = 500  # 每批验证条数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)  # 确保 dist 目录存在

lock = threading.Lock()  # 线程锁，用于安全更新 not_written_counter.json

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
# 下载并合并规则源
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

    filtered_rules, updated_delete_counter = filter_and_update_high_delete_count_rules(merged)
    split_parts(filtered_rules)
    save_json(DELETE_COUNTER_FILE, updated_delete_counter)
    return True

# ===============================
# 删除计数处理
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            print(f"⚠ 删除计数 >=7，跳过规则：{rule} | 删除计数={del_cnt}")
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 17:
                updated_delete_counter[rule] = 5
                print(f"🔁 删除计数达到17，重置规则：{rule} 的删除计数为5")

    return low_delete_count_rules, updated_delete_counter

# ===============================
# 分片
# ===============================
def split_parts(merged_rules):
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part_rules = list(merged_rules)[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

# ===============================
# 并行 DNS 验证
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

def dns_validate(rules):
    valid_rules = []
    total_rules = len(rules)
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in rules}
        completed = 0
        start_time = time.time()
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_rules.append(result)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed
                eta = (total_rules - completed) / speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")
    return valid_rules

# ===============================
# 并行更新未写入规则计数
# ===============================
def update_not_written_counter_multithread(rules, valid, part_name):
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    def process_rule(rule):
        with lock:
            if rule not in not_written_counter:
                not_written_counter[rule] = {}
            if part_name not in not_written_counter[rule]:
                not_written_counter[rule][part_name] = 3
            if valid.get(rule, False):
                not_written_counter[rule][part_name] = 3
            else:
                not_written_counter[rule][part_name] = max(0, not_written_counter[rule][part_name] - 1)

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = [executor.submit(process_rule, r) for r in rules]
        for f in as_completed(futures):
            f.result()

    # 清理 write_counter = 0
    for rule in list(not_written_counter.keys()):
        for part in list(not_written_counter[rule].keys()):
            if not_written_counter[rule][part] == 0:
                del not_written_counter[rule][part]
        if not not_written_counter[rule]:
            del not_written_counter[rule]

    save_json(NOT_WRITTEN_FILE, not_written_counter)

# ===============================
# 核心处理分片
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，拉取规则中…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8") if l.strip()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    final_rules = set(old_rules)

    # 筛选删除计数 < 7 的规则
    for r in lines:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = del_cnt + 1
            print(f"⚠ 删除计数 >=7，跳过规则：{r} | 删除计数={del_cnt}")

    # 并行 DNS 验证
    valid_list = dns_validate(rules_to_validate)
    valid_dict = {r: r in valid_list for r in rules_to_validate}

    # 并行更新未写入计数
    update_not_written_counter_multithread(rules_to_validate, valid_dict, f"validated_part_{part}")

    # 更新删除计数和最终规则
    added_count = 0
    removed_count = 0
    for rule in rules_to_validate:
        if valid_dict[rule]:
            final_rules.add(rule)
            delete_counter[rule] = 0
            added_count += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            print(f"⚠ 连续失败 +1 → {delete_counter[rule]}/{DELETE_THRESHOLD} ：{rule}")
            if delete_counter[rule] >= DELETE_THRESHOLD:
                removed_count += 1
                print(f"🔥 连续失败达到阈值 → 删除规则：{rule}")
                final_rules.discard(rule)

    # 保存
    save_json(DELETE_COUNTER_FILE, delete_counter)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

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
    else:
        # 默认处理所有分片
        for p in range(1, PARTS + 1):
            process_part(p)
