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
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")  # 连续未写入计数文件
DELETE_THRESHOLD = 4  # 规则连续失败多少次后删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过多少次失败跳过 DNS 验证
SKIP_ROUNDS = 10  # 跳过验证的最大轮次
MAX_NOT_WRITTEN = 3  # 连续三次未写入删除

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 工具函数：加载/保存 JSON
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# 跳过验证统一剔除（unified skip remove）
# ===============================
def unified_skip_remove(rules):
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    rules_to_validate = []
    recovered_rules = []

    for r in rules:
        del_cnt = delete_counter.get(r, 0)
        skip_cnt = skip_tracker.get(r, 0)

        if del_cnt >= SKIP_VALIDATE_THRESHOLD:
            # 超过阈值，统一剔除，计数累加
            del_cnt += 1
            skip_cnt += 1
            delete_counter[r] = del_cnt
            skip_tracker[r] = skip_cnt
            not_written_counter[r] = not_written_counter.get(r, 0) + 1

            print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={del_cnt}")

            # 达到 SKIP_ROUNDS 自动恢复验证
            if skip_cnt >= SKIP_ROUNDS:
                print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
                delete_counter[r] = 6
                skip_tracker.pop(r, None)
                recovered_rules.append(r)
        else:
            rules_to_validate.append(r)

    # 更新文件
    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    return rules_to_validate, recovered_rules

# ===============================
# 下载与合并规则模块（HOSTS 转换已移除）
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

    # 先剔除跳过验证规则
    rules_to_validate, recovered_rules = unified_skip_remove(list(merged))

    # 恢复验证规则排到最后一个分片
    merged_ordered = rules_to_validate + recovered_rules

    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged_ordered)))
    return True

# ===============================
# 分片模块（Split Parts）
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
        batch = lines[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule): rule for rule in batch}
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result:
                    valid.append(result)
                if done % 500 == 0 or done == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + done) / elapsed if elapsed > 0 else 0
                    eta = (total_lines - (i + done)) / speed if speed > 0 else 0
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
        split_parts()
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

    delete_counter = load_json(DELETE_COUNTER_FILE)
    skip_tracker = load_json(SKIP_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    rules_to_validate = []
    final_rules = set()
    added_count = 0
    removed_count = 0

    for r in lines:
        c = delete_counter.get(r, 0)
        # 超过阈值直接跳过但计数
        if c > SKIP_VALIDATE_THRESHOLD:
            skip_cnt = skip_tracker.get(r, 0) + 1
            skip_tracker[r] = skip_cnt
            new_del_cnt = c + 1
            delete_counter[r] = new_del_cnt
            print(f"⚠ 跳过验证：{r} （跳过 {skip_cnt}/{SKIP_ROUNDS} 次，连续失败 {new_del_cnt}/{DELETE_THRESHOLD} 次）")
            # 跳过次数达到上限恢复验证
            if skip_cnt >= SKIP_ROUNDS:
                print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
                delete_counter[r] = 6
                skip_tracker.pop(r, None)
                rules_to_validate.append(r)
            continue
        rules_to_validate.append(r)

    valid = set(dns_validate(rules_to_validate))

    all_rules = old_rules | set(lines)
    new_delete_counter = delete_counter.copy()

    for rule in all_rules:
        if rule in valid or rule in final_rules:
            final_rules.add(rule)
            new_delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
            # 连续未写入计数重置
            not_written_counter.pop(rule, None)
            continue

        old_count = delete_counter.get(rule, 0)
        new_count = old_count + 1
        new_delete_counter[rule] = new_count
        not_written_counter[rule] = not_written_counter.get(rule, 0) + 1

        print(f"⚠ 连续失败 +1 → {new_count}/{DELETE_THRESHOLD} ：{rule}")

        if new_count >= DELETE_THRESHOLD or not_written_counter.get(rule, 0) >= MAX_NOT_WRITTEN:
            removed_count += 1
            print(f"🔥 删除规则：{rule}")
            continue

        final_rules.add(rule)

    save_json(DELETE_COUNTER_FILE, new_delete_counter)
    save_json(SKIP_FILE, skip_tracker)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    # 每次增量更新 validated_part_*.txt
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主函数（Main）
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
