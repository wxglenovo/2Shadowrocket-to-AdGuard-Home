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
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50           # 50线程并发
BATCH_SIZE = 500           # 每批处理500条规则
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")
DELETE_THRESHOLD = 4
SKIP_VALIDATE_THRESHOLD = 7
SKIP_ROUNDS = 10
NOT_WRITTEN_THRESHOLD = 3

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写工具
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
# 跳过验证统一剔除
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
            # 超过阈值，统一剔除
            del_cnt += 1
            skip_cnt += 1
            delete_counter[r] = del_cnt
            skip_tracker[r] = skip_cnt
            not_written_counter[r] = not_written_counter.get(r, 0) + 1

            print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={del_cnt}")

            # 超过 SKIP_ROUNDS 自动恢复验证
            if skip_cnt >= SKIP_ROUNDS:
                print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
                delete_counter[r] = 6
                skip_tracker.pop(r, None)
                recovered_rules.append(r)
        else:
            rules_to_validate.append(r)

    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    return rules_to_validate, recovered_rules

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
                if line:
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片处理
# ===============================
def split_parts(additional_rules=None):
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    if additional_rules:
        # 将恢复验证的规则放在最后一个分片
        rules.extend(additional_rules)

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
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，每批 {BATCH_SIZE} 条规则")
    valid = []
    start_time = time.time()

    for i in range(0, len(lines), BATCH_SIZE):
        batch = lines[i:i+BATCH_SIZE]
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
                    speed = done / elapsed if elapsed > 0 else 0
                    remaining = len(lines) - (i + done)
                    eta = remaining / speed if speed > 0 else 0
                    print(f"✅ 已验证 {i + done}/{len(lines)} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")

    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ===============================
# 处理分片
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

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（不剔除注释）")

    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    skip_tracker = load_json(SKIP_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    # 先统一剔除跳过验证
    lines_to_validate, recovered_rules = unified_skip_remove(lines)

    # 将恢复验证规则加入最后一个分片
    if recovered_rules and int(part) == PARTS:
        lines_to_validate.extend(recovered_rules)

    # DNS 验证
    valid = set(dns_validate(lines_to_validate))

    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
            not_written_counter[rule] = 0
            continue

        # DNS 失败
        old_count = delete_counter.get(rule, 0)
        new_count = old_count + 1
        delete_counter[rule] = new_count
        not_written_counter[rule] = not_written_counter.get(rule, 0) + 1

        print(f"⚠ 连续失败 +1 → {new_count}/{DELETE_THRESHOLD} ：{rule}")

        if new_count >= DELETE_THRESHOLD:
            removed_count += 1
            continue
        final_rules.add(rule)

        # 连续三次未写入删除
        if not_written_counter[rule] >= NOT_WRITTEN_THRESHOLD:
            print(f"🔥 连续三次未写入 → 删除规则：{rule}")
            final_rules.discard(rule)
            removed_count += 1
            not_written_counter.pop(rule, None)

    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(SKIP_FILE, skip_tracker)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    # 增量更新 validated_part_*.txt
    existing = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            existing = set([l.strip() for l in f if l.strip()])

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules | existing)))

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
        # 下载后先统一剔除跳过验证
        if os.path.exists(MASTER_RULE):
            with open(MASTER_RULE, "r", encoding="utf-8") as f:
                all_rules = [l.strip() for l in f if l.strip()]
            remaining, recovered = unified_skip_remove(all_rules)
            split_parts(additional_rules=recovered)
        else:
            split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        with open(MASTER_RULE, "r", encoding="utf-8") as f:
            all_rules = [l.strip() for l in f if l.strip()]
        remaining, recovered = unified_skip_remove(all_rules)
        split_parts(additional_rules=recovered)

    if args.part:
        process_part(args.part)
