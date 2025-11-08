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
MASTER_RULE = os.path.join(DIST_DIR, "merged_rules.txt")
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 500
WRITE_COUNTER_MAX = 6

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写
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
    print(f"✅ 已保存 {path}")

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
# 过滤删除计数>=7 的规则
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_rules = set()
    updated = delete_counter.copy()

    skipped = 0
    reset = 0

    for rule in all_rules_set:
        cnt = delete_counter.get(rule, 4)
        if cnt < 7:
            low_rules.add(rule)
        else:
            updated[rule] = cnt + 1
            skipped += 1
            if updated[rule] >= 24:
                updated[rule] = 5
                reset += 1

    print(f"🔢 跳过 {skipped} 条删除计数>=7 的规则，不参与验证")
    print(f"🔁 重置 {reset} 条删除计数>=24 的规则为5")

    return low_rules, updated

# ===============================
# 分片
# ===============================
def split_parts(merged_rules):
    merged_rules = list(sorted(merged_rules))
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条规则")

    for i in range(PARTS):
        part_rules = merged_rules[i * per_part:(i+1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

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

def dns_validate(rules, part):
    valid_rules = []
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")
    total = len(rules)

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in rules}
        done = 0
        start = time.time()
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                valid_rules.append(res)
            done += 1
            if done % DNS_BATCH_SIZE == 0 or done == total:
                speed = done / (time.time()-start)
                eta = (total-done)/speed if speed > 0 else 0
                print(f"✅ 已验证 {done}/{total} | 有效 {len(valid_rules)} | 速度 {speed:.1f}/秒 | ETA {eta:.1f}s")

    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(valid_rules)))

    return set(valid_rules)

# ===============================
# 更新 not_written_counter + 生成 validated_part_X.txt
# ===============================
def update_not_written_counter(part):
    part_key = f"validated_part_{part}"
    counter = load_json(NOT_WRITTEN_FILE)

    for i in range(1, PARTS+1):
        k = f"validated_part_{i}"
        if k not in counter:
            counter[k] = {}

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")

    existing = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            existing = set([l.strip() for l in f if l.strip()])

    tmp_rules = set()
    if os.path.exists(tmp_file):
        with open(tmp_file, "r", encoding="utf-8") as f:
            tmp_rules = set([l.strip() for l in f if l.strip()])

    part_counter = counter[part_key]

    # ✅ 出现在 tmp 的全部写入(生成validated文件的最终内容)
    for rule in tmp_rules:
        part_counter[rule] = WRITE_COUNTER_MAX

    # ✅ 原 validated 中存在，但新 tmp 缺失 → write_counter-- → ≤3 删除
    for rule in list(existing):
        if rule not in tmp_rules:
            if rule in part_counter:
                part_counter[rule] -= 1
            else:
                part_counter[rule] = WRITE_COUNTER_MAX - 1

            if part_counter[rule] <= 3:
                print(f"🔥 write_counter ≤3 → 删除：{rule}")
                del part_counter[rule]

    # ✅ 覆盖写入 validated 文件
    final_rules = sorted(list(part_counter.keys()))
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))

    counter[part_key] = part_counter
    save_json(NOT_WRITTEN_FILE, counter)

    return len(tmp_rules), len(existing), len(final_rules)

# ===============================
# 主分片处理逻辑
# ===============================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则...")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，停止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8") if l.strip()]
    print(f"⏱ 分片 {part} 共 {len(lines)} 条规则")

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    skipped = 0

    for r in lines:
        cnt = delete_counter.get(r, 4)
        if cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = cnt + 1
            skipped += 1

    print(f"🚫 跳过 {skipped} 条删除计数>=7 的规则")
    valid = dns_validate(rules_to_validate, part)

    added = 0
    removed = 0

    for rule in rules_to_validate:
        if rule in valid:
            delete_counter[rule] = 0
            added += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            if delete_counter[rule] >= DELETE_THRESHOLD:
                removed += 1

    save_json(DELETE_COUNTER_FILE, delete_counter)

    # ✅ 写入 validated + JSON 同步
    v_new, v_old, v_final = update_not_written_counter(part)

    print(f"✅ 分片 {part} 完成：总 {v_final}, 新增 {added}, 删除 {removed}, 跳过 {skipped}")
    print(f"COMMIT_STATS:总{v_final},新增{added},删除{removed},跳过{skipped}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
