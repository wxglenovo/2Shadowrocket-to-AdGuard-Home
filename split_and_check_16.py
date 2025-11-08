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
        except Exception as e:
            print(f"⚠ 读取 {path} 时发生错误: {e}")
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ 已保存 {path}")
    except Exception as e:
        print(f"⚠ 保存 {path} 时发生错误: {e}")

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
# 删除计数 >= 7 的规则过滤
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    reset_count = 0
    skipped_count = 0
    skipped_rules = []
    reset_rules = []

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 24:
                updated_delete_counter[rule] = 5
                reset_count += 1
                reset_rules.append(rule)
            skipped_count += 1
            skipped_rules.append(rule)

    for rule in skipped_rules[:20]:
        print(f"⚠ 删除计数 ≥7，跳过验证：{rule}")

    print(f"🔢 共 {skipped_count} 条规则被跳过验证（删除计数≥7）")

    for rule in reset_rules[:20]:
        print(f"🔁 删除计数达到24，重置为 5：{rule}")

    print(f"🔢 共 {reset_count} 条规则已重置为 5")

    return low_delete_count_rules, updated_delete_counter

# ===============================
# 分片
# ===============================
def split_parts(merged_rules):
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条规则")

    for i in range(PARTS):
        part_rules = list(merged_rules)[i*per_part:(i+1)*per_part]
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
    except Exception:
        return None

def dns_validate(rules, part):
    valid_rules = []
    total_rules = len(rules)
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")

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
                eta = (total_rules - completed)/speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} | 速度 {speed:.1f}/秒 | ETA {eta:.1f} 秒")

    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(valid_rules)))

    return valid_rules

# ===============================
# ✅ 更新 not_written_counter.json（你要求的新版逻辑）
# ===============================
def update_not_written_counter(part_num):
    part_key = f"validated_part_{part_num}"
    counter = load_json(NOT_WRITTEN_FILE)

    # 保证所有 validated_part_X 存在
    for i in range(1, PARTS + 1):
        pk = f"validated_part_{i}"
        if pk not in counter:
            counter[pk] = {}

    validated_file = os.path.join(DIST_DIR, f"{part_key}.txt")
    tmp_file = os.path.join(TMP_DIR, f"vpart_{part_num}.tmp")

    # 已验证文件
    existing_rules = set()
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as vf:
            existing_rules = set([l.strip() for l in vf if l.strip()])

    # 临时验证成功文件
    tmp_rules = set()
    if os.path.exists(tmp_file):
        with open(tmp_file, "r", encoding="utf-8") as tf:
            tmp_rules = set([l.strip() for l in tf if l.strip()])

    part_counter = counter.get(part_key, {})

    # ✅ 1. tmp 中成功 -> write_counter = 4
    for rule in tmp_rules:
        part_counter[rule] = 4

    # ✅ 2. validated 中存在但 tmp 缺
    for rule in (existing_rules - tmp_rules):
        if rule in part_counter:
            part_counter[rule] -= 1
        else:
            part_counter[rule] = 3

    # ✅ 3. 从 validated 删除 write_counter <= 0
    deleted_from_validated = 0
    to_delete_validated = [r for r in existing_rules if part_counter.get(r, 0) <= 0]

    for rule in to_delete_validated[:20]:
        print(f"🔥 write_counter ≤ 0 - 将从 {validated_file} 删除：{rule}")

    deleted_from_validated = len(to_delete_validated)
    if deleted_from_validated > 0:
        print(f"🗑 本次从 {validated_file} 删除 共 {deleted_from_validated} 条")

    # 写回 validated 文件
    new_rules = [r for r in existing_rules if part_counter.get(r, 0) > 0]
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(new_rules))

    # ✅ 4. 从 JSON 删除 write_counter <= 0
    json_deleted = [r for r, v in list(part_counter.items()) if v <= 0]

    for rule in json_deleted[:20]:
        print(f"💥 write_counter ≤ 0 → 从 JSON 删除：{rule}")

    if json_deleted:
        print(f"🗑 本次从 JSON 删除 共 {len(json_deleted)} 条规则")
        for r in json_deleted:
            part_counter.pop(r, None)

    counter[part_key] = part_counter
    save_json(NOT_WRITTEN_FILE, counter)

    return deleted_from_validated

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新拉取规则…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    for r in lines:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = del_cnt + 1
            print(f"⚠ 删除计数 ≥7，跳过验证：{r}")

    valid = dns_validate(rules_to_validate, part)

    failure_counts = {}
    for rule in rules_to_validate:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            added_count += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            current_failure_count = delete_counter[rule]
            failure_counts[current_failure_count] = failure_counts.get(current_failure_count, 0) + 1
            if delete_counter[rule] >= DELETE_THRESHOLD:
                removed_count += 1
                final_rules.discard(rule)

    save_json(DELETE_COUNTER_FILE, delete_counter)

    for i in range(1, max(failure_counts.keys(), default=0) + 1):
        if failure_counts.get(i, 0) > 0:
            print(f"⚠ 连续失败 {i}/4 的规则条数: {failure_counts[i]}")

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    # ✅ 更新 write_counter、validated_x 删除、JSON 删除
    deleted_validated = update_not_written_counter(part)

    # ✅ 正确统计数量
    total_count = len(final_rules)
    deleted_count = deleted_validated
    filtered_count = len(rules_to_validate) - len(valid)

    print(f"✅ 分片 {part} 完成: 总{total_count}, 新增{added_count}, 删除{deleted_count}, 过滤{filtered_count}")
    print(f"COMMIT_STATS:总{total_count},新增{added_count},删除{deleted_count},过滤{filtered_count}")

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
