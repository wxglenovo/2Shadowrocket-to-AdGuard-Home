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
DELETE_THRESHOLD = 4           # 连续失败多少次删除
SKIP_VALIDATE_THRESHOLD = 7    # 连续失败 >= 7 → 跳过验证
SKIP_ROUNDS = 10               # 跳过多少轮后恢复验证

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 加载/更新工具
# ===============================
def safe_load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    with open(path, "w", encoding="utf-8") as f:
        json.dump({}, f, indent=2)
    return {}

def safe_update_json(path, new_data):
    old = safe_load_json(path)
    for k, v in new_data.items():
        old[k] = v
    with open(path, "w", encoding="utf-8") as f:
        json.dump(old, f, indent=2, ensure_ascii=False)

# ===============================
# ✅ 新增：下载合并后，先统一剔除“跳过验证规则”
# ===============================
def remove_skip_before_split(merged_rules):
    delete_counter = safe_load_json(DELETE_COUNTER_FILE)
    skip_tracker = safe_load_json(SKIP_FILE)

    result = []
    skipped = 0

    for rule in merged_rules:
        cnt = delete_counter.get(rule, 0)

        # 连续失败 >= 7 → 本轮不参与 DNS，不进入分片
        if cnt >= SKIP_VALIDATE_THRESHOLD:
            skipped += 1
            continue

        result.append(rule)

    print(f"⛔ 已统一剔除 {skipped} 条跳过验证规则（不参与分片与 DNS）")
    return result

# ===============================
# 下载 + 合并
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False

    print("📥 下载规则源...")
    merged = set()

    urls = [u.strip() for u in open(URLS_TXT, "r", encoding="utf-8") if u.strip()]

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

    print(f"✅ 原始合并规则 {len(merged)} 条")

    # ✅>>>>> 新增：统一剔除跳过验证
    merged = remove_skip_before_split(sorted(merged))

    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(merged))
    print(f"✅ 写入 {MASTER_RULE}（最终参与处理 {len(merged)} 条）")

    return True

# ===============================
# 分片
# ===============================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False

    rules = [l.strip() for l in open(MASTER_RULE, "r", encoding="utf-8").read().splitlines() if l.strip()]
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
# DNS 查询
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

    total = len(lines)
    batch_size = 500
    checked = 0
    start = time.time()

    for i in range(0, total, batch_size):
        batch = lines[i:i+batch_size]

        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
            futures = {ex.submit(check_domain, r): r for r in batch}
            for f in as_completed(futures):
                checked += 1
                r = f.result()
                if r:
                    valid.append(r)

        elapsed = time.time() - start
        speed = checked / elapsed if elapsed > 0 else 0
        remain = total - checked
        eta = remain / speed if speed > 0 else 0

        print(
            f"✅ 已验证 {checked}/{total} 条"
            f" | 有效 {len(valid)} 条"
            f" | 速度 {speed:.1f} 条/秒"
            f" | ETA {eta:.1f} 秒"
        )

    elapsed = time.time() - start
    print(f"🎯 DNS验证完成 → 有效 {len(valid)} 条，总耗时 {elapsed:.1f} 秒")
    return valid

# ===============================
# 分片处理（原逻辑全部保留）
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新获取")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在")
        return

    lines = [l for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则")

    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        old_rules = set(l.strip() for l in open(out_file, "r", encoding="utf-8") if l.strip())

    delete_counter = safe_load_json(DELETE_COUNTER_FILE)
    skip_tracker = safe_load_json(SKIP_FILE)

    rules_to_validate = []
    for r in lines:
        c = delete_counter.get(r, 0)
        if c <= SKIP_VALIDATE_THRESHOLD:
            rules_to_validate.append(r)
            continue

        skip_tracker[r] = skip_tracker.get(r, 0) + 1
        print(f"⏩ 跳过验证 {r}（次数 {skip_tracker[r]}/10）")

        if skip_tracker[r] >= SKIP_ROUNDS:
            print(f"🔁 恢复验证：{r}（跳过达到10次 → 重置计数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r)
            rules_to_validate.append(r)

    valid = set(dns_validate(rules_to_validate))

    final_rules = set()
    added = 0
    removed = 0
    merged_all = old_rules | set(lines)
    new_counter = delete_counter.copy()

    for r in merged_all:
        if r in valid:
            final_rules.add(r)
            new_counter[r] = 0
            if r not in old_rules:
                added += 1
            continue

        old = delete_counter.get(r, 0)
        new = (old + 1) if old else 4
        new_counter[r] = new
        print(f"⚠ 连续失败计数 = {new} ：{r}")

        if new >= DELETE_THRESHOLD:
            removed += 1
            continue

        final_rules.add(r)

    safe_update_json(DELETE_COUNTER_FILE, new_counter)
    safe_update_json(SKIP_FILE, skip_tracker)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total}, 新增 {added}, 删除 {removed}")
    print(f"COMMIT_STATS: 总 {total}, 新增 {added}, 删除 {removed}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true")
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
