#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import dns.resolver
import json

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_RULE = "merged_rules.txt"
PARTS = 16
DNS_BATCH_SIZE = 50
DELETE_CONFIRM_TIMES = 4
DELETE_COUNTER_FILE = "delete_counter.json"

def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)

def download_rules():
    print("📥 开始下载规则源...")
    if not os.path.exists(URLS_TXT):
        print(f"❌ 找不到 {URLS_TXT}")
        return []

    rules = []
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        for url in f:
            url = url.strip()
            if not url:
                continue
            print(f"Downloading {url}")
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    for line in r.text.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            rules.append(line)
            except Exception as e:
                print(f"❌ 下载失败：{url} - {e}")
    print(f"✅ 下载完成，总规则数: {len(rules)}")
    return rules

def merge_rules():
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(DIST_DIR, exist_ok=True)

    rules = download_rules()
    rules = sorted(set(rules))

    with open(MERGED_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(rules))

    print(f"✅ 合并完成: {len(rules)} 条规则")

    size = len(rules)
    part_len = size // PARTS + 1

    for i in range(PARTS):
        part = rules[i * part_len:(i + 1) * part_len]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part))
        print(f"📦 分片 {i+1:02d} → {len(part)} 条")

def dns_check(domain):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2
    try:
        resolver.resolve(domain)
        return True
    except:
        return False

def validate_rules(rules):
    valid = []
    for rule in rules:
        d = rule.replace("||", "").replace("^", "")
        if "." not in d:
            continue
        if dns_check(d):
            valid.append(rule)
    return valid

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在: {part_file}")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [i.strip() for i in f if i.strip()]

    total_before = len(rules)
    print(f"⏳ 分片 {part:02d} 共 {total_before} 条，开始 DNS 验证...")

    # 批量验证
    valid = []
    batch = []
    for r in rules:
        batch.append(r)
        if len(batch) >= DNS_BATCH_SIZE:
            valid.extend(validate_rules(batch))
            batch = []
    if batch:
        valid.extend(validate_rules(batch))

    valid = sorted(set(valid))
    total_after = len(valid)
    removed_count = total_before - total_after

    # ===== 连续删除计数 =====
    counter = load_delete_counter()
    part_key = f"part_{part:02d}"
    delete_ratio = removed_count / total_before if total_before else 0

    if delete_ratio > 0.10:  # 删除比例超过 10% 才计次数
        counter[part_key] = counter.get(part_key, 0) + 1
    else:
        counter[part_key] = 0  # 重置
    save_delete_counter(counter)

    # 第四次连续删除才生效
    if counter[part_key] < DELETE_CONFIRM_TIMES:
        print(f"⚠ 分片 {part:02d} 删除 {removed_count} 条，但未达到 {DELETE_CONFIRM_TIMES} 次确认，不写入。")
        valid = rules  # 保留原内容
        removed_count = 0

    # 写入结果文件
    out_file = os.path.join(DIST_DIR, f"validated_part_{part:02d}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    # 统计新增
    # 新增 = dist 最新 - merged_rules
    merged = []
    if os.path.exists(MERGED_RULE):
        with open(MERGED_RULE, "r", encoding="utf-8") as f:
            merged = set(i.strip() for i in f if i.strip())

    old_rules = []
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set(i.strip() for i in f if i.strip())

    added_count = len(old_rules - merged)

    print(f"✅ 分片 {part:02d} 完成 → 总 {len(valid)}, 新增 {added_count}, 删除 {removed_count}")

    # ✅ 写入 GITHUB_ENV 让 workflow 获取统计
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a") as env:
            env.write(f"PART_STATS=总数 {len(valid)}, 新增 {added_count}, 删除 {removed_count}\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证分片 1 ~ 16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载并分片")
    args = parser.parse_args()

    if args.force-update:
        merge_rules()
        print("✅ 已强制更新规则源 & 分片")
        return

    if args.part:
        process_part(args.part)
    else:
        print("❌ 必须指定 --part 或 --force-update")

if __name__ == "__main__":
    main()
