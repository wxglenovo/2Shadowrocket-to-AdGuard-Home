#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import argparse
import requests
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
PARTS = 16
DNS_BATCH_SIZE = 50

FAIL_DB_FILE = "fails.json"

def load_fail_db():
    if not os.path.exists(FAIL_DB_FILE):
        return {}
    try:
        with open(FAIL_DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_fail_db(db):
    with open(FAIL_DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2)

fail_db = load_fail_db()

def download_sources():
    print("📥 开始下载所有规则源...")
    if not os.path.exists(URLS_TXT):
        print(f"❌ 找不到 {URLS_TXT}, 请确认文件存在")
        return

    os.makedirs(TMP_DIR, exist_ok=True)
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip()]

    rules = []
    for url in urls:
        try:
            print(f"Downloading {url}")
            text = requests.get(url, timeout=20).text
            for line in text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    rules.append(line)
        except:
            print(f"❌ 下载失败：{url}")

    print(f"✅ 下载完成，共 {len(rules)} 条规则")
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(rules))
    print("✅ 已生成 merged_rules.txt")

def split_parts():
    os.makedirs(TMP_DIR, exist_ok=True)
    if not os.path.exists(MERGED_FILE):
        print("❌ 没有 merged_rules.txt，无法切片")
        return

    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip()]

    total = len(lines)
    size = total // PARTS + 1

    print(f"✂ 切片规则，共 {total} 条，每片约 {size} 条")

    for i in range(PARTS):
        p = lines[i * size:(i + 1) * size]
        part_file = f"{TMP_DIR}/part_{i+1:02d}.txt"
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(p))
        print(f"✅ {part_file} 共 {len(p)} 条")

def dns_lookup(rule):
    try:
        dns.resolver.resolve(rule, "A", lifetime=2)
        return True
    except:
        return False

def handle_dns_result(rule, success):
    rule = rule.strip()

    if success:
        if rule in fail_db:
            del fail_db[rule]
            save_fail_db(fail_db)
        return "ok"

    # 连续失败计数 +1
    fail_db[rule] = fail_db.get(rule, 0) + 1
    save_fail_db(fail_db)

    if fail_db[rule] < 4:
        print(f"⚠ {rule} 第 {fail_db[rule]} 次失败（未删除）")
        return "keep"

    # 连续失败 ≥4 → 删除
    print(f"❌ {rule} 连续失败 {fail_db[rule]} 次 → 已删除")
    del fail_db[rule]
    save_fail_db(fail_db)
    return "delete"

def validate_part(part_id):
    os.makedirs(DIST_DIR, exist_ok=True)

    part_file = f"{TMP_DIR}/part_{part_id:02d}.txt"
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在：{part_file}")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [x.strip() for x in f if x.strip()]

    print(f"⏱ 开始验证分片 {part_id}，共 {len(rules)} 条规则")
    print(f"🚀 启动 {DNS_BATCH_SIZE} 并发验证")

    valid_rules = []
    with ThreadPoolExecutor(max_workers=DNS_BATCH_SIZE) as executor:
        future_map = {executor.submit(dns_lookup, rule): rule for rule in rules}

        for future in as_completed(future_map):
            rule = future_map[future]
            try:
                success = future.result()
                res = handle_dns_result(rule, success)
                if res in ("ok", "keep"):
                    valid_rules.append(rule)
            except:
                pass

    outfile = f"{DIST_DIR}/validated_part_{part_id:02d}.txt"
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(valid_rules))

    print(f"✅ 分片验证完成，有效 {len(valid_rules)} 条 → {outfile}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    parser.add_argument("--part", type=int, help="指定分片验证")
    args = parser.parse_args()

    # ✅ 首次运行或 --force-update → 下载 & 切片
    need_setup = args.force_update or not os.path.exists(MERGED_FILE)

    if need_setup:
        download_sources()
        split_parts()

    # ✅ 指定分片
    if args.part:
        pf = f"{TMP_DIR}/part_{args.part:02d}.txt"
        if not os.path.exists(pf):
            print(f"⚠ 缺少分片 {pf}，自动重新下载并切片")
            download_sources()
            split_parts()

        validate_part(args.part)
