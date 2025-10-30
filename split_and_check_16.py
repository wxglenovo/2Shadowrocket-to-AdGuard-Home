#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import dns.resolver

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 开始下载规则源...")
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
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    return True


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
        filename = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i + 1}: {len(part_rules)} 条 → {filename}")
    return True


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
    print(f"🚀 启动 {DNS_WORKERS} 并发验证")
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        done = 0
        total = len(lines)
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid


def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)


def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动重新下载并切片")
        download_all_sources()
        split_parts()
        if not os.path.exists(part_file):
            print("❌ 分片仍不存在，终止")
            return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")
    valid = dns_validate(lines)

    # 更新 validated_part 文件
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    prev_rules = []
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            prev_rules = [l.strip() for l in f if l.strip()]

    delete_counter = load_delete_counter()
    new_rules = set(valid)
    final_rules = []

    for rule in prev_rules:
        if rule in new_rules:
            # 验证成功，计数清零
            if rule in delete_counter:
                delete_counter[rule] = 0
            final_rules.append(rule)
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            print(f"⚠ {rule} 连续删除计数: {delete_counter[rule]}")
            if delete_counter[rule] >= DELETE_THRESHOLD:
                print(f"❌ {rule} 达到连续 {DELETE_THRESHOLD} 次删除，移除")
            else:
                final_rules.append(rule)

    # 新增规则直接加入
    for rule in new_rules:
        if rule not in prev_rules:
            final_rules.append(rule)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))
    save_delete_counter(delete_counter)

    print(f"✅ 分片 {part} 完成 → {out_file}，共 {len(final_rules)} 条规则")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
