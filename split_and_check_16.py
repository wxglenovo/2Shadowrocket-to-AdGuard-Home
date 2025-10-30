#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
BATCH_SIZE = 500  # 每批处理数量

DELETE_THRESHOLD = 4  # 连续几次验证被判删除才真正删除

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DNS_CACHE_FILE = os.path.join(DIST_DIR, "dns_cache.json")


def load_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 开始下载规则源...")
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True


def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
    return True


def check_domain(rule, resolver, dns_cache):
    if rule in dns_cache:
        return True
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return False
    try:
        resolver.resolve(domain)
        dns_cache[rule] = True
        return True
    except:
        return False


def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 线程 DNS 验证，共 {len(lines)} 条规则")
    valid = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    dns_cache = load_json(DNS_CACHE_FILE)

    for i in range(0, len(lines), BATCH_SIZE):
        batch = lines[i:i+BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule, resolver, dns_cache): rule for rule in batch}
            for future in as_completed(futures):
                rule = futures[future]
                try:
                    if future.result():
                        valid.append(rule)
                except Exception:
                    continue
        print(f"✅ 已验证 {min(i+BATCH_SIZE, len(lines))}/{len(lines)} 条，有效 {len(valid)} 条")

    save_json(DNS_CACHE_FILE, dns_cache)
    return valid


def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return
    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")

    valid = dns_validate(lines)
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    # 连续删除计数
    delete_counter = load_json(DELETE_COUNTER_FILE)
    new_valid = []

    # 若已有 validated_part 文件，做连续删除判断
    existing_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            existing_rules = set(f.read().splitlines())

    for rule in lines:
        if rule in valid:
            # 验证成功，计数清零
            delete_counter.pop(rule, None)
            new_valid.append(rule)
        else:
            # DNS 失败，计数+1
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            if delete_counter[rule] < DELETE_THRESHOLD:
                # 未到连续删除阈值，仍保留
                new_valid.append(rule)
            # >= DELETE_THRESHOLD 则删除，不加入 new_valid

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(new_valid)))
    save_json(DELETE_COUNTER_FILE, delete_counter)
    print(f"✅ 分片 {part} 验证完成，有效 {len(new_valid)} 条 → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    args = parser.parse_args()

    # 强制刷新
    if args.force_update:
        download_all_sources()
        split_parts()

    # 缺失文件自动补
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
