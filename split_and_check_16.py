#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
import argparse
from datetime import datetime

URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
VALID_CACHE_FILE = os.path.join(DIST_DIR, "valid_cache.json")
SUMMARY_FILE = os.path.join(DIST_DIR, "summary.txt")

PARTS = 16
DNS_WORKERS = 80
DNS_BATCH_SIZE = 500

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


# ✅ DNS 备用服务器
DNS_SERVERS = [
    "8.8.8.8",        # Google
    "1.1.1.1",        # Cloudflare
    "9.9.9.9",        # Quad9
    "114.114.114.114" # 中国运营商
]


def dns_check_multi(domain):
    """多 DNS 尝试，只要任意一个返回成功，就算有效"""
    for dns_ip in DNS_SERVERS:
        try:
            resolver = dns.resolver.Resolver()
            resolver.lifetime = 2
            resolver.timeout = 2
            resolver.nameservers = [dns_ip]
            resolver.resolve(domain, 'A')
            return True
        except:
            continue
    return False


def load_cache():
    if os.path.exists(VALID_CACHE_FILE):
        return json.load(open(VALID_CACHE_FILE, "r"))
    return {}


def save_cache(cache):
    json.dump(cache, open(VALID_CACHE_FILE, "w"), indent=2)


def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        return json.load(open(DELETE_COUNTER_FILE, "r"))
    return {}


def save_delete_counter(counter):
    json.dump(counter, open(DELETE_COUNTER_FILE, "w"), indent=2)


def validate_part(index):
    part_file = os.path.join(DIST_DIR, f"validated_part_{index:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在: {part_file}")
        return (0, 0)

    rules = [i.strip() for i in open(part_file, "r", encoding="utf-8") if i.strip()]
    cache = load_cache()
    counter = load_delete_counter()

    kept = []
    deleted = 0

    for rule in rules:
        domain = rule.replace("||", "").replace("^", "")

        # ✅ 如果之前解析成功过，跳过 DNS 检查
        if domain in cache:
            kept.append(rule)
            continue

        ok = dns_check_multi(domain)

        if ok:
            kept.append(rule)
            cache[domain] = True    # ✅ 加入缓存
            if rule in counter:
                del counter[rule]
        else:
            counter[rule] = counter.get(rule, 0) + 1
            if counter[rule] < 4:
                kept.append(rule)
                print(f"⚠ {rule} 连续删除计数 {counter[rule]}/4")
            else:
                deleted += 1
                print(f"❌ {rule} 连续4次失败 → 移除")

    open(part_file, "w", encoding="utf-8").write("\n".join(kept))
    save_cache(cache)
    save_delete_counter(counter)

    return (len(kept), deleted)


def write_summary(data):
    with open(SUMMARY_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n=== {datetime.utcnow()} UTC ===\n")
        for p, (k, d) in data.items():
            f.write(f"分片 {p:02d}: 保留 {k}, 删除 {d}\n")
        f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, default=0)
    args = parser.parse_args()

    # ✅ 验证单个分片（用于并发）
    if args.part > 0:
        kept, deleted = validate_part(args.part)
        print(f"✅ 分片 {args.part:02d} 处理完成 → 保留 {kept}, 删除 {deleted}")
        exit(0)

    # ✅ 否则串行处理全部
    results = {}
    for i in range(1, PARTS + 1):
        k, d = validate_part(i)
        results[i] = (k, d)

    write_summary(results)
    print("✅ 所有分片处理完毕，并已写入 summary.txt")
