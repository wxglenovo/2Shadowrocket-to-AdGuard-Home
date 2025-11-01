#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
PARTS = 16
DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


def load_urls():
    if not os.path.exists(URLS_FILE):
        print("❌ urls.txt 不存在")
        exit(1)
    return [l.strip() for l in open(URLS_FILE, "r", encoding="utf-8") if l.strip()]


def download_and_merge():
    urls = load_urls()
    merged = []
    print("📥 开始下载规则源…")
    for u in urls:
        try:
            r = requests.get(u, timeout=20)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("!"):
                        merged.append(line)
                print(f"✅ 读取: {u}")
            else:
                print(f"⚠ 无法访问 {u}")
        except:
            print(f"⚠ 请求失败: {u}")

    merged = sorted(set(merged))
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(merged))

    print(f"✅ 合并完成: {len(merged)} 条规则")
    return merged


def split_to_tmp():
    if not os.path.exists(MERGED_FILE):
        download_and_merge()

    rules = [l.strip() for l in open(MERGED_FILE, "r", encoding="utf-8") if l.strip()]
    total = len(rules)
    per = (total + PARTS - 1) // PARTS

    print(f"🪓 分片 {total} 条，每片约 {per}")

    for i in range(PARTS):
        part_rules = rules[i * per:(i + 1) * per]
        path = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 生成 {path} : {len(part_rules)} 条")

    return True


def ensure_tmp_exists():
    for i in range(1, PARTS + 1):
        f = os.path.join(TMP_DIR, f"part_{i:02d}.txt")
        if not os.path.exists(f):
            print(f"⚠ {f} 不存在 → 重新生成全部分片")
            split_to_tmp()
            break


def dns_check(rule):
    domain = rule.replace("||", "").replace("^", "").replace("*", "")
    if not domain:
        return False
    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = DNS_TIMEOUT
        resolver.lifetime = DNS_TIMEOUT
        resolver.resolve(domain)
        return True
    except:
        return False


def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            return json.load(open(DELETE_COUNTER_FILE, "r", encoding="utf-8"))
        except:
            print("⚠ delete_counter.json 损坏，重置")
    return {}


def save_delete_counter(d):
    json.dump(d, open(DELETE_COUNTER_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)


def validate_part(idx):
    part_path = os.path.join(TMP_DIR, f"part_{idx:02d}.txt")
    if not os.path.exists(part_path):
        print(f"⚠ 分片缺失 → 自动重建")
        split_to_tmp()

    rules = [l.strip() for l in open(part_path, "r", encoding="utf-8") if l.strip()]

    out_path = os.path.join(DIST_DIR, f"validated_part_{idx:02d}.txt")
    old = set()
    if os.path.exists(out_path):
        old = {l.strip() for l in open(out_path, "r", encoding="utf-8") if l.strip()}

    delete_counter = load_delete_counter()
    kept = set()
    removed = 0
    added = 0

    print(f"🚀 并发验证分片 {idx}, 共 {len(rules)} 条")

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as e:
        futures = {e.submit(dns_check, r): r for r in rules}
        done = 0
        for fut in as_completed(futures):
            done += 1
            rule = futures[fut]
            ok = fut.result()

            if ok:
                kept.add(rule)
                delete_counter[rule] = 0
            else:
                cnt = delete_counter.get(rule, 0) + 1
                delete_counter[rule] = cnt
                if cnt < DELETE_THRESHOLD:
                    kept.add(rule)
                else:
                    removed += 1

            if done % 500 == 0:
                print(f"✅ {done}/{len(rules)} 已验证，有效 {len(kept)}")

    for r in kept:
        if r not in old:
            added += 1

    save_delete_counter(delete_counter)

    open(out_path, "w", encoding="utf-8").write("\n".join(sorted(kept)))

    print(f"✅ 分片 {idx} 完成: 总 {len(kept)}, 新增 {added}, 删除 {removed}")
    print(f"COMMIT_STATS: 总 {len(kept)}, 新增 {added}, 删除 {removed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int)
    args = parser.parse_args()

    if not os.path.exists(MERGED_FILE):
        download_and_merge()

    ensure_tmp_exists()

    if args.part:
        validate_part(args.part)
