#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# 配置
# -----------------------------
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
PARTS = 16
DELETE_THRESHOLD = 4

DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_BATCH_SIZE = int(os.environ.get("DNS_BATCH_SIZE", 500))

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# -----------------------------
# 下载 & 合并规则
# -----------------------------
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return [u.strip() for u in f if u.strip()]

def download_and_merge(urls):
    all_rules = set()
    for u in urls:
        try:
            r = requests.get(u, timeout=20)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("!"):
                        all_rules.add(line)
        except Exception:
            print(f"⚠ 下载失败: {u}")
    all_rules = sorted(all_rules)
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并完成，共 {len(all_rules)} 条规则")
    return all_rules

# -----------------------------
# 分片 tmp/part_01~16.txt
# -----------------------------
def split_rules():
    if not os.path.exists(MERGED_FILE):
        print("⚠ 缺少 merged_rules.txt")
        return
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    size = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i*size:(i+1)*size]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 生成分片 {i+1:02d}: {len(part_rules)} 条")

# -----------------------------
# 删除计数
# -----------------------------
def load_delete_counter():
    if not os.path.exists(DELETE_COUNTER_FILE):
        return {}
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# -----------------------------
# DNS 验证
# -----------------------------
def dns_check(domain):
    try:
        if not domain:
            return False
        dns.resolver.resolve(domain, 'A', lifetime=2)
        return True
    except:
        return False

def validate_part(index, concurrent=False):
    part_file = os.path.join(TMP_DIR, f"part_{index:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在: {part_file}")
        return 0,0,0
    with open(part_file, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    delete_counter = load_delete_counter()
    kept, deleted = [], []

    def check_rule(rule):
        domain = rule.replace("||","").replace("^","").replace("*","")
        ok = dns_check(domain)
        return rule, ok

    if concurrent:
        print(f"🚀 并发验证分片 {index}, {DNS_WORKERS} 线程, 每批 {DNS_BATCH_SIZE} 条")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = [executor.submit(check_rule, r) for r in rules]
            for i, future in enumerate(as_completed(futures),1):
                rule, ok = future.result()
                if ok:
                    kept.append(rule)
                    delete_counter.pop(rule,None)
                else:
                    count = delete_counter.get(rule,0)+1
                    delete_counter[rule] = count
                    print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
                    if count < DELETE_THRESHOLD:
                        kept.append(rule)
                    else:
                        deleted.append(rule)
                if i % DNS_BATCH_SIZE == 0 or i==len(rules):
                    print(f"✅ 已验证 {i}/{len(rules)} 条, 有效 {len(kept)} 条")
    else:
        for rule in rules:
            domain = rule.replace("||","").replace("^","").replace("*","")
            ok = dns_check(domain)
            if ok:
                kept.append(rule)
                delete_counter.pop(rule,None)
            else:
                count = delete_counter.get(rule,0)+1
                delete_counter[rule] = count
                print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
                if count < DELETE_THRESHOLD:
                    kept.append(rule)
                else:
                    deleted.append(rule)

    # 写回 tmp/分片
    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(kept)))

    save_delete_counter(delete_counter)
    print(f"COMMIT_STATS: 总 {len(rules)}, 新增 {len(kept)-len(rules)}, 删除 {len(deleted)}")
    return len(rules), len(kept), len(deleted)

# -----------------------------
# 主函数
# -----------------------------
if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片 1~16")
    parser.add_argument("--concurrent", action="store_true", help="开启并发验证")
    parser.add_argument("--force", action="store_true", help="强制下载 & 分片")
    args = parser.parse_args()

    if args.force or not os.path.exists(MERGED_FILE):
        urls = load_urls()
        download_and_merge(urls)
        split_rules()

    # 确保 tmp/分片存在
    for i in range(1, PARTS+1):
        part_file = os.path.join(TMP_DIR, f"part_{i:02d}.txt")
        if not os.path.exists(part_file):
            split_rules()
            break

    parts_to_check = [args.part] if args.part else list(range(1, PARTS+1))
    total_sum, kept_sum, deleted_sum = 0,0,0
    for idx in parts_to_check:
        t,k,d = validate_part(idx, concurrent=args.concurrent)
        total_sum += t
        kept_sum += k
        deleted_sum += d
