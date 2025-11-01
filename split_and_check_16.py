#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
import argparse
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
DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_BATCH_SIZE = int(os.environ.get("DNS_BATCH_SIZE", 500))
DELETE_THRESHOLD = 4

# 创建目录
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# -----------------------------
# 加载源 URL
# -----------------------------
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# -----------------------------
# 下载并合并规则
# -----------------------------
def download_and_merge(urls):
    merged = set()
    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("!"):
                        merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败: {url} → {e}")
    merged = sorted(merged)
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(merged))
    print(f"✅ 合并完成，共 {len(merged)} 条规则")
    return merged

# -----------------------------
# 分片
# -----------------------------
def split_rules():
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]
    total = len(rules)
    size = total // PARTS + 1
    for i in range(PARTS):
        part_rules = rules[i*size:(i+1)*size]
        path = os.path.join(DIST_DIR, f"validated_part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 生成分片 {i+1:02d}: {len(part_rules)} 条")

# -----------------------------
# 删除计数
# -----------------------------
def load_delete_counter():
    if not os.path.exists(DELETE_COUNTER_FILE):
        return {}
    try:
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        print(f"⚠ {DELETE_COUNTER_FILE} 解析失败，重建空计数")
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# -----------------------------
# DNS 检查
# -----------------------------
def dns_check(domain):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 3
    resolver.lifetime = 3
    domain = domain.replace("||","").replace("^","").replace("*","")
    if not domain:
        return False
    try:
        resolver.resolve(domain, 'A')
        return True
    except:
        return False

# -----------------------------
# 分片验证
# -----------------------------
def validate_part(index, concurrent=False):
    part_file = os.path.join(DIST_DIR, f"validated_part_{index:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在：{part_file}")
        return 0,0,0

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]

    delete_counter = load_delete_counter()
    kept, deleted = [], []
    batch_warnings = []
    all_warnings = []

    def check_rule(rule):
        return rule, dns_check(rule)

    if concurrent:
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = [executor.submit(check_rule, r) for r in rules]
            for i, future in enumerate(as_completed(futures),1):
                rule, ok = future.result()
                if ok:
                    kept.append(rule)
                    delete_counter.pop(rule, None)
                else:
                    delete_counter[rule] = delete_counter.get(rule,0)+1
                    if delete_counter[rule] < DELETE_THRESHOLD:
                        kept.append(rule)
                        batch_warnings.append(f"⚠ 连续删除计数 {delete_counter[rule]}/{DELETE_THRESHOLD}: {rule}")
                    else:
                        deleted.append(rule)

                if i % DNS_BATCH_SIZE == 0 or i == len(rules):
                    print(f"✅ 已验证 {i}/{len(rules)} 条，有效 {len(kept)} 条")
                    all_warnings.extend(batch_warnings)
                    batch_warnings = []
    else:
        for i, rule in enumerate(rules,1):
            _, ok = check_rule(rule)
            if ok:
                kept.append(rule)
                delete_counter.pop(rule, None)
            else:
                delete_counter[rule] = delete_counter.get(rule,0)+1
                if delete_counter[rule] < DELETE_THRESHOLD:
                    kept.append(rule)
                    batch_warnings.append(f"⚠ 连续删除计数 {delete_counter[rule]}/{DELETE_THRESHOLD}: {rule}")
                else:
                    deleted.append(rule)

            if i % DNS_BATCH_SIZE == 0 or i == len(rules):
                print(f"✅ 已验证 {i}/{len(rules)} 条，有效 {len(kept)} 条")
                all_warnings.extend(batch_warnings)
                batch_warnings = []

    # ⚠ 连续删除计数统一输出
    for w in all_warnings:
        print(w)

    save_delete_counter(delete_counter)

    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(kept))

    print(f"COMMIT_STATS: 总 {len(rules)}, 有效 {len(kept)}, 删除 {len(deleted)}")
    return len(rules), len(kept), len(deleted)

# -----------------------------
# 主流程
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片 1~16")
    parser.add_argument("--concurrent", action="store_true", help="开启并发验证")
    parser.add_argument("--force", action="store_true", help="强制重新下载规则")
    args = parser.parse_args()

    # 下载合并规则
    if args.force or not os.path.exists(MERGED_FILE):
        urls = load_urls()
        download_and_merge(urls)
        split_rules()

    # 生成缺失分片
    for i in range(1, PARTS+1):
        part_file = os.path.join(DIST_DIR, f"validated_part_{i:02d}.txt")
        if not os.path.exists(part_file):
            split_rules()
            break

    # 验证分片
    parts_to_check = [args.part] if args.part else list(range(1, PARTS+1))
    total_all, kept_all, deleted_all = 0,0,0
    for idx in parts_to_check:
        t,k,d = validate_part(idx, concurrent=args.concurrent)
        total_all += t
        kept_all += k
        deleted_all += d

    print(f"🤖 Auto update: validated part {parts_to_check[-1]} → 总 {total_all}, 新增 {kept_all}, 删除 {deleted_all}")
