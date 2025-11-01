#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置
# ===============================
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_BATCH_SIZE = int(os.environ.get("DNS_BATCH_SIZE", 500))
DELETE_THRESHOLD = 4
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

# 创建目录
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 下载并合并规则
# ===============================
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return [i.strip() for i in f if i.strip()]

def download_and_merge(urls):
    all_rules = []
    for u in urls:
        try:
            r = requests.get(u, timeout=15)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("!"):
                        all_rules.append(line)
        except Exception:
            print(f"⚠ 下载失败: {u}")
    all_rules = sorted(set(all_rules))
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并完成，共 {len(all_rules)} 条规则")
    return all_rules

# ===============================
# 分片
# ===============================
def split_rules():
    if not os.path.exists(MERGED_FILE):
        print("❌ 合并文件不存在，无法分片")
        return
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [i.strip() for i in f if i.strip()]
    total = len(rules)
    size = total // PARTS + 1
    for i in range(PARTS):
        part_rules = rules[i * size:(i + 1) * size]
        tmp_path = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        dist_path = os.path.join(DIST_DIR, f"validated_part_{i+1:02d}.txt")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        with open(dist_path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 生成分片 {i+1:02d}: {len(part_rules)} 条")

# ===============================
# 删除计数文件
# ===============================
def load_delete_counter():
    if not os.path.exists(DELETE_COUNTER_FILE):
        return {}
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2)

# ===============================
# DNS 验证
# ===============================
def dns_check(domain):
    try:
        dns.resolver.resolve(domain, 'A', lifetime=3)
        return True
    except:
        return False

# ===============================
# 验证分片
# ===============================
def validate_part(index, concurrent=False):
    tmp_file = os.path.join(TMP_DIR, f"part_{index:02d}.txt")
    dist_file = os.path.join(DIST_DIR, f"validated_part_{index:02d}.txt")
    if not os.path.exists(tmp_file):
        print(f"❌ 分片不存在：{tmp_file}")
        return 0,0,0

    with open(tmp_file, "r", encoding="utf-8") as f:
        rules = [i.strip() for i in f if i.strip()]

    delete_counter = load_delete_counter()
    kept, deleted = [], []
    delayed_warn = []

    def check_rule(rule):
        domain = rule.replace("||", "").replace("^", "")
        ok = dns_check(domain)
        return rule, ok

    if concurrent:
        print(f"🚀 并发验证分片 {index}，{DNS_WORKERS} 线程，每批 {DNS_BATCH_SIZE} 条")
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = [executor.submit(check_rule, r) for r in rules]
            for i, future in enumerate(as_completed(futures), 1):
                rule, ok = future.result()
                if ok:
                    kept.append(rule)
                    delete_counter.pop(rule, None)
                else:
                    count = delete_counter.get(rule, 0) + 1
                    delete_counter[rule] = count
                    if count < DELETE_THRESHOLD:
                        kept.append(rule)
                        delayed_warn.append(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
                    else:
                        deleted.append(rule)
                if i % DNS_BATCH_SIZE == 0 or i == len(rules):
                    print(f"✅ 已验证 {i}/{len(rules)} 条，有效 {len(kept)} 条")
    else:
        for rule in rules:
            rule, ok = check_rule(rule)
            if ok:
                kept.append(rule)
                delete_counter.pop(rule, None)
            else:
                count = delete_counter.get(rule, 0) + 1
                delete_counter[rule] = count
                if count < DELETE_THRESHOLD:
                    kept.append(rule)
                    delayed_warn.append(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
                else:
                    deleted.append(rule)
        print(f"✅ 已验证 {len(rules)}/{len(rules)} 条，有效 {len(kept)} 条")

    # 输出所有连续删除警告，保证在验证完成后
    for w in delayed_warn:
        print(w)

    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("\n".join(kept))
    with open(dist_file, "w", encoding="utf-8") as f:
        f.write("\n".join(kept))

    save_delete_counter(delete_counter)
    print(f"COMMIT_STATS: 总 {len(rules)}, 有效 {len(kept)}, 删除 {len(deleted)}")
    return len(rules), len(kept), len(deleted)

# ===============================
# 主流程
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片")
    parser.add_argument("--concurrent", action="store_true", help="开启并发验证")
    parser.add_argument("--force", action="store_true", help="强制重新下载与分片")
    args = parser.parse_args()

    if args.force or not os.path.exists(MERGED_FILE):
        urls = load_urls()
        download_and_merge(urls)
        split_rules()

    # 如果分片缺失，也重新分片
    for i in range(1, PARTS+1):
        tmp_file = os.path.join(TMP_DIR, f"part_{i:02d}.txt")
        dist_file = os.path.join(DIST_DIR, f"validated_part_{i:02d}.txt")
        if not os.path.exists(tmp_file) or not os.path.exists(dist_file):
            split_rules()
            break

    parts_to_check = [args.part] if args.part else list(range(1, PARTS+1))

    total, kept_total, deleted_total = 0,0,0
    for idx in parts_to_check:
        t,k,d = validate_part(idx, concurrent=args.concurrent)
        total += t
        kept_total += k
        deleted_total += d

    print(f"🤖 Auto update: validated part {parts_to_check[-1]} → 总 {total}, 新增 {kept_total}, 删除 {deleted_total}")
