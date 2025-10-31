#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
split_and_check_16.py
- 下载并合并规则源 (urls.txt)
- 切分为 PARTS 个分片写到 tmp/part_XX.txt
- 提供 --part <n> 验证单个分片并写入 dist/validated_part_XX.txt
- 提供 --force-update 强制重新下载并切片
"""

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 配置 ==========
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16

DNS_WORKERS = 50
BATCH_SIZE = 500
DNS_TIMEOUT = 2

DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

# ensure dirs
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ========== 下载与合并规则 ==========
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 下载规则源...")
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ========== 分片 ==========
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
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ========== DNS 验证 ==========
def _extract_domain_from_rule(rule: str) -> str:
    # 常见规则有 ||domain^ 、|http://... 、 plain domain 等，尽量提取可解析部分
    r = rule.strip()
    # remove anchors and options
    if r.startswith("||"):
        r = r[2:]
    elif r.startswith("|"):
        r = r[1:]
    # cut at ^ or / or $
    for sep in ["^", "/", "$", ":" , " "]:
        if sep in r:
            r = r.split(sep)[0]
    # remove leading wildcards
    r = r.lstrip("*")
    return r

def check_domain(rule):
    domain = _extract_domain_from_rule(rule)
    if not domain:
        return None
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines):
    total = len(lines)
    valid = []
    print(f"🚀 启动 DNS 验证：并发 {DNS_WORKERS}，每批 {BATCH_SIZE} 条，总 {total} 条")
    for start in range(0, total, BATCH_SIZE):
        batch = lines[start:start + BATCH_SIZE]
        batch_valid = []
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule): rule for rule in batch}
            done = 0
            for fut in as_completed(futures):
                done += 1
                res = fut.result()
                if res:
                    batch_valid.append(res)
                if done % 50 == 0 or done == len(batch):
                    print(f"  ✅ 当前批 {start//BATCH_SIZE + 1}: 已验证 {done}/{len(batch)} 条，本批有效 {len(batch_valid)}, 累计有效 {len(valid)+len(batch_valid)}")
        valid.extend(batch_valid)
    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ========== delete_counter.json 读写（合并写，封顶） ==========
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_delete_counter_merge(new_counter: dict):
    """
    将 new_counter 写回 DELETE_COUNTER_FILE，但要与磁盘上已有的旧计数合并：
    - 对每个规则取 max(old, new)
    - 对每个规则封顶为 DELETE_THRESHOLD
    """
    old = load_delete_counter()
    merged = old.copy()
    for k, v in new_counter.items():
        # old may have higher; take max
        merged[k] = max(merged.get(k, 0), v)
        # cap
        merged[k] = min(merged[k], DELETE_THRESHOLD)
    # ensure all values capped
    for k in list(merged.keys()):
        merged[k] = min(merged[k], DELETE_THRESHOLD)
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

# ========== 分片处理 ==========
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，尝试重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines() if l.strip()]
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")

    valid_set = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{int(part):02d}.txt")

    # read old validated rules (if any)
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    # load global delete counter
    delete_counter = load_delete_counter()
    # start new counter as copy of old so other rules not lost
    new_counter = delete_counter.copy()

    final_rules = set()
    removed_count = 0
    added_count = 0

    all_rules = old_rules | set(lines)

    for rule in all_rules:
        if rule in valid_set:
            final_rules.add(rule)
            # reset count
            if delete_counter.get(rule, 0) > 0:
                print(f"🔄 验证成功，清零删除计数: {rule}")
            new_counter[rule] = 0
        else:
            old_cnt = delete_counter.get(rule, 0)
            new_cnt = min(old_cnt + 1, DELETE_THRESHOLD)
            new_counter[rule] = new_cnt
            print(f"⚠ 连续删除计数 {new_cnt}/{DELETE_THRESHOLD}: {rule}")

            # if reach threshold -> truly remove (do not add to final_rules)
            if new_cnt >= DELETE_THRESHOLD:
                # count removal only when it crosses threshold now (old_cnt < threshold)
                if old_cnt < DELETE_THRESHOLD:
                    removed_count += 1
                # do NOT add to final_rules
            else:
                final_rules.add(rule)

        # added count (newly valid and not present in old_rules)
        if rule not in old_rules and rule in valid_set:
            added_count += 1

    # write merged delete_counter.json (merge to avoid overwriting other runs)
    save_delete_counter_merge(new_counter)

    # ensure dist dir, write validated file (sorted)
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        for r in sorted(final_rules):
            f.write(r + "\n")

    total_count = len(final_rules)
    stats_line = f"COMMIT_STATS: validated part {part} → 总 {total_count}, 新增 {added_count}, 删除 {removed_count}"
    # print both for logs
    print(f"validated part {part} → 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(stats_line)
    # also flush to stderr to increase chance workflow grep capture
    try:
        import sys
        print(stats_line, file=sys.stderr)
    except:
        pass

# ========== main ==========
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
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
