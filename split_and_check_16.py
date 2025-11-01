#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

PARTS = 16
DNS_WORKERS = 50
DNS_BATCH_SIZE = 300

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


# ✅ 读取源文件
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return [i.strip() for i in f if i.strip()]


# ✅ 下载并合并所有规则
def download_and_merge(urls):
    all_rules = []
    for u in urls:
        try:
            print(f"⬇ 下载: {u}")
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


# ✅ 分片
def split_rules():
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [i.strip() for i in f if i.strip()]
    total = len(rules)
    size = total // PARTS + 1

    for i in range(PARTS):
        part_rules = rules[i * size:(i + 1) * size]
        path = os.path.join(DIST_DIR, f"validated_part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 生成分片 {i+1:02d}: {len(part_rules)} 条")


# ✅ 加载连续删除计数文件
def load_delete_counter():
    if not os.path.exists(DELETE_COUNTER_FILE):
        print("⚠ delete_counter.json 不存在 → 自动创建")
        return {}
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ✅ 保存连续删除计数
def save_delete_counter(data):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ✅ DNS批量检查（并发）
def dns_check_batch(domains):
    results = {}
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        future_to_domain = {
            executor.submit(dns_check, d): d for d in domains
        }
        for future in as_completed(future_to_domain):
            d = future_to_domain[future]
            try:
                results[d] = future.result()
            except:
                results[d] = False
    return results


def dns_check(domain):
    try:
        dns.resolver.resolve(domain, 'A', lifetime=3)
        return True
    except:
        return False


# ✅ 验证一个分片
def validate_part(index):
    part_file = os.path.join(DIST_DIR, f"validated_part_{index:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在：{part_file}")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [i.strip() for i in f if i.strip()]

    delete_counter = load_delete_counter()

    kept = []
    deleted = []

    # 批量 DNS 测试
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i + DNS_BATCH_SIZE]
        domains = [r.replace("||", "").replace("^", "") for r in batch]
        dns_results = dns_check_batch(domains)

        for rule, domain in zip(batch, domains):
            ok = dns_results[domain]
            if ok:
                kept.append(rule)
                if rule in delete_counter:
                    del delete_counter[rule]
            else:
                delete_counter[rule] = delete_counter.get(rule, 0) + 1
                if delete_counter[rule] < 4:
                    kept.append(rule)
                else:
                    deleted.append(rule)

    # 保存结果
    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(kept))

    save_delete_counter(delete_counter)

    print(f"✅ part {index:02d} → 总 {len(rules)}, 保留 {len(kept)}, 删除 {len(deleted)}")


# ✅ 主流程
if __name__ == "__main__":
    urls = load_urls()

    if not os.path.exists(MERGED_FILE):
        print("⚠ 没找到合并规则 → 自动下载")
        download_and_merge(urls)
        split_rules()

    for i in range(1, PARTS + 1):
        validate_part(i)

    print("✅ 所有分片处理完毕 ✅")
