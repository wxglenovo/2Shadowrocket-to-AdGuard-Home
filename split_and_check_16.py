import os
import sys
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime
import argparse

URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_BATCH_SIZE = 800
MAX_WORKERS = 80  # 并发线程数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

def safe_fetch(url):
    try:
        print(f"📥 下载：{url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except:
        print(f"⚠️ 下载失败：{url}")
        return []

def clean_rule(line):
    l = line.strip()
    if not l or l.startswith("#") or l.startswith("!") or "browser.events.data.msn.cn^" in l:
        return None
    return l

def extract_domain(rule):
    d = rule.lstrip("|").lstrip(".").split("^")[0]
    return d.strip()

def is_valid_domain(domain):
    try:
        resolver.resolve(domain, "A")
        return True
    except:
        return False

def check_rule(rule):
    domain = extract_domain(rule)
    return rule if is_valid_domain(domain) else None

def split_rules(all_rules):
    os.makedirs(TMP_DIR, exist_ok=True)
    total = len(all_rules)
    chunk = total // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(TMP_DIR, f"part_{idx:02}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(all_rules[start:end]))
        print(f"📄 分片 {idx+1} 保存 {len(all_rules[start:end])} 条规则 → {part_file}")
        print(f"前 10 条示例： {all_rules[start:start+10]}")
        part_files.append(part_file)
    return part_files

def load_rules_from_parts():
    return [os.path.join(TMP_DIR, f"part_{i:02}.txt") for i in range(PARTS)]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动指定分片验证 0~15")
    args = parser.parse_args()

    # 首次下载并切片
    if not os.path.exists(TMP_DIR) or not os.listdir(TMP_DIR):
        if not os.path.exists(URLS_FILE):
            print("❌ 未找到 urls.txt")
            return
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            for lines in ex.map(safe_fetch, urls):
                all_rules.extend(lines)

        cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
        print(f"✅ 去重后总计：{len(cleaned):,} 条")
        split_rules(cleaned)

    # 确定处理哪一份
    part_files = load_rules_from_parts()
    if args.part is not None:
        part_index = args.part
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片不存在：{target_file}")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid_rules = []
    total_checked = 0

    def batch_check(batch):
        valid_batch = []
        for r in batch:
            res = check_rule(r)
            if res:
                valid_batch.append(res)
        return valid_batch

    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        valid_batch = [r for r in results if r]
        valid_rules.extend(valid_batch)
        total_checked += len(batch)
        print(f"✅ 已验证 {total_checked}/{len(rules):,} 条，本批有效 {len(valid_batch)} 条")

    valid_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid_rules) + "\n")

    print(f"🎯 本次完成，共 {len(valid_rules):,} 条有效 → 已追加至 {valid_output}")

if __name__ == "__main__":
    main()
