import os
import sys
import argparse
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime

URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
PARTS = 16
MAX_WORKERS = 80
DNS_BATCH_SIZE = 200  # 每批验证数量

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
    if not l or l.startswith("#"):
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

def fetch_and_split():
    if not os.path.exists(URLS_FILE):
        print("❌ 未找到 urls.txt")
        sys.exit(1)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)
    print(f"✅ 去重后总计：{total:,} 条")

    chunk = total // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(OUTPUT_DIR, f"part_{idx}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        part_files.append(part_file)
    print(f"✅ 切成 {PARTS} 份，每份约 {chunk:,} 条")
    return part_files

def validate_part(part_file):
    if not os.path.exists(part_file):
        print(f"⚠️ 分片不存在：{part_file}")
        return []

    with open(part_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    valid = []
    total = len(rules)
    print(f"🔍 当前分片规则：{total:,} 条")

    # 分批 DNS 验证
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        valid.extend([r for r in results if r])
        print(f"  🔹 已验证 {min(i+DNS_BATCH_SIZE, total)}/{total}")

    print(f"✅ 分片有效规则：{len(valid):,} 条")
    return valid

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动验证指定分片 0~15")
    args = parser.parse_args()

    # 首次运行没有分片，下载并切分
    first_part_file = os.path.join(OUTPUT_DIR, "part_0.txt")
    if not os.path.exists(first_part_file):
        print("🧩 首次运行：下载并切片")
        fetch_and_split()

    # 自动轮替分片
    if args.part is not None:
        part_index = args.part
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS

    part_file = os.path.join(OUTPUT_DIR, f"part_{part_index}.txt")
    print(f"⏱ 当前处理分片：{part_file}")

    valid_rules = validate_part(part_file)

    output_file = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")
    with open(output_file, "a", encoding="utf-8") as f:
        f.write("\n".join(valid_rules) + "\n")
    print(f"✅ 已追加有效规则至 {output_file}")

if __name__ == "__main__":
    main()
