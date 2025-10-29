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
DNS_BATCH_SIZE = 800  # 每批 DNS 验证数量
MAX_WORKERS = 80      # DNS 并发线程数

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
    if not l or l.startswith("#") or l.startswith("!"):
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
        print("❌ urls.txt 未找到")
        sys.exit(1)
    
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    # 清理注释并去重
    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)
    print(f"✅ 分片前去重总计：{total:,} 条")

    chunk = total // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(TMP_DIR, f"part_{idx+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        part_files.append(part_file)
        print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end]):,} 条规则 → {part_file}")
        print(f"前 10 条示例： {cleaned[start:end][:10]}")
    return part_files

def validate_part(part_file):
    with open(part_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    total_rules = len(rules)
    print(f"⏱ 当前处理分片：{part_file}, 总规则 {total_rules:,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid = []
    verified_count = 0

    # 分批 DNS 验证
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        verified_count += len(batch)
        print(f"✅ 已验证 {verified_count}/{total_rules} 条，本批有效 {len(batch_valid)} 条")

    # 保存有效规则
    valid_file = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")
    with open(valid_file, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"🎯 本次分片有效规则已追加至 {valid_file} → {len(valid):,} 条")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动验证指定分片 0-15")
    args = parser.parse_args()

    # 首次运行或更新 urls.txt 时切分
    part_files = [os.path.join(TMP_DIR, f"part_{i+1:02d}.txt") for i in range(PARTS)]
    if not all(os.path.exists(pf) for pf in part_files):
        print("🧩 首次运行或更新 urls.txt：下载并切片")
        part_files = fetch_and_split()

    # 确定处理的分片
    if args.part is not None:
        if 0 <= args.part < PARTS:
            target_file = part_files[args.part]
        else:
            print("❌ part 参数无效，应为 0-15")
            sys.exit(1)
    else:
        # 自动轮替，每 1.5 小时轮一次
        now = datetime.utcnow()
        minute = now.hour * 60 + now.minute
        part_index = (minute // 90) % PARTS
        target_file = part_files[part_index]

    validate_part(target_file)

if __name__ == "__main__":
    main()
