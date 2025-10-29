import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime, timezone
import argparse

URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16
MAX_WORKERS = 80
DNS_BATCH_SIZE = 800
VALID_OUTPUT = os.path.join(DIST_DIR, "blocklist_valid.txt")

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

def safe_fetch(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except:
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

def split_rules():
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(DIST_DIR, exist_ok=True)
    if not os.path.exists(URLS_FILE):
        print("❌ 未找到 urls.txt")
        return

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    print(f"⬇️ 下载 {len(urls)} 个源...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    # 去注释 + 去重
    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)
    chunk = total // PARTS

    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(TMP_DIR, f"part_{idx+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end]):,} 条规则 → {part_file}")
        print(f"前 10 条示例： {cleaned[start:end][:10]}")

def validate_part(part_index=None):
    os.makedirs(TMP_DIR, exist_ok=True)
    part_files = [os.path.join(TMP_DIR, f"part_{i+1:02d}.txt") for i in range(PARTS)]

    if part_index is None:
        # 自动轮替
        now = datetime.now(timezone.utc)
        minute = now.hour * 60 + now.minute
        part_index = (minute // 25) % PARTS

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片不存在：{target_file}")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    total = len(rules)
    print(f"⏱ 当前处理分片：{target_file}, 总规则 {total:,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid = []
    verified_count = 0
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        verified_count += len(batch)
        print(f"前 10 条示例： {batch[:10]}")
        print(f"✅ 已验证 {verified_count:,}/{total:,} 条，本批有效 {len(batch_valid):,} 条")

    # 保存有效规则
    os.makedirs(DIST_DIR, exist_ok=True)
    with open(VALID_OUTPUT, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")
    print(f"🎯 本分片有效总计：{len(valid):,} 条 → 已追加至 {VALID_OUTPUT}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", action="store_true", help="切分规则")
    parser.add_argument("--part", type=int, help="验证指定分片 0~15")
    args = parser.parse_args()

    if args.split:
        split_rules()
    else:
        validate_part(args.part)
