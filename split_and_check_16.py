import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime, timezone
import argparse

URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_BATCH_SIZE = 800
MAX_WORKERS = 80

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

parser = argparse.ArgumentParser()
parser.add_argument("--part", type=int, help="手动触发验证分片序号（0-15）")
args = parser.parse_args()

def safe_fetch(url):
    try:
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

def check_batch(batch):
    valid = []
    for rule in batch:
        domain = extract_domain(rule)
        if is_valid_domain(domain):
            valid.append(rule)
    return valid

def split_rules():
    if not os.path.exists(URLS_FILE):
        print("❌ 未找到 urls.txt")
        return

    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)
    print(f"✅ 总规则数（去注释去重后）：{total:,} 条")

    chunk = total // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(TMP_DIR, f"part_{idx:02}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end])} 条规则 → {part_file}")
        part_files.append(part_file)
    return part_files

def validate_part(part_file):
    with open(part_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    total = len(rules)
    valid_rules = []
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        valid = check_batch(batch)
        valid_rules.extend(valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total):,}/{total:,} 条，本批有效 {len(valid):,} 条")
    valid_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid_rules) + "\n")
    print(f"🎯 分片 {os.path.basename(part_file)} 验证完成，有效 {len(valid_rules):,} 条 → 已追加至 {valid_output}")
    if valid_rules:
        print(f"前 10 条规则示例： {valid_rules[:10]}")

def main():
    part_files = [os.path.join(TMP_DIR, f"part_{i:02}.txt") for i in range(PARTS)]
    if not all(os.path.exists(f) for f in part_files):
        print("🧩 分片不存在，开始切分规则")
        part_files = split_rules()

    if args.part is not None:
        if 0 <= args.part < PARTS:
            print(f"🛠 手动触发，验证分片 {args.part}")
            validate_part(part_files[args.part])
        else:
            print("❌ 分片序号不正确，应为 0-15")
        return

    # 自动轮替当前分片，每 1.5 小时轮换
    now = datetime.now(timezone.utc)
    minute = now.hour * 60 + now.minute
    part_index = (minute // 90) % PARTS
    print(f"⏱ 当前处理分片：{part_files[part_index]}, 总规则 {sum(1 for _ in open(part_files[part_index])):,} 条")
    validate_part(part_files[part_index])

if __name__ == "__main__":
    main()
