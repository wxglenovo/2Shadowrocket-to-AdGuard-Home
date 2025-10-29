import os
import requests
import dns.resolver
import concurrent.futures
import argparse
from datetime import datetime

URLS_FILE = "urls.txt"
OUTPUT_DIR = "tmp"
BLOCKLIST_FILE = "dist/blocklist_valid.txt"
PARTS = 16
DNS_BATCH_SIZE = 800
MAX_WORKERS = 80  # 并发线程数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9", "114.114.114.114", "114.114.114.119", "2400:3200::1", "223.5.5.5", "2400:3200:baba::1", "119.29.29.29"]

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

def save_part_file(index, rules):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    part_file = os.path.join(OUTPUT_DIR, f"part_{index+1:02d}.txt")
    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(rules))
    print(f"📄 分片 {index+1} 保存 {len(rules):,} 条规则 → {part_file}")
    print(f"前 10 条示例： {rules[:10]}")

def load_part_file(index):
    part_file = os.path.join(OUTPUT_DIR, f"part_{index+1:02d}.txt")
    if not os.path.exists(part_file):
        return []
    with open(part_file, "r", encoding="utf-8") as f:
        return f.read().splitlines()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动指定分片验证 0~15")
    args = parser.parse_args()

    # 首次下载并切片
    first_run = not all(os.path.exists(os.path.join(OUTPUT_DIR, f"part_{i+1:02d}.txt")) for i in range(PARTS))
    if first_run:
        print("🧩 首次运行：下载并切片")
        if not os.path.exists(URLS_FILE):
            print("❌ urls.txt 不存在")
            return
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
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            save_part_file(idx, cleaned[start:end])

    # 确定处理分片
    if args.part is not None:
        part_index = args.part
        print(f"🛠 手动触发，验证分片 {part_index+1}")
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS
        print(f"⏱ 自动轮替当前分片 {part_index+1}")

    rules = load_part_file(part_index)
    total_rules = len(rules)
    print(f"⏱ 当前处理分片：{OUTPUT_DIR}/part_{part_index+1:02d}.txt, 总规则 {total_rules:,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid = []
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total_rules):,}/{total_rules:,} 条，本批有效 {len(batch_valid):,} 条")

    os.makedirs(os.path.dirname(BLOCKLIST_FILE), exist_ok=True)
    with open(BLOCKLIST_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")
    print(f"✅ 本次有效总计 {len(valid):,} 条 → 已追加至 {BLOCKLIST_FILE}")
    print("🎯 执行结束 ✅")

if __name__ == "__main__":
    main()
