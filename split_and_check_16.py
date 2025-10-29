import os
import sys
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
MAX_WORKERS = 80  # 并行 DNS 查询线程数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

def fetch_rules(url):
    try:
        print(f"📥 下载规则：{url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except Exception as e:
        print(f"⚠️ 下载失败：{url} - {e}")
        return []

def clean_rule(line):
    line = line.strip()
    if not line or line.startswith(("!", "#")):
        return None
    return line

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动验证指定分片 0-15")
    args = parser.parse_args()
    manual_part = args.part

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    part_files = [os.path.join(TMP_DIR, f"part_{i+1:02}.txt") for i in range(PARTS)]
    valid_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")

    # 首次执行，下载 urls.txt 并切片
    if not os.path.exists(part_files[0]):
        if not os.path.exists(URLS_FILE):
            print("❌ 未找到 urls.txt")
            return

        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            for lines in ex.map(fetch_rules, urls):
                all_rules.extend(lines)

        cleaned = list(dict.fromkeys([clean_rule(r) for r in all_rules if clean_rule(r)]))
        total = len(cleaned)
        print(f"✅ 去重后总计：{total:,} 条")

        chunk = total // PARTS
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
            print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end]):,} 条规则 → {part_files[idx]}")
            print(f"前 10 条示例： {cleaned[start:end][:10]}")
        return

    # 确定分片
    if manual_part is not None:
        part_index = manual_part
        print(f"🛠 手动触发验证分片 {part_index}")
    else:
        now = datetime.now(timezone.utc)
        minute = now.hour * 60 + now.minute
        part_index = (minute // 90) % PARTS
        print(f"⏱ 自动轮替验证当前分片：{part_index}")

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片不存在：{target_file}")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid_rules = []
    total_validated = 0

    # 分批 DNS 验证
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            result = list(ex.map(lambda r: r if is_valid_domain(extract_domain(r)) else None, batch))
        batch_valid = [r for r in result if r]
        valid_rules.extend(batch_valid)
        total_validated += len(batch)
        print(f"✅ 已验证 {total_validated:,}/{len(rules):,} 条，本批有效 {len(batch_valid):,} 条")

    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid_rules) + "\n")

    print(f"🎯 本次分片验证完成，有效规则 {len(valid_rules):,} 条，已追加至 {valid_output}")

if __name__ == "__main__":
    main()
