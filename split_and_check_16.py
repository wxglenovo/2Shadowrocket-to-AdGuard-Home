import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime
import argparse
import time

URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
PARTS = 16
MAX_WORKERS = 80
DNS_BATCH_SIZE = 200  # 每批处理200条 DNS

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
    if not l or l.startswith("#") or l.startswith("!") or l.startswith("||browser.events") or l.startswith("||cf.iadsdk") \
       or l.startswith("||dig.bdurl") or l.startswith("||lf-static") or l.startswith("||rt.applovin") or l.startswith("||*.ip6.arpa"):
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

def check_batch(rules):
    valid = []
    for rule in rules:
        domain = extract_domain(rule)
        if is_valid_domain(domain):
            valid.append(rule)
    return valid

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动指定分片 0~15")
    args = parser.parse_args()
    manual_part = args.part

    if not os.path.exists(URLS_FILE):
        print("❌ 未找到 urls.txt")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    part_files = [os.path.join(OUTPUT_DIR, f"part_{i}.txt") for i in range(PARTS)]
    valid_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")

    # 首次运行，下载切片
    if not os.path.exists(part_files[0]):
        print("🧩 首次运行：下载并切片")
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
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
        print(f"✅ 切成 {PARTS} 份，每份约 {chunk:,} 条")
        return

    # 确定当前分片
    if manual_part is not None:
        part_index = manual_part
        print(f"⏱ 手动验证分片：{part_index}")
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 25) % PARTS
        print(f"⏱ 自动轮替验证分片：{part_index}")

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print("⚠️ 分片不存在，跳过")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"🔍 当前分片规则：{len(rules):,} 条")

    valid = []
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = ex.map(lambda r: r if is_valid_domain(extract_domain(r)) else None, batch)
            valid.extend([r for r in results if r])

    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"✅ 本次有效：{len(valid):,} 条 → 已追加至 {valid_output}")
    print("🎯 执行结束，0 错误 ✅")

if __name__ == "__main__":
    main()
