import os
import requests
import dns.resolver
import concurrent.futures
import argparse
from datetime import datetime

URLS_FILE = "urls.txt"
OUTPUT_DIR = "tmp"
PARTS = 16
DNS_BATCH_SIZE = 800
MAX_WORKERS = 80

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
    if not l or l.startswith("#") or l.startswith("!") or l.startswith("||") is False:
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, default=None, help="手动指定验证分片编号 0-15")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    part_files = [os.path.join(OUTPUT_DIR, f"part_{i:02d}.txt") for i in range(PARTS)]
    valid_output = os.path.join("dist", "blocklist_valid.txt")
    os.makedirs("dist", exist_ok=True)

    # 首次切片
    if not os.path.exists(part_files[0]):
        if not os.path.exists(URLS_FILE):
            print("❌ urls.txt 未找到")
            return
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            for lines in ex.map(safe_fetch, urls):
                all_rules.extend(lines)

        cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
        total = len(cleaned)
        print(f"✅ 去重后总计：{total:,} 条规则")

        chunk = total // PARTS
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
            print(f"📄 分片 {idx+1:02d} 保存 {len(cleaned[start:end]):,} 条规则 → {part_files[idx]}")
            print(f"前 10 条示例： {cleaned[start:end][:10]}")
        return

    # 确定当前处理分片
    if args.part is not None:
        part_index = args.part
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS

    target_file = part_files[part_index]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片 {target_file} 不存在，跳过")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # DNS 验证
    valid = []
    total_rules = len(rules)
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        results = check_batch(batch)
        valid.extend(results)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total_rules):,}/{total_rules:,} 条，本批有效 {len(results):,} 条")

    # 保存有效规则
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"🎯 本次处理完成，有效规则 {len(valid):,} 条 → 已追加至 {valid_output}")

if __name__ == "__main__":
    main()
