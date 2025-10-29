import os
import requests
import dns.resolver
import concurrent.futures
import argparse
from datetime import datetime

URLS_FILE = "urls.txt"
OUTPUT_DIR = "tmp"
PARTS = 16
MAX_WORKERS = 80  # DNS 并发线程数
DNS_BATCH_SIZE = 800  # 每批验证数量

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

parser = argparse.ArgumentParser()
parser.add_argument("--part", type=int, help="手动验证指定分片 0-15")
args = parser.parse_args()

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
    if not l or l.startswith("#") or l.startswith("!") or l.startswith("||browser") or l.startswith("||cf.") or l.startswith("||dig.") or l.startswith("||lf-") or l.startswith("||rt.") or l.startswith("||*.ip6.arpa"):
        return None
    return l

def extract_domain(rule):
    return rule.lstrip("|").lstrip(".").split("^")[0].strip()

def is_valid_domain(domain):
    try:
        resolver.resolve(domain, "A")
        return True
    except:
        return False

def check_rule(rule):
    domain = extract_domain(rule)
    return rule if is_valid_domain(domain) else None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 下载 urls.txt 中所有规则
    if not os.path.exists(URLS_FILE):
        print("❌ 未找到 urls.txt")
        return

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    # 去注释、去重
    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)
    print(f"✅ 去重后总计：{total:,} 条")

    # 切成 16 份
    chunk = total // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(OUTPUT_DIR, f"part_{idx+1:02d}.txt")
        part_files.append(part_file)
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end])} 条规则 → {part_file}")
        print(f"前 10 条示例： {cleaned[start:end][:10]}")

    # 确定验证哪一份
    if args.part is not None:
        part_index = args.part
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS  # 每 1.5 小时切换一次

    target_file = part_files[part_index]
    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    valid = []
    count = 0
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        count += len(batch)
        print(f"✅ 已验证 {count}/{len(rules):,} 条，本批有效 {len(batch_valid)} 条")

    valid_output = os.path.join("dist", "blocklist_valid.txt")
    os.makedirs("dist", exist_ok=True)
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"🎯 本次有效规则追加至 {valid_output}")
    print("✅ 执行结束")

if __name__ == "__main__":
    main()
