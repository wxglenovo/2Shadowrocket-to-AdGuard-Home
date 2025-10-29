import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime
import argparse

# ==========================
# 配置
# ==========================
URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
MAX_WORKERS = 80
DNS_BATCH_SIZE = 800  # 每批 DNS 验证数量

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

# ==========================
# 函数
# ==========================
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
    if not l or l.startswith("#") or l.startswith("!") or l.startswith("||browser.events"):
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

def check_rule_batch(rules):
    valid = []
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(lambda r: r if is_valid_domain(extract_domain(r)) else None, batch))
        valid.extend([r for r in results if r])
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, len(rules))}/{len(rules)} 条，本批有效 {len([r for r in results if r])} 条")
    return valid

# ==========================
# 主流程
# ==========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动验证分片编号 0~15", default=None)
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    # 下载 urls.txt 并解析
    if not os.path.exists(URLS_FILE):
        print(f"❌ 未找到 {URLS_FILE}")
        return

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total_rules = len(cleaned)
    print(f"🧩 去注释去重后总计：{total_rules:,} 条规则")

    # 切分 16 份
    chunk = total_rules // PARTS
    part_files = []
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        part_file = os.path.join(TMP_DIR, f"part_{idx:02}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))
        print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end])} 条规则 → {part_file}")
        print(f"前 10 条示例： {cleaned[start:end][:10]}")
        part_files.append(part_file)

    # 选择分片
    if args.part is not None:
        part_index = args.part
    else:
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS  # 每 1.5 小时轮替

    target_file = part_files[part_index]
    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # DNS 验证
    valid = check_rule_batch(rules)

    # 输出有效规则
    valid_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")

    print(f"🎯 本次有效规则 {len(valid):,} 条 → 已追加至 {valid_output}")
    print("✅ 验证完成")

if __name__ == "__main__":
    main()
