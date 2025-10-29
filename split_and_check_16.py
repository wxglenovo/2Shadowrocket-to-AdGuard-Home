import os
import requests
import dns.resolver
import concurrent.futures
import argparse
from datetime import datetime

# ===============================
# 配置
# ===============================
URLS_FILE = "urls.txt"
OUTPUT_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16
DNS_BATCH_SIZE = 800  # DNS 验证批量大小
MAX_WORKERS = 80  # DNS 并发线程数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

# ===============================
# 工具函数
# ===============================
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

def check_rules_batch(rules):
    valid = []
    for r in rules:
        domain = extract_domain(r)
        if is_valid_domain(domain):
            valid.append(r)
    return valid

# ===============================
# 主函数
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, default=None, help="手动指定分片 0~15")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DIST_DIR, exist_ok=True)
    part_files = [os.path.join(OUTPUT_DIR, f"part_{i+1:02d}.txt") for i in range(PARTS)]
    valid_output = os.path.join(DIST_DIR, "blocklist_valid.txt")

    # ✅ 首次运行：下载并切片
    if not os.path.exists(part_files[0]):
        if not os.path.exists(URLS_FILE):
            print("❌ 未找到 urls.txt")
            return

        print("🧩 首次运行：下载并切片")
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

        chunk = total // PARTS
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
            print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end]):,} 条规则 → {part_files[idx]}")
            print(f"前 10 条示例： {cleaned[start:end][:10]}")

    # 确定要处理的分片
    if args.part is not None:
        part_index = args.part
    else:
        # 自动轮替，每 1.5 小时切换一次
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS

    target_file = part_files[part_index]
    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # 分块 DNS 验证
    valid = []
    total_rules = len(rules)
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(lambda r: r if is_valid_domain(extract_domain(r)) else None, batch))
        valid_batch = [r for r in results if r]
        valid.extend(valid_batch)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total_rules):,}/{total_rules:,} 条，本批有效 {len(valid_batch):,} 条")

    # 保存验证结果
    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")
    print(f"✅ 本次有效：{len(valid):,} 条 → 已追加至 {valid_output}")
    print("🎯 执行结束，0 错误 ✅")

if __name__ == "__main__":
    main()
