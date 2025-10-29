import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime
import argparse

# ---------------------------
# 配置
# ---------------------------
URLS_FILE = "urls.txt"
DIST_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_BATCH_SIZE = 800
MAX_WORKERS = 80  # DNS 并发线程数

resolver = dns.resolver.Resolver()
resolver.timeout = 1.5
resolver.lifetime = 1.5
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]

# ---------------------------
# 函数
# ---------------------------
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

# ---------------------------
# 主函数
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, default=-1, help="手动验证指定分片（0~15）")
    args = parser.parse_args()
    part_arg = args.part

    os.makedirs(DIST_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    part_files = [os.path.join(TMP_DIR, f"part_{i+1:02}.txt") for i in range(PARTS)]
    valid_output = os.path.join(DIST_DIR, "blocklist_valid.txt")

    # ---------------------------
    # 如果不存在分片或 urls.txt 更新时间大于一天，则更新 urls.txt 并切分
    # ---------------------------
    need_update = False
    if not os.path.exists(URLS_FILE):
        need_update = True
    else:
        mtime = datetime.fromtimestamp(os.path.getmtime(URLS_FILE))
        if (datetime.utcnow() - mtime).days >= 1:
            need_update = True

    if need_update:
        print("🟢 更新 urls.txt 并切片")
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            for lines in ex.map(safe_fetch, urls):
                all_rules.extend(lines)

        cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
        total = len(cleaned)
        print(f"✅ 总计去重规则 {total:,} 条")

        chunk = total // PARTS
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
            print(f"📄 分片 {idx+1} 保存 {len(cleaned[start:end]):,} 条规则 → {part_files[idx]}")
    else:
        print("🟢 urls.txt 当天已更新，无需重复下载")

    # ---------------------------
    # 确定要验证的分片
    # ---------------------------
    if part_arg >= 0 and part_arg < PARTS:
        target_idx = part_arg
    else:
        # 自动轮替，每 1.5 小时处理一个分片
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        target_idx = (minute // 90) % PARTS

    target_file = part_files[target_idx]
    if not os.path.exists(target_file):
        print(f"⚠️ 分片不存在：{target_file}")
        return

    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    print(f"⏱ 当前处理分片：{target_file}, 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # ---------------------------
    # DNS 验证
    # ---------------------------
    valid = []
    total_rules = len(rules)
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        batch_valid = [r for r in results if r]
        valid.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total_rules):,}/{total_rules:,} 条，本批有效 {len(batch_valid):,} 条")

    with open(valid_output, "a", encoding="utf-8") as f:
        f.write("\n".join(valid) + "\n")
    print(f"🎯 本次分片有效 {len(valid):,} 条 → 已追加至 {valid_output}")
    print("✅ 执行结束")

if __name__ == "__main__":
    main()
