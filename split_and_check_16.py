import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime, timezone
import time
import socket
import argparse

# ===================== 配置 =====================
URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_WORKERS = 10
DNS_BATCH_SIZE = 500
DNS_TIMEOUT = 1.5
BATCH_SLEEP = 0.5
RETRY_ON_FAIL = True

socket.setdefaulttimeout(DNS_TIMEOUT)

resolver = dns.resolver.Resolver()
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
resolver.timeout = DNS_TIMEOUT
resolver.lifetime = DNS_TIMEOUT

# ===================== 函数 =====================
def safe_fetch(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except Exception:
        print(f"⚠️ 下载失败：{url}")
        return []

def clean_rule(line):
    l = line.strip()
    if not l or l.startswith("#") or l.startswith("!"):
        return None
    return l

def extract_domain(rule):
    return rule.lstrip("|").lstrip(".").split("^")[0].strip()

def is_valid_domain(domain):
    try:
        resolver.resolve(domain, "A")
        return True
    except Exception:
        return False

def check_rule(rule):
    try:
        domain = extract_domain(rule)
        if is_valid_domain(domain):
            return rule, None
        else:
            return None, domain
    except Exception:
        return None, extract_domain(rule)

def validate_batch(rules):
    valid_rules = []
    failed_domains = []
    for i in range(0, len(rules), DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        for r, f in results:
            if r:
                valid_rules.append(r)
            if f:
                failed_domains.append(f)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, len(rules)):,}/{len(rules):,} 条，本批有效 {sum(1 for r,f in results if r):,} 条")
        time.sleep(BATCH_SLEEP)
    return valid_rules, failed_domains

# ===================== 主函数 =====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="手动验证指定分片 0~15")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    part_files = [os.path.join(TMP_DIR, f"part_{i:02d}.txt") for i in range(PARTS)]
    validated_files = [os.path.join(TMP_DIR, f"validated_{i:02d}.txt") for i in range(PARTS)]
    failed_files = [os.path.join(TMP_DIR, f"failed_{i:02d}.txt") for i in range(PARTS)]
    final_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")

    # ===================== 首次切片 =====================
    if not os.path.exists(part_files[0]):
        if not os.path.exists(URLS_FILE):
            print("❌ 未找到 urls.txt")
            return
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

        print("📥 下载规则源...")
        all_rules = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for lines in ex.map(safe_fetch, urls):
                all_rules.extend(lines)

        # 去注释 & 去重
        cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
        total = len(cleaned)
        print(f"✅ 去重后总计：{total:,} 条")

        chunk = total // PARTS
        for idx in range(PARTS):
            start = idx * chunk
            end = None if idx == PARTS - 1 else (idx + 1) * chunk
            part_data = cleaned[start:end]
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(part_data))
            print(f"📄 分片 {idx+1} 保存 {len(part_data):,} 条规则 → {part_files[idx]}")
            print("前 10 条示例：", part_data[:10])

    # ===================== 确定处理分片 =====================
    if args.part is not None:
        part_index = args.part
    else:
        now = datetime.now(timezone.utc)
        minute = now.hour * 60 + now.minute
        part_index = (minute // 25) % PARTS  # 每 25 分钟轮替一次

    target_part = part_files[part_index]
    target_validated = validated_files[part_index]
    target_failed = failed_files[part_index]

    if not os.path.exists(target_part):
        print(f"⚠️ 分片 {part_index} 不存在，跳过")
        return

    with open(target_part, "r", encoding="utf-8") as f:
        rules = [x.strip() for x in f if x.strip()]
    total_rules = len(rules)
    print(f"⏱ 当前处理分片：{target_part}, 总规则 {total_rules:,} 条")
    print("前 10 条规则示例：", rules[:10])

    # ===================== DNS 验证 =====================
    valid_rules, failed_domains = validate_batch(rules)

    # 自动重试失败分片一次
    if RETRY_ON_FAIL and failed_domains:
        print(f"🔄 重试失败域名 {len(failed_domains):,} 条")
        time.sleep(2)
        retry_valid, retry_failed = validate_batch(failed_domains)
        valid_rules.extend(retry_valid)
        failed_domains = retry_failed

    # 保存验证结果
    with open(target_validated, "w", encoding="utf-8") as f:
        f.write("\n".join(valid_rules))
    with open(target_failed, "w", encoding="utf-8") as f:
        f.write("\n".join(failed_domains))

    # 输出分片 summary
    success_count = len(valid_rules)
    fail_count = len(failed_domains)
    success_rate = success_count / total_rules * 100 if total_rules else 0
    print(f"\n🎯 分片 {part_index+1}/{PARTS} Summary:")
    print(f"   总规则: {total_rules:,}")
    print(f"   有效: {success_count:,}")
    print(f"   失败: {fail_count:,}")
    print(f"   成功率: {success_rate:.2f}%\n")

    # 合并所有分片验证结果
    all_valid = []
    for vf in validated_files:
        if os.path.exists(vf):
            with open(vf, "r", encoding="utf-8") as f:
                all_valid.extend([line.strip() for line in f if line.strip()])
    all_valid = list(dict.fromkeys(all_valid))
    with open(final_output, "w", encoding="utf-8") as f:
        f.write("\n".join(all_valid))
    print(f"🎯 最终有效规则生成：{final_output} 共 {len(all_valid):,} 条")
    print("✅ 本次执行完成，无错误")

if __name__ == "__main__":
    main()
