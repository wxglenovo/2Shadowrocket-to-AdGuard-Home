import os
import requests
import dns.resolver
import concurrent.futures
from datetime import datetime, timezone
import time
import socket

# ===================== 配置 =====================
URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
DNS_WORKERS = 10        # 并行 DNS 查询线程数
DNS_BATCH_SIZE = 500    # 每批验证数量
DNS_TIMEOUT = 1.5       # DNS 超时（秒）
BATCH_SLEEP = 0.5       # 每批验证间隔秒

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

# ===================== 主函数 =====================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    part_files = [os.path.join(TMP_DIR, f"part_{i:02d}.txt") for i in range(PARTS)]
    validated_files = [os.path.join(TMP_DIR, f"validated_{i:02d}.txt") for i in range(PARTS)]
    failed_files = [os.path.join(TMP_DIR, f"failed_{i:02d}.txt") for i in range(PARTS)]
    final_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")

    # ✅ 首次运行：下载 urls.txt 并切片
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
            with open(part_files[idx], "w", encoding="utf-8") as f:
                f.write("\n".join(cleaned[start:end]))
        print(f"✅ 切成 {PARTS} 份，每份约 {chunk:,} 条")
        return

    # 当前分片，每 1.5 小时轮替
    now = datetime.now(timezone.utc)
    minute = now.hour * 60 + now.minute
    part_index = (minute // 90) % PARTS
    target_part = part_files[part_index]
    target_validated = validated_files[part_index]
    target_failed = failed_files[part_index]

    print(f"⏱ 当前处理分片：{target_part}")

    if not os.path.exists(target_part):
        print("⚠️ 分片不存在，跳过")
        return

    with open(target_part, "r", encoding="utf-8") as f:
        rules = [x.strip() for x in f if x.strip()]
    total_rules = len(rules)
    print(f"🔍 当前分片规则总数：{total_rules:,} 条")

    # DNS 验证（分批处理）
    valid_rules = []
    failed_domains = []
    for i in range(0, total_rules, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
            results = list(ex.map(check_rule, batch))
        for r, f in results:
            if r:
                valid_rules.append(r)
            if f:
                failed_domains.append(f)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total_rules):,}/{total_rules:,} 条，本批有效 {sum(1 for r,f in results if r):,} 条")
        time.sleep(BATCH_SLEEP)

    # 保存当前分片验证结果
    with open(target_validated, "w", encoding="utf-8") as f:
        f.write("\n".join(valid_rules))
    with open(target_failed, "w", encoding="utf-8") as f:
        f.write("\n".join(failed_domains))

    # 输出 summary
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
