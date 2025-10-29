from datetime import datetime, timezone
import os
import requests
import concurrent.futures
import dns.resolver

# 配置
URLS_FILE = "urls.txt"
OUTPUT_DIR = "dist"
TMP_DIR = "tmp"
PARTS = 16
MAX_WORKERS = 40  # DNS 并发线程数

resolver = dns.resolver.Resolver()
resolver.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
resolver.timeout = 1.5
resolver.lifetime = 1.5

# 下载单个源
def safe_fetch(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.splitlines()
    except:
        return []

# 清理规则
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
    except:
        return False

def check_rule(rule):
    domain = extract_domain(rule)
    return rule if is_valid_domain(domain) else None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    part_files = [os.path.join(TMP_DIR, f"part_{i:02d}.txt") for i in range(PARTS)]
    validated_files = [os.path.join(TMP_DIR, f"validated_{i:02d}.txt") for i in range(PARTS)]
    final_output = os.path.join(OUTPUT_DIR, "blocklist_valid.txt")

    # 1️⃣ 下载最新源并合并
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip() and not x.startswith("#")]

    all_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for lines in ex.map(safe_fetch, urls):
            all_rules.extend(lines)

    # 2️⃣ 去注释 + 去重
    cleaned = list(dict.fromkeys([clean_rule(x) for x in all_rules if clean_rule(x)]))
    total = len(cleaned)

    # 3️⃣ 切 16 份
    chunk = total // PARTS
    for idx in range(PARTS):
        start = idx * chunk
        end = None if idx == PARTS - 1 else (idx + 1) * chunk
        with open(part_files[idx], "w", encoding="utf-8") as f:
            f.write("\n".join(cleaned[start:end]))

    # 4️⃣ 当前分片（每 1.5 小时轮替）
    now = datetime.now(timezone.utc)
    minute = now.hour * 60 + now.minute
    part_index = (minute // 90) % PARTS
    target_part = part_files[part_index]
    target_validated = validated_files[part_index]

    if not os.path.exists(target_part):
        print(f"分片 {target_part} 不存在")
        return

    with open(target_part, "r", encoding="utf-8") as f:
        rules = [x.strip() for x in f if x.strip()]

    # 5️⃣ DNS 验证
    valid_rules = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(check_rule, rules))

    for r in results:
        if r:
            valid_rules.append(r)

    # 6️⃣ 保存当前分片验证结果
    with open(target_validated, "w", encoding="utf-8") as f:
        f.write("\n".join(valid_rules))

    # 7️⃣ 合并所有 validated
    all_valid = []
    for vf in validated_files:
        if os.path.exists(vf):
            with open(vf, "r", encoding="utf-8") as f:
                all_valid.extend([line.strip() for line in f if line.strip()])

    all_valid = list(dict.fromkeys(all_valid))
    with open(final_output, "w", encoding="utf-8") as f:
        f.write("\n".join(all_valid))

    print(f"最终有效规则生成：{final_output} 共 {len(all_valid):,} 条")

if __name__ == "__main__":
    main()
