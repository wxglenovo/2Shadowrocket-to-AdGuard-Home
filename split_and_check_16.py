import requests
import os
import argparse
import time
import random
import dns.resolver

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16
DNS_BATCH_SIZE = 800

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 获取规则
# ===============================
def update_urls():
    print("📥 更新 urls.txt")
    url = "https://raw.githubusercontent.com/wxglenovo/AdGuardHome-Filter/refs/heads/main/urls.txt"
    r = requests.get(url)
    r.raise_for_status()
    with open(URLS_TXT, "w", encoding="utf-8") as f:
        f.write(r.text)
    print(f"✅ urls.txt 更新完成，共 {len(r.text.splitlines())} 条规则")

# ===============================
# 切分规则
# ===============================
def split_rules():
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    parts_files = []
    for i in range(PARTS):
        part_rules = rules[i*per_part : (i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        parts_files.append(filename)
        print(f"📄 分片 {i+1} 保存 {len(part_rules)} 条规则 → {filename}")
        print(f"前 10 条示例： {part_rules[:10]}")
    return parts_files, total

# ===============================
# DNS 验证
# ===============================
def validate_rules(filename):
    with open(filename, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip()]
    total = len(rules)
    valid_rules = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2

    for i in range(0, total, DNS_BATCH_SIZE):
        batch = rules[i:i+DNS_BATCH_SIZE]
        batch_valid = []
        for rule in batch:
            domain = rule.lstrip("|").rstrip("^").split("/")[0]
            try:
                resolver.resolve(domain)
                batch_valid.append(rule)
            except:
                continue
        valid_rules.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE, total)}/{total} 条，本批有效 {len(batch_valid)} 条")
        print(f"前 10 条规则示例： {batch_valid[:10]}")
        time.sleep(random.uniform(0.5,1.5))
    return valid_rules, total

# ===============================
# 主流程
# ===============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片 1~16")
    args = parser.parse_args()

    if not os.path.exists(URLS_TXT):
        update_urls()

    parts_files, total_rules = split_rules()

    if args.part:
        idx = args.part - 1
        if 0 <= idx < PARTS:
            print(f"⏱ 当前处理分片：{parts_files[idx]}, 总规则 {total_rules} 条")
            valid_rules, _ = validate_rules(parts_files[idx])
            part_file = os.path.join(TMP_DIR, f"validated_{idx+1:02d}.txt")
            with open(part_file, "w", encoding="utf-8") as f:
                f.write("\n".join(valid_rules))
        else:
            print("❌ 分片编号错误，应为 1~16")
    else:
        # 自动轮替验证，按顺序处理每个分片
        for idx, part_file in enumerate(parts_files):
            print(f"⏱ 当前处理分片：{part_file}, 总规则 {total_rules} 条")
            valid_rules, _ = validate_rules(part_file)
            validated_file = os.path.join(TMP_DIR, f"validated_{idx+1:02d}.txt")
            with open(validated_file, "w", encoding="utf-8") as f:
                f.write("\n".join(valid_rules))
            time.sleep(2)

    # 汇总总有效规则
    all_valid = []
    for i in range(PARTS):
        validated_file = os.path.join(TMP_DIR, f"validated_{i+1:02d}.txt")
        if os.path.exists(validated_file):
            with open(validated_file, "r", encoding="utf-8") as f:
                all_valid.extend([line.strip() for line in f if line.strip()])
    with open(os.path.join(DIST_DIR, "blocklist_valid.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(all_valid))
    print(f"🎯 总有效规则保存到 {os.path.join(DIST_DIR, 'blocklist_valid.txt')} 共 {len(all_valid)} 条")

if __name__ == "__main__":
    main()
