import os
import requests
import argparse
import time
import dns.resolver

DNS_BATCH_SIZE = 800
PARTS = 16
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--part", type=int, help="指定验证分片 0~15")
args = parser.parse_args()

# 1️⃣ 每天更新 urls.txt
if not os.path.exists(URLS_TXT) or (time.time() - os.path.getmtime(URLS_TXT)) > 24*3600:
    print("📥 下载最新 urls.txt")
    url = "https://raw.githubusercontent.com/wxglenovo/Shadowrocket-to-AdGuard-Home/main/urls.txt"
    r = requests.get(url)
    with open(URLS_TXT, "w", encoding="utf-8") as f:
        f.write(r.text)
urls = [line.strip() for line in open(URLS_TXT, encoding="utf-8") if line.strip()]

total_rules = len(urls)
batch_count = (total_rules + DNS_BATCH_SIZE - 1) // DNS_BATCH_SIZE

# 分片
split_size = (total_rules + PARTS - 1) // PARTS
parts = [urls[i*split_size:(i+1)*split_size] for i in range(PARTS)]

def check_dns(rule):
    try:
        domain = rule.lstrip("|").rstrip("^")
        dns.resolver.resolve(domain, 'A')
        return True
    except:
        return False

# 2️⃣ 处理指定分片或全部分片
part_list = [args.part] if args.part is not None else list(range(PARTS))
for idx in part_list:
    part_rules = parts[idx]
    valid_rules = []
    print(f"📄 分片 {idx+1} 保存 {len(part_rules)} 条规则 → {TMP_DIR}/part_{idx+1:02}.txt")
    print("前 10 条示例：", part_rules[:10])

    for i, rule in enumerate(part_rules):
        if check_dns(rule):
            valid_rules.append(rule)
        if (i+1) % DNS_BATCH_SIZE == 0 or (i+1) == len(part_rules):
            print(f"✅ 已验证 {i+1}/{len(part_rules)} 条，本批有效 {len(valid_rules)} 条")

    # 保存分片
    with open(f"{TMP_DIR}/part_{idx+1:02}.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(part_rules))
    
# 3️⃣ 合并所有有效规则
all_valid = []
for idx in range(PARTS):
    with open(f"{TMP_DIR}/part_{idx+1:02}.txt", encoding="utf-8") as f:
        all_valid.extend([line.strip() for line in f if line.strip()])
with open(f"{DIST_DIR}/blocklist_valid.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(all_valid))
print(f"✅ 合并完成，共 {len(all_valid)} 条有效规则 → {DIST_DIR}/blocklist_valid.txt")
