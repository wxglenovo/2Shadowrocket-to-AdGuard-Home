#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import concurrent.futures
import dns.resolver
import re
from tqdm import tqdm
import glob
import os

# ===============================
# 创建临时目录
# ===============================
os.makedirs("tmp", exist_ok=True)
os.makedirs("dist", exist_ok=True)

# ===============================
# DNS 配置
# ===============================
resolver = dns.resolver.Resolver()
resolver.timeout = 1
resolver.lifetime = 1

# ===============================
# 域名正则
# ===============================
domain_regex = re.compile(r"^(?!-)([A-Za-z0-9-]{1,63}\.)+[A-Za-z]{2,}$")

# ===============================
# 提取规则域名
# ===============================
def extract_domain(rule: str) -> str:
    rule = rule.strip().lstrip("|").lstrip("@@").split("^")[0]
    rule = rule.replace("*", "").replace("||", "")
    return rule

# ===============================
# 验证域名有效性
# ===============================
def is_valid_domain(rule: str) -> bool:
    domain = extract_domain(rule)
    if not domain or not domain_regex.match(domain):
        return False
    try:
        resolver.resolve(domain, "A")
        return True
    except dns.resolver.NXDOMAIN:
        return False
    except (dns.resolver.NoNameservers, dns.resolver.Timeout, dns.resolver.YXDOMAIN):
        return True
    except:
        return True

# ===============================
# 批量验证
# ===============================
def validate_batch(rules, batch_size=50000, max_workers=200):
    valid = []
    total_rules = len(rules)

    for i in range(0, total_rules, batch_size):
        batch = rules[i:i+batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(tqdm(executor.map(is_valid_domain, batch), total=len(batch), desc=f"Batch {i//batch_size+1}"))
            for rule, ok in zip(batch, results):
                if ok:
                    valid.append(rule)
        print(f"Processed {min(i+batch_size, total_rules)}/{total_rules} rules, valid: {len(valid)}")
    return valid

# ===============================
# 读取所有源文件
# ===============================
all_rules = []
for file_path in sorted(glob.glob("tmp/*")):
    with open(file_path, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        all_rules.extend(rules)

print(f"Total rules loaded from all sources: {len(all_rules)}")

# ===============================
# 验证规则
# ===============================
valid_rules = validate_batch(all_rules)

# ===============================
# 写入结果
# ===============================
with open("dist/valid_rules.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(valid_rules))

print(f"✅ Finished validation: total {len(all_rules)}, valid {len(valid_rules)}, removed {len(all_rules)-len(valid_rules)}")
