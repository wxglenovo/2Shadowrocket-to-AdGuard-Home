#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import concurrent.futures
import dns.resolver
import re
from tqdm import tqdm

# ===============================
# DNS 配置
# ===============================
resolver = dns.resolver.Resolver()
resolver.timeout = 1
resolver.lifetime = 1

# ===============================
# 域名正则
# ===============================
domain_regex = re.compile(
    r"^(?!-)([A-Za-z0-9-]{1,63}\.)+[A-Za-z]{2,}$"
)

# ===============================
# 提取规则中的域名
# ===============================
def extract_domain(rule: str) -> str:
    rule = rule.strip().lstrip("|").lstrip("@@").split("^")[0]
    rule = rule.replace("*", "").replace("||", "")
    return rule

# ===============================
# 验证域名是否有效
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
        # SERVFAIL / REFUSED / TIMEOUT 都视为有效
        return True
    except:
        return True

# ===============================
# 批量验证函数
# ===============================
def validate_batch(input_file: str, output_file: str, batch_size: int = 50000, max_workers: int = 200):
    # 读取规则
    with open(input_file, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    valid = []
    total_rules = len(rules)

    for i in range(0, total_rules, batch_size):
        batch = rules[i:i+batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(tqdm(executor.map(is_valid_domain, batch), total=len(batch), desc=f"批次 {i//batch_size+1}"))
            for rule, ok in zip(batch, results):
                if ok:
                    valid.append(rule)
        print(f"✅ 已处理 {min(i+batch_size, total_rules)}/{total_rules} 条规则，有效累计: {len(valid)}")

    # 写入有效规则
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    print(f"\n🎯 验证完成: {input_file} → {output_file}")
    print(f"总规则: {total_rules}, 有效: {len(valid)}, 删除: {total_rules - len(valid)}")

# ===============================
# 示例调用
# ===============================
if __name__ == "__main__":
    validate_batch("rules.txt", "valid_rules.txt")
