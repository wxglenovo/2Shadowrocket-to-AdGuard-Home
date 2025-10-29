#!/usr/bin/env python3
import concurrent.futures
import dns.resolver
import re

resolver = dns.resolver.Resolver()
resolver.timeout = 1
resolver.lifetime = 1

domain_regex = re.compile(
    r"^(?!-)([A-Za-z0-9-]{1,63}\.)+[A-Za-z]{2,}$"
)

def extract_domain(rule):
    rule = rule.strip().lstrip("|").lstrip("@@").split("^")[0]
    rule = rule.replace("*", "").replace("||", "")
    return rule

def is_valid_domain(rule):
    domain = extract_domain(rule)
    if not domain or not domain_regex.match(domain):
        return False
    try:
        resolver.resolve(domain, "A")
        return True
    except dns.resolver.NXDOMAIN:
        return False
    except:
        # SERVFAIL / REFUSED / TIMEOUT 都视为有效（被阻断域名也会这样）
        return True

def validate_batch(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as f:
        rules = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    valid = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        results = executor.map(is_valid_domain, rules)

    for rule, ok in zip(rules, results):
        if ok:
            valid.append(rule)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    print(f"✅ {input_file} → {output_file}")
    print(f"✅ 总: {len(rules)}, 有效: {len(valid)}, 删除: {len(rules) - len(valid)}")
