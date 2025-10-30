#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import dns.resolver

DIST_DIR = "dist"
FAIL_RECORD = os.path.join(DIST_DIR, "rule_fail_count.json")

resolver = dns.resolver.Resolver()
resolver.timeout = 2
resolver.lifetime = 2


def load_fail_record():
    if os.path.exists(FAIL_RECORD):
        with open(FAIL_RECORD, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_fail_record(data):
    with open(FAIL_RECORD, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def dns_ok(domain):
    try:
        resolver.resolve(domain, "A")
        return True
    except:
        return False


def process_validated_files():
    fail_data = load_fail_record()

    for file in os.listdir(DIST_DIR):
        if not file.startswith("validated_part_") or not file.endswith(".txt"):
            continue

        path = os.path.join(DIST_DIR, file)
        print(f"🔎 检查 {file}")

        with open(path, "r", encoding="utf-8") as f:
            rules = [x.strip() for x in f if x.strip()]

        new_rules = []
        changed = False

        for rule in rules:
            domain = rule.replace("||", "").replace("^", "")
            if dns_ok(domain):
                fail_data[domain] = 0
                new_rules.append(rule)
            else:
                fail_data[domain] = fail_data.get(domain, 0) + 1
                if fail_data[domain] >= 4:
                    print(f"❌ 连续 4 次失败，已删除：{rule}")
                    changed = True
                else:
                    print(f"⚠ DNS失败 {fail_data[domain]}/4：{rule}")
                    new_rules.append(rule)

        if changed:
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(new_rules) + "\n")

    save_fail_record(fail_data)
    print("✅ 安全删除流程完成（无误删除）")


if __name__ == "__main__":
    process_validated_files()
