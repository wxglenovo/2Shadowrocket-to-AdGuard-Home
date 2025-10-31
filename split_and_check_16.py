#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import dns.resolver
import time

DNS_TIMEOUT = 2
DNS_RETRY = 1
DNS_WORKERS = 50

def check_rule(rule):
    """
    返回 True = 可解析（有效）
    返回 False = 无法解析（无效）
    """
    try:
        domain = rule.replace("^", "").replace("||", "").strip()
        resolver = dns.resolver.Resolver()
        resolver.lifetime = DNS_TIMEOUT
        resolver.timeout = DNS_TIMEOUT
        resolver.nameservers = ["8.8.8.8", "1.1.1.1"]

        for _ in range(DNS_RETRY+1):
            try:
                resolver.resolve(domain)
                return True
            except:
                time.sleep(0.2)

        return False
    except:
        return False


def load_validated(path):
    """
    加载 validated_part_XX.txt
    每行格式：    rule\tcount
    若旧格式没有计数，默认记为 count=0
    """
    rules = {}
    if not os.path.exists(path):
        return rules

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if "\t" in line:
                r, cnt = line.split("\t", 1)
                rules[r] = int(cnt)
            else:
                rules[line] = 0
    return rules


def save_validated(path, rules_dict):
    with open(path, "w", encoding="utf-8") as f:
        for r, c in rules_dict.items():
            f.write(f"{r}\t{c}\n")


def validate_part(part_file, validated_file, log_file):
    # 装载旧验证数据（带连续删除计数）
    validated_map = load_validated(validated_file)

    # 当前分片规则
    with open(part_file, "r", encoding="utf-8") as f:
        part_rules = [x.strip() for x in f if x.strip()]

    new_validated = {}
    added = 0
    removed = 0
    remained = 0

    with open(log_file, "a", encoding="utf-8") as log:
        log.write(f"📌 开始验证: {part_file}\n")
        log.flush()

        for rule in part_rules:
            ok = check_rule(rule)

            if ok:     # ✅ 解析成功
                if rule not in validated_map:
                    added += 1
                    log.write(f"✅ 新增有效: {rule}\n")
                else:
                    remained += 1
                new_validated[rule] = 0  # 成功 → 重置计数

            else:       # ❌ 解析失败
                old_cnt = validated_map.get(rule, 0)
                new_cnt = old_cnt + 1

                log.write(f"⚠ 连续删除计数 {new_cnt}/4: {rule}\n")

                if new_cnt >= 4:
                    log.write(f"❌ 已连续失败 4 次 -> 删除: {rule}\n")
                    removed += 1
                else:
                    # 仍保留，等待下次验证
                    new_validated[rule] = new_cnt
                    remained += 1

            log.flush()

    # ✅ 保存更新结果（非常关键！）
    save_validated(validated_file, new_validated)

    return added, removed, remained


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: python validate.py <part_X.txt> <validated_part_X.txt> <log_file>")
        sys.exit(1)

    part_file = sys.argv[1]
    validated_file = sys.argv[2]
    log_file = sys.argv[3]

    added, removed, remained = validate_part(part_file, validated_file, log_file)

    # 输出给 GitHub Actions 用
    print(f"COMMIT_STATS: 总 {added + removed + remained}, 新增 {added}, 删除 {removed}, 保留 {remained}")
