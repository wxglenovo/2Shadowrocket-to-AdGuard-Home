#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import dns.resolver
import argparse

TMP_DIR = "tmp"
DIST_DIR = "dist"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4


def load_delete_counter():
    """加载全局连续失败计数"""
    if not os.path.exists(DELETE_COUNTER_FILE):
        return {}
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_delete_counter(counter):
    """保存全局连续失败计数，不清除其他分片的记录"""
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)


def validate_dns(rule):
    """DNS 验证"""
    try:
        domain = rule.split("/")[-1]
        dns.resolver.resolve(domain)
        return True
    except:
        return False


def process_part(part_number):
    part_file = os.path.join(TMP_DIR, f"part_{part_number:02}.txt")
    validated_file = os.path.join(DIST_DIR, f"validated_part_{part_number:02}.txt")

    if not os.path.exists(part_file):
        print(f"❌ {part_file} 不存在，跳过")
        return

    print(f"🔍 开始验证分片 {part_number:02}")

    with open(part_file, "r", encoding="utf-8") as f:
        rules = set(line.strip() for line in f if line.strip())

    delete_counter = load_delete_counter()                # ✅ 全局
    new_delete_counter = delete_counter.copy()            # ✅ 最关键：继承所有分片的历史，而不是覆盖

    valid_rules = []
    removed_rules = 0

    for rule in rules:

        if validate_dns(rule):
            valid_rules.append(rule)
            new_delete_counter[rule] = 0                  # ✅ 当前片成功验证 → 清零
        else:
            # 连续失败计数 +1
            old = delete_counter.get(rule, 0)
            new = old + 1
            new_delete_counter[rule] = new

            # 达到阈值 → 不收入有效列表
            if new < DELETE_THRESHOLD:
                valid_rules.append(rule)
            else:
                removed_rules += 1

    # ✅ 保存当前片结果
    with open(validated_file, "w", encoding="utf-8") as f:
        for r in sorted(valid_rules):
            f.write(r + "\n")

    # ✅ 保存所有规则计数(包含未参与本片的规则)
    save_delete_counter(new_delete_counter)

    print(f"✅ 分片 {part_number:02} 验证完成")
    print(f"✅ 保留 {len(valid_rules)}   ❌ 连续失败达到阈值并不再保留：{removed_rules}")

    # ✅ 给 GitHub Action 提取用
    print(f"COMMIT_STATS: 保留 {len(valid_rules)}, 移除 {removed_rules}")
