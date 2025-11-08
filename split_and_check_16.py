#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

DIST_DIR = "dist"
TMP_DIR = "tmp"
VALIDATED_PREFIX = os.path.join(DIST_DIR, "validated_part_")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")

os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

def load_not_written():
    if os.path.exists(NOT_WRITTEN_FILE):
        with open(NOT_WRITTEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {f"validated_part_{i}": {} for i in range(1, 17)}

def save_not_written(data):
    with open(NOT_WRITTEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def dns_query(domain):
    try:
        dns.resolver.resolve(domain, "A")
        return True
    except:
        return False

def dns_validate(rules, part_num):
    valid = []
    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = {ex.submit(dns_query, r): r for r in rules}
        for fu in as_completed(futures):
            if fu.result():
                valid.append(futures[fu])
    return valid

def update_not_written_counter(part_num):
    """
    ✅ 更新 NOT_WRITTEN_FILE 中 validated_part_X 区
    ✅ 更新 validated_part_X.txt
    ✅ 打印删除日志（前20条）

    规则：
      - tmp/vpart_X.tmp 中的规则 → write_counter = 4
      - 原 validated_part_X.txt 中有，但 tmp 中不存在：
          - 若 JSON 已有 → write_counter -= 1
          - 否则 → write_counter = 3
      - write_counter <= 0:
          - 从 validated_part_X.txt 中删除（打印前20条）
          - 从 JSON 删除
    返回: 删除数量
    """

    json_data = load_not_written()
    key = f"validated_part_{part_num}"

    tmp_file = os.path.join(TMP_DIR, f"vpart_{part_num}.tmp")
    validated_file = f"{VALIDATED_PREFIX}{part_num}.txt"

    if not os.path.exists(validated_file):
        with open(validated_file, "w", encoding="utf-8") as f:
            pass

    old_rules = []
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            old_rules = [l.strip() for l in f if l.strip()]

    tmp_rules = []
    if os.path.exists(tmp_file):
        with open(tmp_file, "r", encoding="utf-8") as f:
            tmp_rules = [l.strip() for l in f if l.strip()]

    # ================== ✅ Step 1: tmp出现的 → write_counter = 4
    for rule in tmp_rules:
        json_data[key][rule] = 4

    # ================== ✅ Step 2: 原 validated 有但 tmp 没有
    deleted_list = []
    for rule in old_rules:
        if rule not in tmp_rules:
            if rule in json_data[key]:
                json_data[key][rule] -= 1
            else:
                json_data[key][rule] = 3

            # write_counter <= 0 → 删除
            if json_data[key][rule] <= 0:
                deleted_list.append(rule)

    # ================= ✅ 删除逻辑：validated文件中移除 + JSON移除
    if deleted_list:
        # 显示前20条
        for r in deleted_list[:20]:
            print(f"💥 write_counter ≤ 3 → 从 JSON 删除：{r}")

        print(f"🗑 本次从 JSON 删除 共 {len(deleted_list)} 条规则")

    # 过滤 validated 中保留的规则
    new_validated_rules = [r for r in old_rules if r not in deleted_list]

    # 覆盖写回 validated_part_X.txt ✅
    with open(validated_file, "w", encoding="utf-8") as f:
        for r in tmp_rules:  # tmp中的规则 一定保留
            f.write(r + "\n")
        for r in new_validated_rules:
            if r not in tmp_rules:
                f.write(r + "\n")

    # ✅ JSON 删除对应项、写回
    for r in deleted_list:
        if r in json_data[key]:
            del json_data[key][r]

    save_not_written(json_data)

    return len(deleted_list)


def process_part(part):
    part = int(part)
    tmp_file = os.path.join(TMP_DIR, f"part_{part}.txt")
    vtmp_file = os.path.join(TMP_DIR, f"vpart_{part}.tmp")
    validated_file = f"{VALIDATED_PREFIX}{part}.txt"

    if not os.path.exists(tmp_file):
        print(f"❌ 缺少 tmp/part_{part}.txt")
        return

    with open(tmp_file, "r", encoding="utf-8") as f:
        rules_to_validate = [l.strip() for l in f if l.strip()]

    total_to_test = len(rules_to_validate)

    valid_rules = dns_validate(rules_to_validate, part)
    filtered_count = total_to_test - len(valid_rules)

    # ✅ 写入 vtmp（验证成功）
    with open(vtmp_file, "w", encoding="utf-8") as f:
        for r in valid_rules:
            f.write(r + "\n")

    # ✅ 更新 JSON 和 validated文件
    deleted_count = update_not_written_counter(part)

    # ✅ 统计
    if os.path.exists(validated_file):
        with open(validated_file, "r", encoding="utf-8") as f:
            final_count = len([l.strip() for l in f if l.strip()])
    else:
        final_count = 0

    added_count = len(valid_rules)

    print(f"✅ 分片 {part} 完成: 总{final_count}, 新增{added_count}, 删除{deleted_count}, 过滤{filtered_count}")
    print(f"COMMIT_STATS:总{final_count},新增{added_count},删除{deleted_count},过滤{filtered_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("part", help="分片 1~16")
    args = parser.parse_args()
    process_part(args.part)
