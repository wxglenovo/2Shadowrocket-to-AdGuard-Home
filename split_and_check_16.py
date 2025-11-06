#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ===============================
# 配置区（Config）
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")
DELETE_THRESHOLD = 4
DNS_BATCH_SIZE = 500
WRITE_COUNTER_MAX = 6

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠ 读取 {path} 时发生错误: {e}")
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ 已保存 {path}")
    except Exception as e:
        print(f"⚠ 保存 {path} 时发生错误: {e}")

# ===============================
# DNS 验证
# ===============================
def dns_validate(rules_to_validate):
    valid_rules = set()

    def check_dns(rule):
        try:
            # 执行 DNS 查询，检查是否存在该域名
            dns.resolver.resolve(rule, "A", lifetime=DNS_TIMEOUT)
            valid_rules.add(rule)
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            pass  # 规则无效
        except Exception as e:
            print(f"⚠ DNS 查询失败 {rule}: {e}")

    # 使用多线程并行验证规则
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = [executor.submit(check_dns, rule) for rule in rules_to_validate]
        for future in as_completed(futures):
            future.result()  # 获取结果，保证异常被捕获

    return valid_rules

# ===============================
# 下载并合并规则源
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False

    print("📥 下载规则源...")
    merged = set()

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line:
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    filtered_rules, updated_delete_counter = filter_and_update_high_delete_count_rules(merged)
    split_parts(filtered_rules)
    save_json(DELETE_COUNTER_FILE, updated_delete_counter)
    return True

# ===============================
# 分片处理
# ===============================
def split_parts(merged_rules):
    total = len(merged_rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条规则")

    for i in range(PARTS):
        part_rules = list(merged_rules)[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

# ===============================
# 处理删除计数 >=7 的规则
# ===============================
def filter_and_update_high_delete_count_rules(all_rules_set):
    delete_counter = load_json(DELETE_COUNTER_FILE)
    low_delete_count_rules = set()
    updated_delete_counter = delete_counter.copy()

    reset_count = 0  # 记录重置的规则数量
    reset_limit = 20  # 限制只显示前20条重置的规则
    skipped_count = 0  # 记录跳过的规则数量
    skipped_rules = []  # 存储跳过的规则
    reset_rules = []  # 存储重置规则的日志

    for rule in all_rules_set:
        del_cnt = delete_counter.get(rule, 4)
        if del_cnt < 7:
            low_delete_count_rules.add(rule)
        else:
            updated_delete_counter[rule] = del_cnt + 1
            if updated_delete_counter[rule] >= 17:
                updated_delete_counter[rule] = 5
                reset_count += 1  # 重置计数器加1
                reset_rules.append(rule)  # 将重置规则添加到日志中

            # 对于删除计数达到7或以上的规则进行跳过
            if del_cnt >= 7:
                skipped_count += 1
                skipped_rules.append(rule)

    # 先输出跳过规则日志（只显示前20条）
    for i, rule in enumerate(skipped_rules[:20]):
        print(f"⚠ 删除计数达到 7 或以上，跳过规则：{rule} | 删除计数={delete_counter.get(rule)}")

    # 输出跳过规则总数
    print(f"🔢 共 {skipped_count} 条规则删除计数达到 7 或以上被跳过验证")

    # 输出重置规则日志（只显示前20条）
    for i, rule in enumerate(reset_rules[:20]):
        print(f"🔁 删除计数达到 17，重置规则：{rule} 的删除计数为 5")

    # 输出重置规则总数
    print(f"🔢 共 {reset_count} 条规则删除计数达到 17的删除计数被重置为 5")

    return low_delete_count_rules, updated_delete_counter

# ===============================
# 更新 not_written_counter.json
# ===============================
def update_not_written_counter(part, final_rules, old_rules):
    current_part_prefix = f"validated_part_{part}"
    print(f"开始更新 not_written_counter.json，处理分片 {part} 中的 {len(final_rules)} 条规则")
    
    # 加载当前的数据
    counter = load_json(NOT_WRITTEN_FILE)

    # 记录新增的规则数量
    new_rules_count = 0
    deleted_rules_count = 0
    deleted_rules = []  # 存储被删除的规则（write_counter 为 0 的规则）

    # 1. 处理已存在的规则（可能已经被验证过）
    for rule in final_rules:
        # 如果规则不存在于 old_rules 中，表示它是新增的规则
        if rule not in old_rules:
            counter[rule] = {"write_counter": 6, "part": current_part_prefix}
            new_rules_count += 1
        else:
            # 如果规则已在 old_rules 中，保持其原有的 part 信息并检查 write_counter
            if counter.get(rule, {}).get('part') == current_part_prefix:
                counter[rule]["write_counter"] = 6
                counter[rule]["part"] = current_part_prefix

    # 2. 处理没有出现在当前分片中的规则，write_counter - 1
    for rule, info in list(counter.items()):
        if "part" not in info:
            continue  # 跳过没有 'part' 键的规则

        if info["part"] == current_part_prefix and rule not in final_rules:
            counter[rule]["write_counter"] -= 1

            # 如果 write_counter <= 3，从当前分片中删除
            if counter[rule]["write_counter"] <= 3:
                print(f"🔥 write_counter <= 3，删除 {rule} 于分片 {info['part']}")
                counter.pop(rule)
                deleted_rules.append(rule)

            # 如果 write_counter <= 0，从 not_written_counter.json 中删除
            if counter.get(rule, {}).get("write_counter", 0) <= 0:
                print(f"🔥 write_counter <= 0，删除 {rule} 于 not_written_counter.json")
                counter.pop(rule)
                deleted_rules.append(rule)

    save_json(NOT_WRITTEN_FILE, counter)

    # 输出相关日志
    print(f"🔥 写入规则 {{'write_counter': 6, 'part': '{current_part_prefix}'}} 数量: {new_rules_count}")
    print(f"🔥 规则 write_counter 为 0，删除该规则于 not_written_counter.json 数量: {len(deleted_rules)}")

    return len(deleted_rules)

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，拉取规则中…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（不剔除注释）")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0
    deleted_rules_count = 0  # 新增删除的规则计数

    for r in lines:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = del_cnt + 1
            print(f"⚠ 删除计数达到 7 或以上，跳过规则：{r} | 删除计数={del_cnt}")

    valid = dns_validate(rules_to_validate)

    failure_counts = {}

    for rule in rules_to_validate:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            added_count += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            current_failure_count = delete_counter[rule]
            failure_counts[current_failure_count] = failure_counts.get(current_failure_count, 0) + 1
            if delete_counter[rule] >= DELETE_THRESHOLD:
                removed_count += 1
                final_rules.discard(rule)
                deleted_rules_count += 1  # 增加删除规则的数量

    save_json(DELETE_COUNTER_FILE, delete_counter)

    for i in range(1, max(failure_counts.keys()) + 1):
        if failure_counts[i] > 0:
            print(f"⚠ 连续失败 {i}/4 的规则条数: {failure_counts[i]} 条")

    # 将有效规则写入对应的分片文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    # 更新 `not_written_counter.json` 文件，并返回删除规则数量
    deleted_count = update_not_written_counter(part, final_rules, old_rules)

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}, 删除规则数量: {deleted_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}, 删除规则数量: {deleted_count}")

# ===============================
# 主入口
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()

    if args.part:
        process_part(args.part)
