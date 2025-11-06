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
WRITE_COUNTER_MAX = 3

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
# 分片
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
# DNS 验证
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except Exception as e:
        return None

def dns_validate(rules):
    valid_rules = []
    total_rules = len(rules)
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in rules}
        completed = 0
        start_time = time.time()
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid_rules.append(result)
            completed += 1
            if completed % DNS_BATCH_SIZE == 0 or completed == total_rules:
                elapsed = time.time() - start_time
                speed = completed / elapsed
                eta = (total_rules - completed)/speed if speed > 0 else 0
                print(f"✅ 已验证 {completed}/{total_rules} 条 | 有效 {len(valid_rules)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")
    return valid_rules

# ===============================
# 更新 not_written_counter.json
# ===============================
def update_not_written_counter(part, final_rules):
    print(f"开始更新 not_written_counter.json，处理分片 {part} 中的 {len(final_rules)} 条规则")
    counter = load_json(NOT_WRITTEN_FILE)

    deleted_rules_count = 0  # 用于记录删除规则数量
    deleted_rules = []  # 存储被删除的规则（write_counter 为 0 的规则）

    # 重置当前分片规则 write_counter = 6
    for rule in final_rules:
        counter[rule] = {"write_counter": WRITE_COUNTER_MAX, "part": f"validated_part_{part}"}

    # 对其他规则未出现的，write_counter-1
    for rule, info in list(counter.items()):
        if "part" not in info:
            continue  # 跳过没有 'part' 键的规则

        if info["part"] == f"validated_part_{part}" and rule not in final_rules:
            counter[rule]["write_counter"] -= 1
            if counter[rule]["write_counter"] <= 0:
                print(f"🔥 write_counter 为0，删除 {rule} 于 {info['part']}")
                counter.pop(rule)
                deleted_rules.append(rule)  # 记录被删除的规则

    # 输出准备保存更新后的数据的前20项
    print(f"⚠ 准备保存更新后的数据的前20项：")
    for i, (rule, info) in enumerate(list(counter.items())[:20]):
        print(f"🔥 {rule}: {info}")

    # 输出总规则数量
    print(f"🔢 共 {len(counter)} 条规则数据已更新")

    # 调试输出
    print(f"准备保存更新后的数据：{counter}")
    save_json(NOT_WRITTEN_FILE, counter)

    return len(deleted_rules)  # 返回被删除的规则数量

# ===============================
# 处理分片
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 文件 {part_file} 不存在")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        part_rules = [line.strip() for line in f if line.strip()]
    
    valid_rules = dns_validate(part_rules)
    
    # 更新 not_written_counter.json
    deleted_count = update_not_written_counter(part, valid_rules)
    print(f"✅ 已删除 {deleted_count} 条规则")

# ===============================
# 主程序
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="处理分片规则")
    parser.add_argument("part", type=int, help="需要处理的分片编号")
    args = parser.parse_args()

    download_all_sources()  # 下载并合并规则
    process_part(args.part)  # 处理指定分片
