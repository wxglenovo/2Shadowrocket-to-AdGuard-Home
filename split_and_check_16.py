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
URLS_TXT = "urls.txt"  # urls.txt 存放所有规则源 URL
TMP_DIR = "tmp"  # 临时分片目录
DIST_DIR = "dist"  # 处理后输出目录
MASTER_RULE = "merged_rules.txt"  # 合并后的主规则文件
PARTS = 16  # 分片总数
DNS_WORKERS = 50  # DNS 并发验证线程数
DNS_TIMEOUT = 2  # DNS 查询超时时间
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # 连续失败计数文件路径
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")  # 跳过验证计数文件路径
NOT_WRITTEN_FILE = os.path.join(DIST_DIR, "not_written_counter.json")  # 连续未写入计数
DELETE_THRESHOLD = 4  # 规则连续失败多少次后删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过多少次失败跳过 DNS 验证
SKIP_ROUNDS = 10  # 跳过验证的最大轮次
DNS_BATCH_SIZE = 500  # 每批验证条数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 数据读取/保存工具函数
# ===============================
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ===============================
# 下载规则源并合并
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

    # 保存合并后的规则
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    # 下载完成后先统一剔除跳过验证
    recovered_rules = unified_skip_remove(merged)
    # 分片
    split_parts(recovered_rules)
    return True

# ===============================
# 统一剔除跳过验证函数
# ===============================
def unified_skip_remove(all_rules_set):
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    recovered_rules = []
    all_rules = list(all_rules_set)

    for r in all_rules:
        del_cnt = delete_counter.get(r, 0)
        skip_cnt = skip_tracker.get(r, 0)

        # 只有删除计数>=SKIP_VALIDATE_THRESHOLD才跳过验证
        if del_cnt < SKIP_VALIDATE_THRESHOLD:
            continue

        # 累加跳过次数
        skip_cnt += 1
        skip_tracker[r] = skip_cnt

        # 累加删除计数
        del_cnt += 1
        delete_counter[r] = del_cnt

        # 打印统一剔除日志
        print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={del_cnt}")

        # 超过 SKIP_ROUNDS 自动恢复验证
        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r)
            recovered_rules.append(r)

    # 保存 JSON
    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    return recovered_rules

# ===============================
# 分片模块
# recovered_rules 会放在最后一个分片
# ===============================
def split_parts(recovered_rules=None):
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    # 将恢复验证的规则放到最后一个分片
    if recovered_rules:
        for r in recovered_rules:
            if r in rules:
                rules.remove(r)
        rules.extend(recovered_rules)

    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

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
    except:
        return None

def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，每批 {DNS_BATCH_SIZE} 条规则")
    valid = []
    start_time = time.time()

    for i in range(0, len(lines), DNS_BATCH_SIZE):
        batch = lines[i:i + DNS_BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, rule): rule for rule in batch}
            done = 0
            for future in as_completed(futures):
                done += 1
                result = future.result()
                if result:
                    valid.append(result)

                # 每 500 条打印一次
                if done % 500 == 0 or done == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + done) / elapsed
                    eta = (len(lines) - (i + done)) / speed if speed > 0 else 0
                    print(f"✅ 已验证 {i + done}/{len(lines)} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")
    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ===============================
# 核心处理分片逻辑
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（不剔除注释）")

    # 增量更新 validated_part_*.txt
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    skip_tracker = load_json(SKIP_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)

    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    # 处理跳过逻辑
    for r in lines:
        c = delete_counter.get(r, 0)
        if c < SKIP_VALIDATE_THRESHOLD:
            rules_to_validate.append(r)
            continue

        # 超过阈值跳过验证
        skip_cnt = skip_tracker.get(r, 0) + 1
        skip_tracker[r] = skip_cnt
        delete_counter[r] = c + 1

        print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={delete_counter[r]}")

        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            delete_counter[r] = 6
            skip_tracker.pop(r)
            rules_to_validate.append(r)

    # DNS 验证
    valid = set(dns_validate(rules_to_validate))
    all_rules = final_rules | set(lines)

    for rule in all_rules:
        if rule in valid or rule in final_rules:
            final_rules.add(rule)
            delete_counter[rule] = 0
            if rule not in old_rules:
                added_count += 1
            # 清理未写入计数
            if rule in not_written_counter:
                not_written_counter.pop(rule)
            continue

        # 未通过验证
        old_count = delete_counter.get(rule, 0)
        new_count = old_count + 1
        delete_counter[rule] = new_count
        print(f"⚠ 连续失败 +1 → {new_count}/{DELETE_THRESHOLD} ：{rule}")
        if new_count >= DELETE_THRESHOLD:
            removed_count += 1
            if rule in not_written_counter:
                not_written_counter.pop(rule)
            continue
        final_rules.add(rule)

    # 更新未写入计数
    for r in all_rules:
        if r not in final_rules:
            not_written_counter[r] = not_written_counter.get(r, 0) + 1
            if not_written_counter[r] >= 3:
                removed_count += 1
                final_rules.discard(r)
                print(f"🔥 连续三次未写入 → 删除规则：{r}")
                not_written_counter.pop(r)

    # 保存 JSON
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(SKIP_FILE, skip_tracker)
    save_json(NOT_WRITTEN_FILE, not_written_counter)

    # 写入 validated_part
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主函数
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
