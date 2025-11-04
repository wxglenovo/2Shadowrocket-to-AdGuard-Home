#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict

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
DELETE_THRESHOLD = 4  # 连续失败多少次后删除
SKIP_VALIDATE_THRESHOLD = 7  # 超过多少次失败跳过 DNS 验证（删除计数 >= 7）
SKIP_ROUNDS = 10  # 跳过验证的最大轮次，超过后恢复验证
DNS_BATCH_SIZE = 500  # 每批验证条数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# JSON 读写封装
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
# 下载源并合并
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

    recovered_rules = unified_skip_remove(merged)
    split_parts(recovered_rules)
    return True

# ===============================
# ✅ 统一剔除跳过验证模块（核心）
# ===============================
def unified_skip_remove(all_rules_set):
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    not_written_counter = load_json(NOT_WRITTEN_FILE)
    recovered_rules = []

    log_count = defaultdict(int)  # 记录每个日志出现次数

    for r in list(all_rules_set):
        del_cnt = delete_counter.get(r, 0)
        skip_cnt = skip_tracker.get(r, 0)

        # 只有删除计数 >= SKIP_VALIDATE_THRESHOLD 才跳过验证
        if del_cnt < SKIP_VALIDATE_THRESHOLD:
            continue

        # 累加跳过次数（从文件中读取后 +1）
        skip_cnt += 1
        skip_tracker[r] = skip_cnt

        # 删除计数继续 +1（历史累加）
        del_cnt += 1
        delete_counter[r] = del_cnt

        # 严格日志
        log_msg = f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={del_cnt}"
        if log_count[log_msg] < 20:  # 如果该日志没有超过20次，打印
            print(log_msg)
            log_count[log_msg] += 1
        elif log_count[log_msg] == 20:  # 打印次数达到20次时，显示数量
            print(f"⚠ 日志超出次数限制，显示数量：{log_msg}...")

        # 当跳过 >= SKIP_ROUNDS 时，**删除该规则并重置失败次数为 6**
        if skip_cnt >= SKIP_ROUNDS:
            print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（删除并重置连续失败次数=6）")
            # 删除规则记录
            skip_tracker.pop(r)
            delete_counter[r] = 6  # 重置失败次数
            recovered_rules.append(r)

    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written_counter)
    return recovered_rules

# ===============================
# 分片
# ===============================
def split_parts(recovered_rules=None):
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少主规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    # ✅ 恢复验证的规则放在最后一个分片
    if recovered_rules:
        for r in recovered_rules:
            if r in rules:
                rules.remove(r)
        rules.extend(recovered_rules)

    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part} 条规则")

    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ===============================
# DNS 验证函数
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

# ===============================
# 验证并打印完整日志
# ===============================
def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，每批 {DNS_BATCH_SIZE} 条规则")
    valid = []
    start_time = time.time()

    for i in range(0, len(lines), DNS_BATCH_SIZE):
        batch = lines[i:i + DNS_BATCH_SIZE]

        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = {executor.submit(check_domain, r): r for r in batch}

            completed = 0
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                if result:
                    valid.append(result)

                # ✅ 每 500 条打印一次
                if completed % 500 == 0 or completed == len(batch):
                    elapsed = time.time() - start_time
                    speed = (i + completed) / elapsed
                    eta = (len(lines) - (i + completed)) / speed if speed > 0 else 0
                    print(f"✅ 已验证 {i + completed}/{len(lines)} 条 | 有效 {len(valid)} 条 | 速度 {speed:.1f} 条/秒 | ETA {eta:.1f} 秒")

    print(f"✅ 分片验证完成，总有效 {len(valid)} 条")
    return valid

# ===============================
# ✅ 核心：处理分片 & 跳过验证逻辑
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
    skip_tracker = load_json(SKIP_FILE)
    not_written = load_json(NOT_WRITTEN_FILE)

    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    # 遍历当前分片规则
    for r in lines:
        del_cnt = delete_counter.get(r, 0)

        # delete_counter >= 7 → 跳过验证、直接剔除、不进入分片
        if del_cnt >= SKIP_VALIDATE_THRESHOLD:
            skip_cnt = skip_tracker.get(r, 0) + 1
            skip_tracker[r] = skip_cnt
            delete_counter[r] = del_cnt + 1

            print(f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={skip_cnt} | 删除计数={delete_counter[r]}")

            # 跳过累计 ≥10 → 恢复验证
            if skip_cnt >= SKIP_ROUNDS:
                print(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
                skip_tracker.pop(r)
                delete_counter[r] = 6
                rules_to_validate.append(r)
            continue  # 不写入分片

        # 未达到跳过阈值 → 正常进入 DNS 验证队列
        rules_to_validate.append(r)

    # 开始 DNS 验证
    valid = set(dns_validate(rules_to_validate))

    # 已验证的规则写入
    for rule in rules_to_validate:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            if rule in not_written:
                not_written.pop(rule)
            if rule not in old_rules:
                added_count += 1
        else:
            # 未通过验证 → 连续失败计数 +1
            old = delete_counter.get(rule, 0)
            new = old + 1
            delete_counter[rule] = new
            print(f"⚠ 连续失败 +1 → {new}/{DELETE_THRESHOLD} ：{rule}")

            # 达到删除阈值 → 删除
            if new >= DELETE_THRESHOLD:
                removed_count += 1
                print(f"🔥 连续失败达到阈值 → 删除规则：{rule}")
                if rule in not_written:
                    not_written.pop(rule)
                continue
            final_rules.add(rule)

    # 没写入 validated_part 的规则 → 记失败轮次
    for rule in list(final_rules):
        if rule not in valid and rule not in old_rules:
            cnt = not_written.get(rule, 0) + 1
            not_written[rule] = cnt
            if cnt >= 3:
                print(f"🔥 连续三次未写入 → 删除规则：{rule}")
                removed_count += 1
                final_rules.discard(rule)
                not_written.pop(rule)

    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(SKIP_FILE, skip_tracker)
    save_json(NOT_WRITTEN_FILE, not_written)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

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
