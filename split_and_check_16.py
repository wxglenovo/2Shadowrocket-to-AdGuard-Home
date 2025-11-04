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

    recovered_rules = unified_skip_remove_fast(merged)
    split_parts(recovered_rules)
    return True

# ===============================
# 高性能统一剔除跳过验证模块（核心）
# ===============================
def unified_skip_remove_fast(all_rules_list):
    """
    高性能统一剔除函数：
    - 只处理 delete_counter 中已经 >= SKIP_VALIDATE_THRESHOLD 的规则与 all_rules_list 的交集
    - 批量收集日志，最终一次性写回
    - 返回 recovered_rules（需要恢复验证并放到最后分片的规则）
    """
    # 读取计数器一次
    skip_tracker = load_json(SKIP_FILE)
    delete_counter = load_json(DELETE_COUNTER_FILE)
    not_written = load_json(NOT_WRITTEN_FILE)

    # 把 all_rules_list 转成集合供快速查找
    rules_set = set(all_rules_list)

    # 候选：只有 delete_counter 中的键且在 rules_set 中，避免遍历所有规则
    candidate_keys = [k for k, v in delete_counter.items() if v >= SKIP_VALIDATE_THRESHOLD and k in rules_set]
    if not candidate_keys:
        # 无候选，确保文件写回（以防文件不存在）
        save_json(SKIP_FILE, skip_tracker)
        save_json(DELETE_COUNTER_FILE, delete_counter)
        save_json(NOT_WRITTEN_FILE, not_written)
        return []

    recovered_rules = []
    logs = []  # 日志缓冲，最后批量打印或写入
    log_count = {}  # 用于限制日志输出数量

    # 遍历候选而不是全部规则
    for r in candidate_keys:
        # 安全读取当前值（避免 race）
        cur_del = delete_counter.get(r, 0)
        cur_skip = skip_tracker.get(r, 0)

        # 累加跳过次数并写回内存 dict
        cur_skip += 1
        skip_tracker[r] = cur_skip

        # 累加 delete_counter
        cur_del += 1
        delete_counter[r] = cur_del

        # 缓存日志（严格格式）
        log_message = f"⚠ 统一剔除（跳过验证）：{r} | 跳过次数={cur_skip} | 删除计数={cur_del}"
        
        # 控制相同日志内容的输出次数（最多显示 20 次）
        if log_message not in log_count:
            log_count[log_message] = 1
        elif log_count[log_message] < 20:
            log_count[log_message] += 1

        # 只打印前 20 次出现的相同日志
        if log_count[log_message] <= 20:
            logs.append(log_message)

        # 如果达到恢复阈值
        if cur_skip >= SKIP_ROUNDS:
            logs.append(f"🔁 跳过次数达到 {SKIP_ROUNDS} 次 → 恢复验证：{r}（重置连续失败次数=6）")
            # 清除 skip 计数
            skip_tracker.pop(r, None)
            # set delete_counter to 6
            delete_counter[r] = 6
            recovered_rules.append(r)

    # 批量打印日志（一次性写入控制台，减少 IO 阻塞）
    # 如果日志行数非常多，你也可以改为写入文件：with open('dist/skip_log.txt','a') as lf: lf.write("\n".join(logs)+"\n")
    print("\n".join(logs))

    # 批量写回 JSON（只写一次）
    save_json(SKIP_FILE, skip_tracker)
    save_json(DELETE_COUNTER_FILE, delete_counter)
    save_json(NOT_WRITTEN_FILE, not_written)

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

    # 恢复验证的规则放在最后一个分片
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
        batch
