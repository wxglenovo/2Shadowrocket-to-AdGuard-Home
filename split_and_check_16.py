#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# ============================
#   AdGuard 多源规则构建脚本
#   - 下载合并
#   - HOSTS → AdGuard 转换 ✅新增
#   - 分片处理
#   - DNS 并发验证
#   - 连续失败计数与慢删策略
#   - 跳过验证机制
# ============================
#

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed


# ==============================================
# ① 基本配置常量
# ==============================================
URLS_TXT = "urls.txt"               # 规则源列表
TMP_DIR = "tmp"                     # 分片存放目录
DIST_DIR = "dist"                   # 验证后规则存放目录
MASTER_RULE = "merged_rules.txt"    # 合并主规则文件

PARTS = 16                          # 分片数量
DNS_WORKERS = 50                    # 并发验证线程数
DNS_TIMEOUT = 2                     # DNS 超时

DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # 连续失败计数器
SKIP_FILE = os.path.join(DIST_DIR, "skip_tracker.json")              # 跳过验证计数器

DELETE_THRESHOLD = 4                # 连续失败 4 次 → 删除
SKIP_VALIDATE_THRESHOLD = 7         # 失败超 7 次 → 暂停验证
SKIP_ROUNDS = 10                    # 暂停验证 10 轮后恢复

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)



# ==============================================
# ② 跳过验证计数器（避免每次都检测慢规则）
# ==============================================
def load_skip_tracker():
    if os.path.exists(SKIP_FILE):
        try:
            return json.load(open(SKIP_FILE, "r", encoding="utf-8"))
        except:
            return {}
    return {}

def save_skip_tracker(data):
    json.dump(data, open(SKIP_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)



# ==============================================
# ③ ✅ 下载 + 合并规则源
#    ✅ 新增功能：HOSTS → AdGuard 转换
# ==============================================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False

    print("📥 下载规则源...")
    merged = set()
    urls = [u.strip() for u in open(URLS_TXT, "r", encoding="utf-8") if u.strip()]

    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()

            for raw in r.text.splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                # ✅【新增】HOSTS → AdGuard Home 转换
                #   0.0.0.0 a.com   → ||a.com^
                #   127.0.0.1 b.com → ||b.com^
                parts = line.split()
                if len(parts) == 2 and parts[0] in ("0.0.0.0", "127.0.0.1"):
                    domain = parts[1].strip()
                    if domain and "." in domain:      # 避免 localhost / broadcasthost
                        line = f"||{domain}^"

                merged.add(line)

        except Exception as e:
            print(f"⚠ 下载失败 {url}，原因：{e}")

    print(f"✅ 已收集合并 {len(merged)} 条规则")

    # 写入主规则文件
    open(MASTER_RULE, "w", encoding="utf-8").write(
        "\n".join(sorted(merged))
    )
    return True



# ==============================================
# ④ 将 merged_rules.txt 切成指定数量分片
# ==============================================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并结果，无法分片")
        return False

    rules = [l.strip() for l in open(MASTER_RULE, "r", encoding="utf-8") if l.strip()]
    total = len(rules)
    per = (total + PARTS - 1) // PARTS
    print(f"🪓 切分规则：共 {total} 条，每片约 {per}")

    for i in range(PARTS):
        chunk = rules[i * per:(i + 1) * per]
        path = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        open(path, "w", encoding="utf-8").write("\n".join(chunk))
        print(f"📄 分片 {i+1}: {len(chunk)} 条 → {path}")

    return True



# ==============================================
# ⑤ DNS 并发验证
# ==============================================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT

    # 截取域名部分：||domain.com^ → domain.com
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None

    try:
        resolver.resolve(domain)
        return rule
    except:
        return None


def dns_validate(lines):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证")
    valid = []

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in lines}
        total = len(lines)
        done = 0

        for future in as_completed(futures):
            done += 1
            res = future.result()
            if res:
                valid.append(res)

            # 只显示进度，不打印每条记录
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total}，有效 {len(valid)}")

    print(f"✅ 完成验证，有效 {len(valid)} 条")
    return valid



# ==============================================
# ⑥ 连续失败计数器
#    失败次数越多 → 越偏向删除
# ==============================================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            return json.load(open(DELETE_COUNTER_FILE, "r", encoding="utf-8"))
        except:
            print("⚠ delete_counter.json 损坏，重建空计数")
    return {}

def save_delete_counter(data):
    json.dump(data, open(DELETE_COUNTER_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)



# ==============================================
# ⑦ 核心功能：处理一个分片
#    - 跳过验证逻辑
#    - DNS 验证
#    - 失败计数 + 慢删
# ==============================================
def process_part(part):
    path = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(path):
        print(f"⚠ 找不到分片 {part}，重新下载并切片")
        download_all_sources()
        split_parts()

    if not os.path.exists(path):
        print("❌ 分片仍不存在，终止")
        return

    rules = open(path, "r", encoding="utf-8").read().splitlines()
    out_path = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    print(f"⏱ 开始验证分片 {part}，总 {len(rules)} 条")

    old = set(open(out_path, "r", encoding="utf-8").read().splitlines()) if os.path.exists(out_path) else set()
    delete_counter = load_delete_counter()
    skip_tracker = load_skip_tracker()

    # -------- 筛选需要验证 & 跳过验证的规则 --------
    need_validate = []
    for r in rules:
        c = delete_counter.get(r)

        # 新规则：先验证
        if c is None:
            need_validate.append(r)
            continue

        # 失败未超过阈值：继续验证
        if c <= SKIP_VALIDATE_THRESHOLD:
            need_validate.append(r)
            continue

        # 超过阈值：跳过本轮验证
        skip_tracker[r] = skip_tracker.get(r, 0) + 1
        print(f"⏩ 跳过验证 {r}（跳过 {skip_tracker[r]}/10）")

        # 跳满 10 轮 → 恢复验证
        if skip_tracker[r] >= SKIP_ROUNDS:
            print(f"🔁 恢复验证 {r}（重置计数=4）")
            delete_counter[r] = 4
            skip_tracker.pop(r)
            need_validate.append(r)

    # -------- DNS 并发验证 --------
    valid = set(dns_validate(need_validate))

    # -------- 处理新增、保留、删除逻辑 --------
    final = set()
    added = removed = 0
    all_rules = old | set(rules)
    new_counter = delete_counter.copy()

    for r in all_rules:
        # 验证通过 → 必保留
        if r in valid:
            final.add(r)
            new_counter[r] = 0
            if r not in old:
                added += 1
            continue

        # 未通过：计数 +1（新规则从 4 起步）
        old_c = delete_counter.get(r)
        new_c = 4 if old_c is None else (old_c + 1)
        new_counter[r] = new_c

        print(f"⚠ 连续失败 {new_c}：{r}")

        # 达阈值 → 删除
        if new_c >= DELETE_THRESHOLD:
            removed += 1
            continue

        final.add(r)

    # 保存计数与结果
    save_delete_counter(new_counter)
    save_skip_tracker(skip_tracker)
    open(out_path, "w", encoding="utf-8").write("\n".join(sorted(final)))

    print(f"✅ 分片 {part} 完成：总 {len(final)}，新增 {added}，删除 {removed}")
    print(f"COMMIT_STATS: 总 {len(final)}, 新增 {added}, 删除 {removed}")



# ==============================================
# ⑧ 主入口：支持
#    --force-update   重新下载合并+切片
#    --part X         验证指定分片
# ==============================================
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--part", help="验证指定分片 1~16")
    p.add_argument("--force-update", action="store_true", help="强制下载+切片")
    args = p.parse_args()

    # 处理 force-update
    if args.force_update:
        download_all_sources()
        split_parts()

    # 如果首次运行，没有文件 → 自动下载切片
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺文件，自动拉取规则源并分片")
        download_all_sources()
        split_parts()

    # 仅验证指定分片
    if args.part:
        process_part(args.part)
