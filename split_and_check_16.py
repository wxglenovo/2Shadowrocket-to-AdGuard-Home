#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# -----------------------------
# 配置
# -----------------------------
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
VALIDATED_PREFIX = os.path.join(DIST_DIR, "validated_part_")
WORKERS = 50  # DNS 并发
PER_PART = 5000  # 每片数量可调


# ✅ 辅助函数：读取 delete_counter.json
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ✅ 写回 delete_counter.json
def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)


# ✅ 下载并合并所有规则（新增 HOSTS 转换逻辑）
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ 未找到 urls.txt，无法继续")
        return False

    print("📥 正在下载规则源...")
    merged = set()

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 正在获取：{url}")
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()

            for raw in r.text.splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                # ✅✅✅【本次新增逻辑】HOSTS → AdGuard 转换开始
                # 支持格式：
                #   0.0.0.0 domain.com
                #   127.0.0.1  xxx.net
                parts = line.split()
                if len(parts) == 2 and parts[0] in ("0.0.0.0", "127.0.0.1"):
                    domain = parts[1].strip()
                    if domain and "." in domain:
                        line = f"||{domain}^"
                # ✅✅✅【本次新增逻辑】HOSTS → AdGuard 转换结束

                merged.add(line)

        except Exception as e:
            print(f"⚠ 无法下载：{url}   原因：{e}")

    print(f"✅ 下载并合并完成，共 {len(merged)} 条规则")

    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    return True


# ✅ 切片
def split_to_parts():
    print("🔪 正在切片规则...")
    if not os.path.exists(MERGED_FILE):
        print("❌ merged_rules.txt 不存在，无法切片")
        return False

    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [r.strip() for r in f if r.strip()]

    os.makedirs(TMP_DIR, exist_ok=True)

    part = 1
    for i in range(0, len(rules), PER_PART):
        part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(rules[i:i + PER_PART]))
        print(f"✅ 已生成 {part_file}")
        part += 1

    return True


# ✅ DNS 验证单条
def check_domain(rule):
    # 只验证 AdGuard 域名类规则：||domain^
    if rule.startswith("||") and rule.endswith("^"):
        domain = rule[2:-1]
    else:
        return True, rule  # 保留非域名规则

    resolver = dns.resolver.Resolver()
    resolver.lifetime = 2
    resolver.timeout = 2
    try:
        resolver.resolve(domain)
        return True, rule
    except:
        return False, rule


# ✅ 验证某个分片（part）
def validate_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片 {part_file} 不存在")
        return

    print(f"🚀 开始验证分片 {part_file}")
    counter = load_delete_counter()

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [r.strip() for r in f if r.strip()]

    valid = []
    removed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(check_domain, r) for r in rules]
        for fu in tqdm(as_completed(futures), total=len(futures), desc=f"分片 {part} 验证中"):
            ok, rule = fu.result()

            if ok:
                valid.append(rule)
                if rule in counter:
                    counter[rule] = 0  # 成功一次清零
            else:
                # 连续失败计数 +1
                counter[rule] = counter.get(rule, 0) + 1
                if counter[rule] < 4:
                    valid.append(rule)
                else:
                    removed += 1  # 超过 3 次才真正删除

    # 写回验证后的结果文件
    out_file = f"{VALIDATED_PREFIX}{int(part):02d}.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(set(valid))))

    save_delete_counter(counter)

    print(f"✅ 分片 {part} 验证完成：共 {len(rules)} 条 → 保留 {len(valid)} 条 → 删除 {removed} 条")
    print(f"COMMIT_STATS: 分片{part} 共{len(rules)}条 保留{len(valid)}条 删除{removed}条")


# ✅ 主函数
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=str, help="指定验证分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载 + 切片")
    args = parser.parse_args()

    if args.force_update:
        print("🔄 强制刷新规则源...")
        download_all_sources()
        split_to_parts()
        print("✅ 强制刷新结束")
    else:
        if args.part:
            validate_part(args.part)
        else:
            print("⚠ 未提供分片参数，也未 --force-update，自动使用分片 01")
            validate_part("1")
