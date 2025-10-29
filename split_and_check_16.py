#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import dns.resolver

URLS_TXT = "urls.txt"  # 这里存放的是规则源地址，而不是规则本身
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"  # 下载与合并后的规则
PARTS = 16
DNS_BATCH_SIZE = 800

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


def download_all_sources():
    """从 urls.txt 下载所有远程规则文件，并合并去重"""
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在，无法获取规则源")
        return False

    print("📥 开始下载所有规则源...")
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
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 下载完成，共合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    return True


def split_parts():
    """分割 merged_rules.txt"""
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件，无法切片")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 正在分片 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i + 1}: {len(part_rules)} 条 → {filename}")

    return True


def dns_validate(lines):
    valid = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2

    for idx, rule in enumerate(lines, 1):
        domain = rule.lstrip("|").split("^")[0].replace("*", "")
        if not domain:
            continue

        try:
            resolver.resolve(domain)
            valid.append(rule)
        except:
            pass

        if idx % DNS_BATCH_SIZE == 0:
            print(f"✅ 已验证 {idx}/{len(lines)} 条，有效 {len(valid)} 条")

    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid


def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动重新下载并切片")
        download_all_sources()
        split_parts()

    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")

    valid = dns_validate(lines)
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    print(f"✅ 分片 {part} 验证完成 → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    args = parser.parse_args()

    # 强制刷新
    if args.force_update:
        download_all_sources()
        split_parts()

    # 若缺失规则文件或分片则自动补
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
