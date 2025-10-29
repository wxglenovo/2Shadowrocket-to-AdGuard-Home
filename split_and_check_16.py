#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import dns.resolver

URL_SOURCE = "https://raw.githubusercontent.com/wxglenovo/Shadowrocket-to-AdGuard-Home/main/urls.txt"
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16
DNS_BATCH_SIZE = 800

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


def download_urls():
    print("📥 下载最新 urls.txt ...")
    try:
        r = requests.get(URL_SOURCE, timeout=15)
        r.raise_for_status()
        with open(URLS_TXT, "w", encoding="utf-8") as f:
            f.write(r.text)
        print(f"✅ urls.txt 下载完成，共 {len(r.text.splitlines())} 条")
    except Exception as e:
        print(f"❌ 下载失败: {e}")


def split_parts():
    if not os.path.exists(URLS_TXT):
        print("⚠ urls.txt 不存在，无法切片")
        return

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    total = len(lines)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🔧 正在生成分片，共 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part = lines[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part))
        print(f"📄 分片 {i + 1} 已保存 {len(part)} 条 → {filename}")


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
        except Exception:
            pass

        if idx % DNS_BATCH_SIZE == 0:
            print(f"✅ 已验证 {idx}/{len(lines)} 条，有效 {len(valid)} 条")

    print(f"✅ 完成验证，共有效 {len(valid)} 条")
    return valid


def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片文件不存在: {part_file}")
        print("🔄 自动重新下载 urls.txt 并重新生成所有分片")
        download_urls()
        split_parts()

    if not os.path.exists(part_file):
        print("❌ 无法生成分片，退出")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")

    valid = dns_validate(lines)
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    print(f"✅ 分片 {part} 验证完成，有效 {len(valid)} 条 → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制更新 urls.txt 并重新切片")
    args = parser.parse_args()

    # 强制更新
    if args.force_update:
        download_urls()
        split_parts()

    # 非强制更新，但 urls.txt 或分片缺失时自动处理
    if not os.path.exists(URLS_TXT) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少 urls.txt 或分片，自动刷新")
        download_urls()
        split_parts()

    # 如果指定分片，执行验证
    if args.part:
        process_part(args.part)
