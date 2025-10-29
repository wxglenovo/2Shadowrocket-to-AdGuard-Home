#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import asyncio
import dns.asyncresolver

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
MAX_CONCURRENCY = 500   # 并发数（越大越快，可调 300~1000）

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)


# =====================================================
# ✅ 下载并合并规则
# =====================================================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在，无法下载规则")
        return False

    print("📥 开始下载并合并所有规则源...")
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    for url in urls:
        print(f"🌐 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")

    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))

    return True


# =====================================================
# ✅ 分成 PARTS 片
# =====================================================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("❌ 缺少 merged_rules.txt，无法分片")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    per = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片 ~{per}")

    for i in range(PARTS):
        sub = rules[i * per:(i + 1) * per]
        fname = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\n".join(sub))
        print(f"📄 part_{i + 1:02d}: {len(sub)} 条")

    return True


# =====================================================
# ✅ 异步 DNS 批量验证
# =====================================================
async def check_domain(resolver, rule):
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        await resolver.resolve(domain)
        return rule
    except:
        return None


async def dns_validate_async(lines):
    resolver = dns.asyncresolver.Resolver()
    resolver.nameservers = ["8.8.8.8", "1.1.1.1"]  # 加速
    resolver.timeout = 2
    resolver.lifetime = 2

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    valid = []

    async def worker(rule):
        async with sem:
            r = await check_domain(resolver, rule)
            if r:
                valid.append(r)

    tasks = [worker(rule) for rule in lines]
    print(f"🚀 开始异步验证 {len(lines)} 条...")
    await asyncio.gather(*tasks)

    print(f"✅ 有效规则: {len(valid)}")
    return valid


def dns_validate(lines):
    return asyncio.run(dns_validate_async(lines))


# =====================================================
# ✅ 处理单个分片
# =====================================================
def process_part(part):
    part = int(part)
    part_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    out_file = os.path.join(DIST_DIR, f"validated_part_{part:02d}.txt")

    if os.path.exists(out_file):
        print(f"⏩ 分片 {part} 已验证，跳过 → {out_file}")
        return

    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载切片")
        download_all_sources()
        split_parts()

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part} 共 {len(lines)} 条")

    valid = dns_validate(lines)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))

    print(f"✅ 分片 {part} 完成 → {out_file}")


# =====================================================
# ✅ 合并所有最终结果
# =====================================================
def merge_validated_results():
    print("📦 合并所有已验证分片...")
    valid_all = set()

    for i in range(1, PARTS + 1):
        f = os.path.join(DIST_DIR, f"validated_part_{i:02d}.txt")
        if os.path.exists(f):
            with open(f, "r", encoding="utf-8") as fp:
                for line in fp:
                    valid_all.add(line.strip())

    out = os.path.join(DIST_DIR, "validated_all.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(valid_all)))

    print(f"✅ ✅ 最终文件已生成 → {out}")
    print(f"✅ 共 {len(valid_all)} 条有效规则")


# =====================================================
# ✅ 主入口
# =====================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载和切片")
    parser.add_argument("--merge", action="store_true", help="合并所有分片结果")
    args = parser.parse_args()

    # 强制下载
    if args.force_update:
        download_all_sources()
        split_parts()

    # 自动补齐
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        download_all_sources()
        split_parts()

    # 验证单片
    if args.part:
        process_part(args.part)
        exit(0)

    # 合并最终结果
    if args.merge:
        merge_validated_results()
        exit(0)

    print("ℹ 用法:")
    print("   python3 script.py --part 1        # 验证第1片")
    print("   python3 script.py --merge         # 合并验证结果")
    print("   python3 script.py --force-update  # 重新下载 + 分片")
