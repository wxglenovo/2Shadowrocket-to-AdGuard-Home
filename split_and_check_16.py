#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, requests, argparse, dns.resolver, json
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    merged=set()
    with open(URLS_TXT,"r",encoding="utf-8") as f:
        urls=[u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r=requests.get(url,timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line=line.strip()
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    with open(MASTER_RULE,"w",encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    print(f"✅ 合并 {len(merged)} 条规则", flush=True)
    return True

def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE,"r",encoding="utf-8") as f:
        rules=[l.strip() for l in f if l.strip()]
    total=len(rules)
    per_part=(total+PARTS-1)//PARTS
    for i in range(PARTS):
        part_rules=rules[i*per_part:(i+1)*per_part]
        filename=os.path.join(TMP_DIR,f"part_{i+1:02d}.txt")
        with open(filename,"w",encoding="utf-8") as f:
            f.write("\n".join(part_rules))
    return True

def check_domain(rule):
    resolver=dns.resolver.Resolver()
    resolver.timeout=DNS_TIMEOUT
    resolver.lifetime=DNS_TIMEOUT
    domain=rule.lstrip("|").split("^")[0].replace("*","")
    if not domain: return None
    try:
        resolver.resolve(domain)
        return rule
    except: return None

def dns_validate(lines, batch_size=500):
    print(f"🚀 启动 {DNS_WORKERS} 并发验证，每批 {batch_size} 条", flush=True)
    valid=[]
    total=len(lines)
    for i in range(0,total,batch_size):
        batch=lines[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures={executor.submit(check_domain,rule):rule for rule in batch}
            done_count=0
            for future in as_completed(futures):
                done_count+=1
                result=future.result()
                if result: valid.append(result)
                if done_count%50==0:
                    print(f"✅ 已验证 {i+done_count}/{total} 条，有效 {len(valid)} 条", flush=True)
    print(f"✅ 分片验证完成，有效 {len(valid)} 条", flush=True)
    return valid

def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE,"r",encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE,"w",encoding="utf-8") as f:
        json.dump(counter,f,indent=2,ensure_ascii=False)

def process_part(part):
    part_file=os.path.join(TMP_DIR,f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        download_all_sources()
        split_parts()
    lines=open(part_file,"r",encoding="utf-8").read().splitlines()
    valid=set(dns_validate(lines))
    out_file=os.path.join(DIST_DIR,f"validated_part_{part}.txt")
    old_rules=set()
    if os.path.exists(out_file):
        with open(out_file,"r",encoding="utf-8") as f:
            old_rules=set([l.strip() for l in f if l.strip()])
    delete_counter=load_delete_counter()
    new_delete_counter={}
    final_rules=set()
    removed_count=0
    added_count=0
    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            new_delete_counter[rule]=0
        else:
            count=delete_counter.get(rule,0)+1
            new_delete_counter[rule]=count
            if count>=DELETE_THRESHOLD:
                removed_count+=1
            else:
                final_rules.add(rule)
        if rule not in old_rules and rule in valid:
            added_count+=1
    save_delete_counter(new_delete_counter)
    with open(out_file,"w",encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))
    print(f"✅ 分片 {part} 完成: 总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}", flush=True)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--part",help="验证指定分片 1~16")
    parser.add_argument("--force-update",action="store_true",help="强制重新下载规则源并切片")
    args=parser.parse_args()
    if args.force_update:
        download_all_sources()
        split_parts()
    if args.part:
        process_part(args.part)
