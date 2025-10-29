DNS_BATCH_SIZE = 800
PARTS = 16
MAX_WORKERS = 80

# 省略下载 urls.txt、去注释、去重、切分等逻辑

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--part', type=int, help='手动指定分片 0~15')
    args = parser.parse_args()

    # 计算当前分片
    if args.part is not None:
        part_index = args.part
    else:
        from datetime import datetime
        minute = datetime.utcnow().hour * 60 + datetime.utcnow().minute
        part_index = (minute // 90) % PARTS

    target_file = f"tmp/part_{part_index:02d}.txt"
    print(f"📄 当前处理分片：{target_file}")
    
    # 读取规则
    with open(target_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()
    print(f"⏱ 总规则 {len(rules):,} 条")
    print(f"前 10 条规则示例： {rules[:10]}")

    # DNS 验证
    import concurrent.futures
    valid = []
    def check_batch(batch):
        batch_valid = []
        import dns.resolver
        resolver = dns.resolver.Resolver()
        resolver.timeout = 1.5
        resolver.lifetime = 1.5
        resolver.nameservers = ["1.1.1.1","8.8.8.8","9.9.9.9"]
        for rule in batch:
            domain = rule.lstrip('|').lstrip('.').split('^')[0]
            try:
                resolver.resolve(domain, 'A')
                batch_valid.append(rule)
            except:
                continue
        return batch_valid

    batches = [rules[i:i+DNS_BATCH_SIZE] for i in range(0,len(rules),DNS_BATCH_SIZE)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for i, result in enumerate(ex.map(check_batch, batches), 1):
            valid.extend(result)
            print(f"✅ 已验证 {min(i*DNS_BATCH_SIZE,len(rules)):,}/{len(rules):,} 条，本批有效 {len(result):,} 条")

    # 保存有效规则
    import os
    os.makedirs("dist", exist_ok=True)
    with open("dist/blocklist_valid.txt", "a", encoding="utf-8") as f:
        f.write("\n".join(valid)+"\n")
