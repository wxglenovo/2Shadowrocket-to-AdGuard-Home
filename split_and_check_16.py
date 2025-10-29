from concurrent.futures import ThreadPoolExecutor, as_completed

def dns_validate(lines, workers=50):
    """多线程 DNS 验证，提高速度"""
    valid = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2
    resolver.nameservers = ["8.8.8.8", "1.1.1.1"]

    def check(rule):
        domain = rule.lstrip("|").split("^")[0].replace("*", "")
        if not domain:
            return None
        try:
            resolver.resolve(domain)
            return rule
        except:
            return None

    total = len(lines)
    print(f"🚀 多线程 DNS 验证启动：{workers} 并发，总计 {total} 条")

    processed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check, rule): rule for rule in lines}

        for future in as_completed(futures):
            processed += 1
            result = future.result()
            if result:
                valid.append(result)

            if processed % DNS_BATCH_SIZE == 0:
                print(f"✅ 已验证 {processed}/{total} 条，有效 {len(valid)} 条")

    print(f"✅ 分片验证结束：有效 {len(valid)} 条 / {total} 条")
    return valid
