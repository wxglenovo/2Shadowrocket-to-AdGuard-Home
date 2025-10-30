def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")
    valid = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            if rule in delete_counter:
                print(f"🔄 验证成功，清零删除计数: {rule}")
            # 验证成功清零计数
            new_delete_counter[rule] = 0
        else:
            count = delete_counter.get(rule, 0) + 1
            new_delete_counter[rule] = count
            print(f"⚠ 连续删除计数 {count}/{DELETE_THRESHOLD}: {rule}")
            if count >= DELETE_THRESHOLD:
                removed_count += 1
            else:
                final_rules.add(rule)
        if rule not in old_rules and rule in valid:
            added_count += 1

    save_delete_counter(new_delete_counter)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    # commit 信息显示统计
    print(f"💾 Commit 信息: 分片 {part} → 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
