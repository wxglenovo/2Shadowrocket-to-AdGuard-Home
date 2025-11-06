import subprocess

def commit_to_git():
    try:
        # 添加更改到暂存区
        subprocess.run(["git", "add", "."], check=True)

        # 提交更改
        commit_message = "自动提交生成的文件"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        # 推送更改到远程仓库
        subprocess.run(["git", "push"], check=True)

        print("✅ 自动提交和推送成功！")
    except subprocess.CalledProcessError as e:
        print(f"⚠ Git 操作失败: {e}")

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，拉取规则中…")
        download_all_sources()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = [l.strip() for l in open(part_file, "r", encoding="utf-8").read().splitlines()]
    print(f"⏱ 验证分片 {part}, 共 {len(lines)} 条规则（不剔除注释）")

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_json(DELETE_COUNTER_FILE)
    rules_to_validate = []
    final_rules = set(old_rules)
    added_count = 0
    removed_count = 0

    for r in lines:
        del_cnt = delete_counter.get(r, 4)
        if del_cnt < 7:
            rules_to_validate.append(r)
        else:
            delete_counter[r] = del_cnt + 1
            print(f"⚠ 删除计数达到 7 或以上，跳过规则：{r} | 删除计数={del_cnt}")

    valid = dns_validate(rules_to_validate)

    # 连续失败计数
    failure_count = [0] * (DELETE_THRESHOLD + 1)  # 用于记录每个失败次数的规则数量

    for rule in rules_to_validate:
        if rule in valid:
            final_rules.add(rule)
            delete_counter[rule] = 0
            added_count += 1
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            fail_count = delete_counter[rule]

            # 更新失败计数
            if fail_count <= DELETE_THRESHOLD:
                failure_count[fail_count] += 1
                print(f"⚠ 连续失败 {fail_count}/{DELETE_THRESHOLD} 的规则条数: {failure_count[fail_count]} 条")
                
            if fail_count >= DELETE_THRESHOLD:
                removed_count += 1
                final_rules.discard(rule)

    save_json(DELETE_COUNTER_FILE, delete_counter)

    # 将有效规则写入对应的分片文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    # 更新 `not_written_counter.json` 文件
    update_not_written_counter(part, final_rules)

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

    # 自动提交更改
    commit_to_git()
