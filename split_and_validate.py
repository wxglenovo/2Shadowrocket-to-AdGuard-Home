import logging
import os
import dns.resolver
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import time

# 设置日志目录
if not os.path.exists('logs'):
    os.makedirs('logs')

# 配置日志，设置编码为 utf-8
LOG_FILE = 'logs/validation.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # 设置日志编码为 utf-8
)

# 配置文件路径
MERGED_FILE = 'merged_rules.txt'
SPLIT_DIR = 'split_rules'
VALIDATED_DIR = 'validated_parts'
DELETE_COUNTER_FILE = 'delete_counter.json'

# 加载或初始化删除计数
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return defaultdict(int)

# 保存删除计数
def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, 'w', encoding='utf-8') as f:
        json.dump(counter, f, indent=4, ensure_ascii=False)

# 下载并分片规则
def split_rules():
    if not os.path.exists(MERGED_FILE):
        print(f"{MERGED_FILE} 文件不存在。")
        return
    
    with open(MERGED_FILE, 'r', encoding='utf-8') as f:
        rules = f.readlines()

    # 计算每个分片的大小
    total_rules = len(rules)
    rules_per_part = total_rules // 24
    os.makedirs(SPLIT_DIR, exist_ok=True)
    
    # 将规则分成 24 份
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(24):
            start_idx = i * rules_per_part
            end_idx = (i + 1) * rules_per_part if i != 23 else total_rules
            part_file = os.path.join(SPLIT_DIR, f"part_{i+1}.txt")
            futures.append(executor.submit(write_part, part_file, rules[start_idx:end_idx]))

        for future in futures:
            future.result()  # 等待所有分片写入完成

    print(f"规则已分成 {24} 份。")
    logging.info(f"规则已分成 {24} 份。")

def write_part(part_file, part_rules):
    with open(part_file, 'w', encoding='utf-8') as part:
        part.writelines(part_rules)

# DNS 查询验证
def validate_dns(rule):
    try:
        resolver = dns.resolver.Resolver()
        response = resolver.resolve(rule, 'A')  # 使用 A 记录进行解析
        return True if response else False
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout):
        return False

# 批量 DNS 验证
def batch_validate_dns(rules_batch):
    results = {}
    for rule in rules_batch:
        results[rule] = validate_dns(rule)
    return results

# 计算预计剩余时间
def estimate_remaining_time(start_time, processed, total):
    elapsed_time = time.time() - start_time
    remaining_time = (elapsed_time / processed) * (total - processed) if processed else 0
    return remaining_time

# 处理验证结果
def process_validation_results():
    delete_counter = load_delete_counter()
    os.makedirs(VALIDATED_DIR, exist_ok=True)

    total_rules = sum(1 for _ in open(MERGED_FILE, 'r', encoding='utf-8'))
    rules_processed = 0
    start_time = time.time()

    # 分片验证
    with ThreadPoolExecutor(max_workers=80) as executor:
        futures = []
        for i in range(24):
            part_file = os.path.join(SPLIT_DIR, f"part_{i+1}.txt")
            validated_part_file = os.path.join(VALIDATED_DIR, f"validated_part_{i+1}.txt")
            futures.append(executor.submit(validate_part, part_file, validated_part_file, delete_counter, start_time, total_rules))

        for future in futures:
            future.result()  # 等待所有分片验证完成

    # 保存更新的删除计数
    save_delete_counter(delete_counter)

def validate_part(part_file, validated_part_file, delete_counter, start_time, total_rules):
    # 每次处理 1000 条规则
    batch_size = 1000
    not_written_counter = defaultdict(int)
    rules_processed = 0
    valid_count = 0
    total_batch = sum(1 for _ in open(part_file, 'r', encoding='utf-8')) // batch_size
    rules_batch = []

    with open(part_file, 'r', encoding='utf-8') as part, open(validated_part_file, 'w', encoding='utf-8') as validated_part:
        for rule in part:
            rules_batch.append(rule.strip())
            if len(rules_batch) >= batch_size:
                # 批量验证
                validation_results = batch_validate_dns(rules_batch)
                valid_count += sum(1 for res in validation_results.values() if res)
                for rule, success in validation_results.items():
                    process_rule(rule, success, delete_counter, validated_part, not_written_counter)
                rules_processed += len(rules_batch)

                # 打印日志
                elapsed_time = time.time() - start_time
                remaining_time = estimate_remaining_time(start_time, rules_processed, total_rules)
                logging.info(f"批次完成：有效数 {valid_count}，剩余时间估算：{remaining_time:.2f}秒")

                # 重置批次
                rules_batch = []

        # 处理剩余的规则
        if rules_batch:
            validation_results = batch_validate_dns(rules_batch)
            valid_count += sum(1 for res in validation_results.values() if res)
            for rule, success in validation_results.items():
                process_rule(rule, success, delete_counter, validated_part, not_written_counter)

            rules_processed += len(rules_batch)

            # 打印日志
            elapsed_time = time.time() - start_time
            remaining_time = estimate_remaining_time(start_time, rules_processed, total_rules)
            logging.info(f"最后一批完成：有效数 {valid_count}，剩余时间估算：{remaining_time:.2f}秒")

        # 删除计数 >= 17 的规则，重置为 5
        for rule in list(delete_counter.keys()):
            if delete_counter[rule] >= 17:
                delete_counter[rule] = 5
                logging.info(f"规则 {rule} 删除计数过多，已重置为 5。")
        
        # 删除连续三次未写入的规则
        for rule, count in not_written_counter.items():
            if count >= 3:
                logging.info(f"规则 {rule} 已删除，因为在当前分片中连续三次未写入。")
                with open(validated_part_file, 'r', encoding='utf-8') as validated_part_read:
                    lines = validated_part_read.readlines()
                with open(validated_part_file, 'w', encoding='utf-8') as validated_part_write:
                    for line in lines:
                        if line.strip() != rule:
                            validated_part_write.write(line)

def process_rule(rule, success, delete_counter, validated_part, not_written_counter):
    if success:
        delete_counter[rule] = 0
        validated_part.write(f"{rule}\n")
        not_written_counter[rule] = 0
        logging.info(f"规则验证成功: {rule}")
    else:
        delete_counter[rule] += 1
        if delete_counter[rule] >= 4:
            logging.info(f"规则 DNS 验证失败: {rule} (失败次数 {delete_counter[rule]})")
        if delete_counter[rule] >= 7:
            logging.info(f"规则因 7 次失败而被标记删除: {rule}")

def handle_deletion_counter(delete_counter):
    """并行处理删除计数大于7和小于7的规则"""
    with ThreadPoolExecutor(max_workers=2) as executor:
        # 将大于 7 和小于 7 的规则分开处理
        future_7_plus = executor.submit(process_large_delete_count, delete_counter)
        future_less_7 = executor.submit(process_small_delete_count, delete_counter)

        # 等待所有任务完成
        future_7_plus.result()
        future_less_7.result()

def process_large_delete_count(delete_counter):
    """处理删除计数大于等于7的规则"""
    for rule, count in list(delete_counter.items()):
        if count >= 7:
            delete_counter[rule] += 1
            if delete_counter[rule] >= 17:
                delete_counter[rule] = 5  # 重置为 5
                logging.info(f"规则 {rule} 删除计数大于等于 17，已重置为 5。")
            logging.info(f"规则 {rule} 删除计数更新为 {delete_counter[rule]}。")

def process_small_delete_count(delete_counter):
    """处理删除计数小于7的规则，进行分片和轮替 DNS 验证"""
    small_delete_count_rules = [rule for rule, count in delete_counter.items() if count < 7]
    
    # 将这些规则进行分片并轮替 DNS 验证
    os.makedirs(SPLIT_DIR, exist_ok=True)
    batch_size = 1000
    with ThreadPoolExecutor(max_workers=80) as executor:
        futures = []
        for i in range(0, len(small_delete_count_rules), batch_size):
            batch = small_delete_count_rules[i:i+batch_size]
            futures.append(executor.submit(batch_validate_dns, batch))

        for future in futures:
            future.result()  # 等待所有验证完成

    logging.info("删除计数小于 7 的规则已完成 DNS 验证。")

if __name__ == "__main__":
    split_rules()  # 分片规则
    delete_counter = load_delete_counter()  # 加载删除计数
    handle_deletion_counter(delete_counter)  # 并行处理删除计数
    save_delete_counter(delete_counter)  # 保存更新的删除计数
