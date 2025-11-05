import os
import json
import dns.resolver
import logging
from collections import defaultdict

# 配置文件路径
MERGED_FILE = 'merged_rules.txt'
SPLIT_DIR = 'split_rules'
VALIDATED_DIR = 'validated_parts'
LOG_FILE = 'logs/validation.log'
DELETE_COUNTER_FILE = 'delete_counter.json'

# 设置日志格式
logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

# 确保日志文件目录存在
os.makedirs('logs', exist_ok=True)

# 加载或初始化删除计数
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, 'r') as f:
            return json.load(f)
    return defaultdict(int)

# 保存删除计数
def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, 'w') as f:
        json.dump(counter, f, indent=4)

# 下载并分片规则
def split_rules():
    if not os.path.exists(MERGED_FILE):
        print(f"{MERGED_FILE} does not exist.")
        return
    
    with open(MERGED_FILE, 'r') as f:
        rules = f.readlines()

    # 计算每个分片的大小
    total_rules = len(rules)
    rules_per_part = total_rules // 24
    os.makedirs(SPLIT_DIR, exist_ok=True)
    
    # 将规则分成 24 份
    for i in range(24):
        start_idx = i * rules_per_part
        end_idx = (i + 1) * rules_per_part if i != 23 else total_rules
        part_file = os.path.join(SPLIT_DIR, f"part_{i+1}.txt")
        with open(part_file, 'w') as part:
            part.writelines(rules[start_idx:end_idx])

    print(f"Rules have been split into {24} parts.")
    logging.info(f"Rules have been split into {24} parts.")

# DNS 查询验证
def validate_dns(rule):
    try:
        resolver = dns.resolver.Resolver()
        response = resolver.resolve(rule, 'A')  # 使用 A 记录进行解析
        return True if response else False
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout):
        return False

# 处理验证结果
def process_validation_results():
    delete_counter = load_delete_counter()
    os.makedirs(VALIDATED_DIR, exist_ok=True)
    
    # 分片验证
    for i in range(24):
        part_file = os.path.join(SPLIT_DIR, f"part_{i+1}.txt")
        validated_part_file = os.path.join(VALIDATED_DIR, f"validated_part_{i+1}.txt")
        not_written_counter = defaultdict(int)
        
        with open(part_file, 'r') as part, open(validated_part_file, 'w') as validated_part:
            for rule in part:
                rule = rule.strip()
                success = validate_dns(rule)
                
                # DNS 验证成功
                if success:
                    delete_counter[rule] = 0
                    validated_part.write(f"{rule}\n")
                    not_written_counter[rule] = 0
                    logging.info(f"Rule validated successfully: {rule}")
                # DNS 验证失败
                else:
                    delete_counter[rule] += 1
                    if delete_counter[rule] >= 4:
                        logging.info(f"Rule failed DNS validation: {rule} (failure count {delete_counter[rule]})")
                    if delete_counter[rule] >= 7:
                        logging.info(f"Rule marked for deletion due to 7 failures: {rule}")
            
            # 删除计数 >= 17 的规则，重置为 5
            for rule in list(delete_counter.keys()):
                if delete_counter[rule] >= 17:
                    delete_counter[rule] = 5
                    logging.info(f"Reset delete count for {rule} to 5 due to excessive failures.")
            
            # 删除连续三次未写入的规则
            for rule, count in not_written_counter.items():
                if count >= 3:
                    logging.info(f"Rule {rule} deleted from part {i+1} due to no consecutive write.")
                    with open(validated_part_file, 'r') as validated_part_read:
                        lines = validated_part_read.readlines()
                    with open(validated_part_file, 'w') as validated_part_write:
                        for line in lines:
                            if line.strip() != rule:
                                validated_part_write.write(line)
            
    # 保存更新的删除计数
    save_delete_counter(delete_counter)

if __name__ == "__main__":
    split_rules()
    process_validation_results()
