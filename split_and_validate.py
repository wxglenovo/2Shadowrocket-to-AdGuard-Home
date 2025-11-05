import logging
import os
import dns.resolver
from collections import defaultdict

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
    for i in range(24):
        start_idx = i * rules_per_part
        end_idx = (i + 1) * rules_per_part if i != 23 else total_rules
        part_file = os.path.join(SPLIT_DIR, f"part_{i+1}.txt")
        with open(part_file, 'w', encoding='utf-8') as part:
            part.writelines(rules[start_idx:end_idx])

    print(f"规则已分成 {24} 份。")
    logging.info(f"规则已分成 {24} 份。")

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
        
        with open(part_file, 'r', encoding='utf-8') as part, open(validated_part_file, 'w', encoding='utf-8') as validated_part:
            for rule in part:
                rule = rule.strip()
                success = validate_dns(rule)
                
                # DNS 验证成功
                if success:
                    delete_counter[rule] = 0
                    validated_part.write(f"{rule}\n")
                    not_written_counter[rule] = 0
                    logging.info(f"规则验证成功: {rule}")
                # DNS 验证失败
                else:
                    delete_counter[rule] += 1
                    if delete_counter[rule] >= 4:
                        logging.info(f"规则 DNS 验证失败: {rule} (失败次数 {delete_counter[rule]})")
                    if delete_counter[rule] >= 7:
                        logging.info(f"规则因 7 次失败而被标记删除: {rule}")
            
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
            
    # 保存更新的删除计数
    save_delete_counter(delete_counter)

if __name__ == "__main__":
    split_rules()
    process_validation_results()
