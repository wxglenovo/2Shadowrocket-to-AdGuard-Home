import os
import sys
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# 获取删除计数
def get_deletion_count(rule_id):
    deletion_count_file = "tmp/deletion_count.txt"
    
    if os.path.exists(deletion_count_file):
        with open(deletion_count_file, "r") as f:
            for line in f:
                if line.startswith(rule_id):
                    return int(line.split()[1])  # 返回删除计数
    return 0  # 如果文件不存在或没有找到规则ID，则返回0

# 更新删除计数
def update_deletion_count(rule_id, count):
    deletion_count_file = "tmp/deletion_count.txt"
    
    # 读取文件内容
    lines = []
    if os.path.exists(deletion_count_file):
        with open(deletion_count_file, "r") as f:
            lines = f.readlines()

    # 检查该规则ID是否已经存在，并更新计数
    updated = False
    with open(deletion_count_file, "w") as f:
        for line in lines:
            if line.startswith(rule_id):
                f.write(f"{rule_id} {count}\n")
                updated = True
            else:
                f.write(line)
        if not updated:
            # 如果该规则ID不存在，新增条目
            f.write(f"{rule_id} {count}\n")

# 规则删除逻辑
def process_rule(rule_id):
    # 获取该规则的删除计数
    deletion_count = get_deletion_count(rule_id)
    
    # 判断是否可以删除规则（当删除计数 >= 4 时删除）
    if deletion_count >= 4:
        logging.info(f"规则 {rule_id} 被标记为删除。")
        # 在此可以实现删除规则的具体操作
        return True  # 返回 True 表示该规则被删除
    else:
        logging.info(f"规则 {rule_id} 删除计数: {deletion_count}, 仍然保留。")
        return False  # 返回 False 表示该规则未删除

# 执行分片检查
def validate_part(part):
    rule_id = f"part_{part:02d}"  # 使用分片编号作为规则ID
    logging.info(f"开始验证分片 {rule_id}")
    
    # 模拟规则的验证（这里只是个示例，可以根据实际需求修改）
    deleted = process_rule(rule_id)
    
    # 如果规则被删除了，输出提示
    if deleted:
        logging.info(f"分片 {rule_id} 已被删除。")
    else:
        logging.info(f"分片 {rule_id} 继续保留。")
    
    # 更新删除计数
    current_count = get_deletion_count(rule_id)
    update_deletion_count(rule_id, current_count + 1)

    logging.info(f"分片 {rule_id} 的删除计数已更新为 {current_count + 1}。")

# 主函数
def main():
    try:
        part = int(sys.argv[1])  # 从命令行获取分片编号
    except (IndexError, ValueError):
        logging.error("未提供有效的分片编号，默认使用分片 01")
        part = 1  # 默认验证第一个分片
    
    validate_part(part)  # 验证分片

if __name__ == "__main__":
    main()
