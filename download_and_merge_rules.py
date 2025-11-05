import requests
import os
from concurrent.futures import ThreadPoolExecutor

# 定义文件路径
URLS_FILE = 'urls.txt'
MERGED_FILE = 'merged_rules.txt'

def download_rule(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"下载 {url} 失败: {e}")
        return None

def download_and_merge_rules():
    if not os.path.exists(URLS_FILE):
        print(f"{URLS_FILE} 文件不存在。")
        return
    
    with open(URLS_FILE, 'r') as file:
        urls = file.readlines()

    # 使用 ThreadPoolExecutor 来并行下载规则
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(download_rule, url.strip()) for url in urls]

    # 合并所有下载的规则
    with open(MERGED_FILE, 'w', encoding='utf-8') as merged_file:
        for future in futures:
            result = future.result()
            if result:
                merged_file.write(result)
                merged_file.write('\n')

    print(f"所有规则已合并到 {MERGED_FILE}")

if __name__ == "__main__":
    download_and_merge_rules()
