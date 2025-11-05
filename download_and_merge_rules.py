import requests
import os

# 定义文件路径
URLS_FILE = 'urls.txt'
MERGED_FILE = 'merged_rules.txt'

def download_rule(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def download_and_merge_rules():
    if not os.path.exists(URLS_FILE):
        print(f"{URLS_FILE} does not exist.")
        return
    
    with open(URLS_FILE, 'r') as file:
        urls = file.readlines()

    with open(MERGED_FILE, 'w') as merged_file:
        for url in urls:
            url = url.strip()
            print(f"Downloading rules from {url}...")
            rules = download_rule(url)
            if rules:
                merged_file.write(rules)
                merged_file.write('\n')

    print(f"All rules have been merged into {MERGED_FILE}")

if __name__ == "__main__":
    download_and_merge_rules()
