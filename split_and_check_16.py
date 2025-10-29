name: AdGuardHome Blocklist Auto Update

on:
  schedule:
    - cron: "0 0 * * *"
    - cron: "0 6 * * *"
    - cron: "0 12 * * *"
    - cron: "0 18 * * *"
  workflow_dispatch:

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: 3.12

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests dnspython

      - name: Run blocklist split & DNS check
        run: |
          echo "⏱ Workflow 开始：$(date -u)"
          # 可手动传 part 参数，例如：--part 3
          python split_and_check_16.py | tee split_check.log
          echo "⏱ Workflow 结束：$(date -u)"

      - name: Commit & Push changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          
          # 添加 dist 与 tmp 文件
          [ -f dist/blocklist_valid.txt ] && git add dist/blocklist_valid.txt
          shopt -s nullglob
          for f in tmp/validated_*.txt tmp/failed_*.txt; do
            git add "$f"
          done

          git commit -m "🤖 Auto update: valid blocklist" || echo "No changes"
          git push origin main || echo "⚠️ Push 失败"
