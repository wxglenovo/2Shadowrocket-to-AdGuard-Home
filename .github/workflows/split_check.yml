name: Split & DNS Check

# -----------------------------
# 触发条件
# -----------------------------
on:
  schedule:
    - cron: "0 0 * * *"   # 每天 00:00 UTC
    - cron: "0 6 * * *"   # 每天 06:00 UTC
    - cron: "0 12 * * *"  # 每天 12:00 UTC
    - cron: "0 18 * * *"  # 每天 18:00 UTC
    - cron: "*/22 * * * *" # 每 22 分钟一次
  workflow_dispatch:
    inputs:
      part:
        description: '手动验证指定分片 1~16'
        required: false
        default: ''

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest
    env:
      PYTHONUNBUFFERED: 1

    steps:

      # -----------------------------
      # 1. 检出仓库
      # -----------------------------
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      # -----------------------------
      # 2. 设置 Python
      # -----------------------------
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      # -----------------------------
      # 3. 安装依赖
      # -----------------------------
      - name: Install dependencies
        run: pip install --upgrade requests dnspython

      # -----------------------------
      # 4. 配置 Git
      # -----------------------------
      - name: Configure Git
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"

      # -----------------------------
      # 5. 确定当前分片 (PART)
      # -----------------------------
      - name: Determine PART index
        id: detect
        run: |
          mkdir -p tmp
          LAST_PART_FILE="tmp/last_part.txt"
          PART_INPUT="${{ github.event.inputs.part }}"

          if [ -n "$PART_INPUT" ]; then
            PART="$PART_INPUT"
            echo "🛠 手动指定分片：$PART"
          else
            if [ -f "$LAST_PART_FILE" ]; then
              LAST_PART=$(cat "$LAST_PART_FILE")
              PART=$(( (LAST_PART % 16) + 1 ))
            else
              PART=1
            fi
            echo "⏱ 自动轮替分片：$PART"
          fi

          echo "$PART" > "$LAST_PART_FILE"
          echo "part=$PART" >> $GITHUB_OUTPUT

      # -----------------------------
      # 6. 定时下载规则源（每天四次）并覆盖分片
      # -----------------------------
      - name: Force download rules at schedule times
        run: |
          CURRENT_HOUR=$(date -u +"%H")
          if [[ "$CURRENT_HOUR" == "00" || "$CURRENT_HOUR" == "06" || "$CURRENT_HOUR" == "12" || "$CURRENT_HOUR" == "18" ]]; then
            echo "✅ 强制下载规则源并生成所有分片"
            python3 split_and_check_16.py --force-update
          else
            echo "⏩ 非下载时间，不强制更新"
          fi

      # -----------------------------
      # 7. 确保 rules 和首个分片存在（首次运行）
      # -----------------------------
      - name: Ensure rules and first part exist
        run: |
          MERGED_FILE="merged_rules.txt"
          FIRST_PART="tmp/part_01.txt"
          if [ ! -f "$MERGED_FILE" ] || [ ! -f "$FIRST_PART" ]; then
            echo "⚠ 缺少规则文件或分片 → 重新拉取"
            python3 split_and_check_16.py --force-update
          else
            echo "✅ 规则文件和分片存在"
          fi

      # -----------------------------
      # 8. 确保 delete_counter.json 存在
      # -----------------------------
      - name: Ensure delete_counter.json exists
        run: |
          mkdir -p dist
          if [ ! -f dist/delete_counter.json ]; then
            echo "{}" > dist/delete_counter.json
            echo "✅ 创建 dist/delete_counter.json"
          else
            echo "✅ delete_counter.json 已存在"
          fi

      # -----------------------------
      # 9. 对当前分片进行 DNS 验证
      # -----------------------------
      - name: Run DNS validation for current part
        env:
          PART: ${{ steps.detect.outputs.part }}
        run: |
          mkdir -p logs
          echo "⏱ 开始验证分片 $PART"
          python3 split_and_check_16.py --part "$PART" | tee logs/split_check_part_${PART}.log

      # -----------------------------
      # 10. 提交并推送验证后的规则
      # -----------------------------
      - name: Commit & Push Validated Rules
        env:
          PART: ${{ steps.detect.outputs.part }}
        run: |
          STATS=$(grep "COMMIT_STATS" logs/split_check_part_${PART}.log | tail -n1 | sed 's/COMMIT_STATS: //')
          
          # ✅ 添加文件，首次不存在也不会报错
          git add dist
          for f in dist/validated_part_*.txt; do
            [ -f "$f" ] && git add "$f"
          done
          git add merged_rules.txt tmp/last_part.txt

          # ✅ commit message 使用日志 STATS
          git commit -m "🤖 part $PART → $STATS" || echo "⚠ 无可提交内容"

          # ✅ pull 失败时忽略
          git pull --rebase || echo "⚠ Pull failed, 已忽略"

          # ✅ push
          git push || echo "⚠ Push failed"

      # -----------------------------
      # 11. 输出 tmp 目录内容（调试用）
      # -----------------------------
      - name: Show tmp directory
        run: |
          echo "📂 当前 tmp 目录内容："
          ls -lh tmp || echo "⚠ tmp 目录不存在或无法访问"
          echo "ℹ 如果 tmp/part_**.txt 无法生成，请检查 split_and_check_16.py 是否正常生成分片"
