#! /usr/bin/env bash
# 檢查Container是否第一次啟動
CONTAINER_ALREADY_STARTED="/var/._init_container_"
# 第一次啟動的邏輯
if [ ! -e $CONTAINER_ALREADY_STARTED ]; then
    # 建立一個檔案，有這檔案存在代表，代表Container已經啟動過了
    touch $CONTAINER_ALREADY_STARTED
    echo "-- First container startup --"
    bash /app/first_run.sh
# 非第一次啟動的邏輯
else
    echo "-- Not first container startup --"
fi