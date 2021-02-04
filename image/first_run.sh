#! /bin/bash
apt-get update
apt-get install -y nano
apt-get -y autoremove

pip install --upgrade pip

filename='/app/requirements.txt'
if [ -e $filename ]; then
    while IFS='' read -r line || [[ -n "$line" ]]; do
        if [[ -z "$line" ]]; then
            # echo "Skip empty line in requirements.txt"
            continue
        fi
        cmd="pip install $line"

        echo "$cmd" # 顯示目前正要安裝什麼
        output=$($cmd 2>&1)
        exit_code=$?
        if [ $exit_code -eq 1 ]; then
            echo $output
            exit
        fi
    done < $filename
fi

echo "API_PROPERTY=${API_PROPERTY}" > /etc/cron.d/cron
echo "SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# m h dom mon dow command
0,30 0   *   *   *  cd /app/schedule && python dcard_forums.py
0,30 *   *   *   *  cd /app/schedule && python dcard_article.py
0,30 *   *   *   *  cd /app/schedule && python dcard_comment.py

#* *   *   *   *  cd /app/schedule && python dcard_forums.py
#* *   *   *   *  cd /app/schedule && python dcard_article.py
#* *   *   *   *  cd /app/schedule && python dcard_comment.py" >> /etc/cron.d/cron
crontab /etc/cron.d/cron

rm /app/settings/crawler_status_*.json

echo "Delete cache files."
find /app/ | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
echo "first_run finish."