#!/bin/sh

output_dir=/tmp_epod/tmp

if [ "$#" -gt 0 ]; then
  output_dir=$1
fi

last_update() {
  local file=$1
  echo $(stat -c %Y $file)
}

current_time() {
  echo $(date +%s)
}

fail_if_too_old() {
  local file=$1
  local max_secs=$2

  local file_changed_at=$(last_update $file1)
  local now=$(current_time)
  local diff=$((now - file_changed_at))

  if [ $diff -gt $max_secs ]; then
    echo "$file has not been written to recently - did the job run?"
    exit 1
  fi
}

sh run-cluster $output_dir

file1="$output_dir/subtile_average_17_2.tif"
file2="$output_dir/subtile_average_18_2.tif"

fail_if_too_old $file1 120
fail_if_too_old $file2 120

echo "e3fbba4004d73e7f932b5705afe6259a  $file1" | md5sum -c - && \
echo "7a82d71b78534195686775b112837cb5  $file2" | md5sum -c -

