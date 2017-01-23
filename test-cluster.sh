#!/bin/sh

OUTPUT_DIR=/tmp_epod/tmp

if [ "$#" -gt 0 ]; then
  OUTPUT_DIR=$1
fi

rm -f $OUTPUT_DIR/subtile_average_17_2.tif && \
rm -f $OUTPUT_DIR/subtile_average_18_2.tif && \
sh run-cluster $OUTPUT_DIR && \
echo "d18b61eec14e203157961571b84b795b  $OUTPUT_DIR/subtile_average_17_2.tif" | md5sum -c - && \
echo "39978405e412a2e0cf09c5ad6193c1d0  $OUTPUT_DIR/subtile_average_18_2.tif" | md5sum -c -

