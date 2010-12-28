#!/bin/bash

if [ ! "$TPCH_PATH" ] ; then
    echo "TPCH_PATH is not defined."
    exit 1
fi

SF=$1
SFF=$(bc <<< "scale = 2; $SF / 100.0")

pushd $TPCH_PATH
rm *.tbl
./dbgen -f -s $SFF
popd

cut -d'|' -f 1 $TPCH_PATH/region.tbl > region_${SF}.txt
cut -d'|' -f 1,3 $TPCH_PATH/nation.tbl > nation_${SF}.txt
cut -d'|' -f 1,4 $TPCH_PATH/customer.tbl > customer_${SF}.txt
cut -d'|' -f 1,2 $TPCH_PATH/orders.tbl > orders_${SF}.txt
cut -d'|' -f 1,5,6 $TPCH_PATH/lineitem.tbl > lineitem_${SF}.txt
