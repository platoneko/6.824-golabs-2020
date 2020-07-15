#!/bin/sh
x=30
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run TestPersistPartitionUnreliableLinearizable3A
    let x--
done