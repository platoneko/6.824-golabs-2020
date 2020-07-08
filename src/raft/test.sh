#!/bin/sh
x=10
while [ $x -gt 0 ]
do
    go test -run 2B
    let x--
done