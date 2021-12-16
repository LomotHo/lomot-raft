#!/bin/bash

for((i=1;i<=100;i++));  
do
  echo $i
  go test -run 2A |grep FAIL
done  
