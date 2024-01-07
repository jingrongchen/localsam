#!/bin/bash

cd /home/ubuntu/opt/pixels
mvn clean install

cd /home/ubuntu/opt/localsam
rm -r .aws-sam
cd HelloWorldFunction
mvn clean package
cd ..
sam build