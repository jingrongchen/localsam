#!/bin/bash

cd /home/ubuntu/opt/pixels
mvn clean install

cd /home/ubuntu/opt/lambda-java8
rm -r .aws-sam
cd HelloWorldFunction
mvn clean
cd ..
sam build