#!/usr/bin/env bash

PREV=`pwd`
ACTIVATOR_DIRECTORY=activator-1.3.10-minimal

cd /tmp
wget https://downloads.typesafe.com/typesafe-activator/1.3.10/typesafe-$ACTIVATOR_DIRECTORY.zip -O activator.zip
unzip activator.zip
sudo mv $ACTIVATOR_DIRECTORY/* /usr/local
cd $PREV
