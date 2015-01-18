#!/bin/bash

CASSANDRA_VERSION=2.1.2
CASSANDRA_DISTRIBUTION=apache-cassandra-$CASSANDRA_VERSION
CASSANDRA_HOME=~/proj/$CASSANDRA_DISTRIBUTION

init()
{
    sudo rm -rf /var/lib/cassandra
    sudo rm -rf /var/log/cassandra

    sudo mkdir -p /var/lib/cassandra/data
    sudo mkdir -p /var/lib/cassandra/saved_caches
    sudo mkdir -p /var/lib/cassandra/commitlog
    sudo mkdir -p /var/log/cassandra/

    sudo chown -R $USER:$USER /var/lib/cassandra
    sudo chown -R $USER:$USER /var/log/cassandra

    ls -l /var/lib/cassandra
    ls -l /var/log/cassandra

    echo "CASSANDRA_HOME=$CASSANDRA_HOME" | sudo tee -a /etc/environment
    source /etc/environment

    tsocks wget http://www.us.apache.org/dist/cassandra/$CASSANDRA_VERSION/$CASSANDRA_DISTRIBUTION-bin.tar.gz
    tar -zxvf $CASSANDRA_DISTRIBUTION-bin.tar.gz
    cp ./cass_yaml_config $CASSANDRA_DISTRIBUTION/conf/

    cd $CASSANDRA_DISTRIBUTION/conf
    ./cass_yaml_config $1 $2 $3 $4

    cd ../bin
    ./cassandra
}

stop()
{
    ./$CASSANDRA_DISTRIBUTION/bin/nodetool stopdaemon 2>/dev/null
    echo "Cassandra stopped."
}

restart()
{
    stop
    ./$CASSANDRA_DISTRIBUTION/bin/cassandra
}

clear()
{
    echo -n "Do you really want to clear all the data?(yes/no): "
    read CHOICE
    if [ "$CHOICE"x = "yes"x ]
    then
        stop
        sudo rm -rf /var/lib/cassandra/data/*
        sudo rm -rf /var/lib/cassandra/saved_caches/*
        sudo rm -rf /var/lib/cassandra/commitlog/*
        sudo rm -rf /var/log/cassandra/*
        echo -n "Do you want to restart the cassandra server?(yes/no): "
        read RESTART
        if [ "$RESTART"x = "yes"x ]
        then
            restart
        else
            echo "clear done."
        fi
    else
        echo "not clear"
    fi
}

# main function
case "$1" in
  "init")
    init $2 $3 $4 $5
    ;;
  "stop")
    stop
    ;;
  "restart")
    restart
    ;;
  "clear")
    clear
    ;;
  *)
    echo "usage: $0 init [arg1 arg2 arg3 arg4]|stop|restart|clear"
    ;;
esac
