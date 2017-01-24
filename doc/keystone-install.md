# Install OpenStack keystone on Ubuntu 14.04

## Prep the instance
        apt-get update
        apt-get install -y vim screen git ntp rabbitmq-server
        update-alternatives --set editor /usr/bin/vim.basic

        apt-get install ubuntu-cloud-keyring
        echo "deb http://ubuntu-cloud.archive.canonical.com/ubuntu" \
            "trusty-updates/kilo main" > /etc/apt/sources.list.d/cloudarchive-kilo.list

        apt-get install -y mariadb-server python-mysqldb  #root/cfskeystone

        cp my.cnf /etc/mysql/conf.d/mysqld_openstack.cnf
        # [mysqld]
        # bind-address = 127.0.0.1
        # ...
        # default-storage-engine = innodb
        # innodb_file_per_table
        # collation-server = utf8_general_ci
        # init-connect = 'SET NAMES utf8'
        # character-set-server = utf8

        service mysql restart
        mysql_secure_installation

        rabbitmqctl add_user openstack cfsrabbit
        rabbitmqctl set_permissions openstack ".*" ".*" ".*"


## Install and Setup OpenStack keystone
        mysql -u root -p

        CREATE DATABASE keystone;
        GRANT ALL PRIVILEGES ON keystone.* TO 'keystone'@'localhost' \
            IDENTIFIED BY 'KEYSTONE_DBPASS';    # keystone/cfskeystone
        GRANT ALL PRIVILEGES ON keystone.* TO 'keystone'@'%' \
            IDENTIFIED BY 'KEYSTONE_DBPASS';    # keystone/cfskeystone

        openssl rand -hex 10  # generate admin token  028475ab47c1074e1153

        echo "manual" > /etc/init/keystone.override

        apt-get install -y keystone python-openstackclient apache2 libapache2-mod-wsgi memcached python-memcache

        vim /etc/keystone/keystone.conf
            # [DEFAULT]
            # admin_token = 028475ab47c1074e1153
            # verbose = True
            # [database]
            # connection = mysql://keystone:cfskeystone@127.0.0.1/keystone
            # [memcache]
            # servers = localhost:11211
            # [token]
            # provider = keystone.token.providers.uuid.Provider
            # driver = keystone.token.persistence.backends.memcache.Token
            # [revoke]
            # driver = keystone.contrib.revoke.backends.sql.Revoke
        
        su -s /bin/sh -c "keystone-manage db_sync" keystone

    




    

