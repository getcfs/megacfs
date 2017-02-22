# Keystone

## create a vm to run keystone
./rack servers instance create --name=wreese-keystone.rackfs.com --flavor-id=general1-8 --image-name="Ubuntu 14.04 LTS (Trusty Tahr) (PVHVM)" --keypair=wreese

## log into the vm
ssh -A root@<ip>

## install keystone
apt-get update
apt-get install keystone

## need these to bootstrap keystone from the cli
export OS_SERVICE_TOKEN=ADMIN
export OS_SERVICE_ENDPOINT=http://127.0.0.1:35357/v2.0

## create an admin tenant and user
keystone tenant-create --name admin --description "Admin Tenant"
keystone role-create --name admin
keystone user-create --name admin --pass admin --tenant admin
keystone user-role-add --user admin --role admin --tenant admin 

## create a service tenant and user
keystone tenant-create --name service --description "Service Tenant"
keystone role-create --name service
keystone user-create --name service --pass service --tenant service
keystone user-role-add --user service --role service --tenant service

## create a test tenant and user
keystone tenant-create --name test --description "Test Tenant"
keystone user-create --name test --pass test --tenant test

## create a cfs user, service and endpoint
keystone user-create --name cfs --pass cfs --tenant service
keystone user-role-add --user cfs --role admin --tenant service
keystone service-create --name cfs --type cfs --description cfs
keystone endpoint-create --publicurl 1.1.1.1:8445 --service-id $(keystone service-list | awk '/ cfs / {print $2}')

## get token for the admin user
export ADMIN_TOKEN=`
curl -s -i -H "Content-Type: application/json" -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "admin",
          "domain": { "id": "default" },
          "password": "admin"
        }
      }
    }
  }
}' http://localhost:5000/v3/auth/tokens | grep 'X-Subject-Token' | cut -d' ' -f2`

## get token for the service user
export SERVICE_TOKEN=`
curl -s -i -H "Content-Type: application/json" -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "service",
          "domain": { "id": "default" },
          "password": "service"
        }
      }
    }
  }
}' http://localhost:5000/v3/auth/tokens | grep 'X-Subject-Token' | cut -d' ' -f2`

## get token for test user
export TEST_TOKEN=`
curl -s -i -H "Content-Type: application/json" -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "test",
          "domain": { "id": "default" },
          "password": "test"
        }
      }
    }
  }
}' http://localhost:5000/v3/auth/tokens | grep 'X-Subject-Token' | cut -d' ' -f2`

export OS_TOKEN=`
curl -s -i -H "Content-Type: application/json" -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "test",
          "domain": { "id": "default" },
          "password": "test"
        }
      }
    }
  }
}' http://localhost:5000/v3/auth/tokens | grep 'X-Subject-Token' | cut -d' ' -f2`

## validate tokens with the super token
curl -i \
-H "X-Auth-Token: ADMIN" \
-H "X-Subject-Token: $ADMIN_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: ADMIN" \
-H "X-Subject-Token: $SERVICE_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: ADMIN" \
-H "X-Subject-Token: $TEST_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
# validate tokens with an admin token
curl -i \
-H "X-Auth-Token: $ADMIN_TOKEN" \
-H "X-Subject-Token: $ADMIN_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: $ADMIN_TOKEN" \
-H "X-Subject-Token: $SERVICE_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: $ADMIN_TOKEN" \
-H "X-Subject-Token: $TEST_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo

## validate tokens with a service token
curl -i \
-H "X-Auth-Token: $SERVICE_TOKEN" \
-H "X-Subject-Token: $ADMIN_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: $SERVICE_TOKEN" \
-H "X-Subject-Token: $SERVICE_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo
curl -i \
-H "X-Auth-Token: $SERVICE_TOKEN" \
-H "X-Subject-Token: $TEST_TOKEN" \
http://localhost:5000/v3/auth/tokens ; echo


curl -s -i -H "Content-Type: application/json" -d '
{ "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "admin",
          "domain": { "id": "default" },
          "password": "admin"
        }
      }
    }
  }
}' http://cfsadmin.qe02.iad.rackfs.com:5000/v3/auth/tokens | grep 'X-Subject-Token' | cut -d' ' -f2
