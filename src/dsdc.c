#include "redis.h"
#include "dsdc_sunrpc.h"
#include "sha1.h"

//-----------------------------------------------------------------------------

void* dsdcHeartbeatLoop(void* arg);
CLIENT* dsdcMasterConnection(int index);
void dsdcMakekey(dsdc_key_t* out, const char* hostname, int port, int id);
void dsdcSpawnMasterThread(int index);

//-----------------------------------------------------------------------------

void dsdcInit(void) {
    for (int i = 0; i < server.dsdc_master_count; i++) {
        dsdcSpawnMasterThread(i);
    }
}

//-----------------------------------------------------------------------------

void dsdcSpawnMasterThread(int index) {
    pthread_attr_t attr;
    pthread_t thread;

    CLIENT* clnt = dsdcMasterConnection(index);

    pthread_attr_init(&attr);
    if (pthread_create(&thread,&attr,dsdcHeartbeatLoop,clnt) != 0) {
        redisLog(REDIS_WARNING,"Fatal: Can't initialize DSDC Heartbeat.");
        exit(1);
    }
}

//-----------------------------------------------------------------------------

CLIENT* dsdcMasterConnection(int index) {
    char myname[256];
    char* master_host = server.dsdc_masters[index];
    int num_keys = server.dsdc_num_keys;

    CLIENT* clnt = clnt_create(master_host, DSDC_PROG, DSDC_VERS, "tcp");

    if (clnt) {
        printf("dsdc: connected to master: %s\n", master_host);
    } else {
        clnt_pcreateerror(master_host);
        exit(1);
    }

    dsdc_register2_arg_t reg_arg;
    reg_arg.primary = TRUE;
    reg_arg.lock_server = FALSE;

    gethostname(myname, 256); 

    dsdcx_slave2_t slave;
    slave.hostname = myname;
    slave.port = server.port;
    slave.slave_type = DSDC_REDIS_SLAVE;
    
    dsdc_keyset_t keys;
    keys.dsdc_keyset_t_val = zmalloc(sizeof(dsdc_key_t) * num_keys);
    keys.dsdc_keyset_t_len = num_keys;

    for (int i = 0; i < num_keys; i++) {
        dsdcMakekey(&keys.dsdc_keyset_t_val[i], myname, server.port, i);
    }

    slave.keys = keys;
    reg_arg.slave = slave;  

    dsdc_res_t* res = dsdc_register2_1(&reg_arg, clnt);
    if (!(res && *res == DSDC_OK)) {
        clnt_perror(clnt, "dsdc: registration failed");
        exit(1);
    }

    return clnt;
}

//-----------------------------------------------------------------------------

void* dsdcHeartbeatLoop(void* arg) {

    CLIENT* clnt = (CLIENT*) arg;

    while (TRUE) {
        dsdc_res_t* res = dsdc_heartbeat_1(NULL, clnt);
        if (!res) {
            printf("dsdc: heartbeat failed\n");
        }

        sleep(1);
    }

    return NULL;
}

//-----------------------------------------------------------------------------

void dsdcMakekey(dsdc_key_t* out, const char* hostname, int port, 
                 int index) {

    char buffer[1024];
    SHA1_CTX ctx;
    
    int len = snprintf(buffer, 1024, "%s-%d-%d", hostname, port, index);

    SHA1Init(&ctx);
    SHA1Update(&ctx, (unsigned char*) buffer, len);
    SHA1Final( (unsigned char*) out, &ctx);

}
