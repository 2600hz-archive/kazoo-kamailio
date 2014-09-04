/*
 * $Id$
 *
 * Kazoo module interface
 *
 * Copyright (C) 2013 2600Hz
 *
 * This file is part of Kamailio, a free SIP server.
 *
 * Kamailio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 * Kamailio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License 
 * along with this program; if not, write to the Free Software 
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * History:
 * --------
 * 2013-04  first version (Anca Vamanu)
 */

#include <stdio.h>
#include <stdlib.h>

#include "../../sr_module.h"
#include "../../lib/srdb1/db.h"
#include "../../dprint.h"
#include "../../lib/kmi/mi.h"
#include "../tm/tm_load.h"
#include "../../cfg/cfg_struct.h"

#include "../pua/pua.h"
#include "../pua/pua_bind.h"
#include "../pua/send_publish.h"
#include "../presence/bind_presence.h"

#include "dbase.h"
//#include "blf.h"
#include "presentity.h"
#include "kz_amqp.h"
#include "kz_json.h"
#include "kz_fixup.h"
#include "kz_trans.h"
#include "kz_pua.h"

#define DBK_DEFAULT_NO_CONSUMERS 4

static int mod_init(void);
static int  mod_child_init(int rank);
static int fire_init_event(int rank);
static void mod_destroy(void);
static void  mod_consumer_proc(int rank);
int db_kazoo_bind_api(db_func_t * dbb);
static void init_shared_connection(const str* url, void* (*new_connection)(), db_pooling_t pooling);

str dbk_node_hostname = { 0, 0 };
str dbk_reg_fs_path = { 0, 0 };

int dbk_auth_wait_timeout = 3;
int dbk_reconn_retries = 8;

int dbk_presentity_phtable_size = 4096;

int dbk_dialog_expires = 30;
int dbk_presence_expires = 3600;
int dbk_mwi_expires = 3600;
int dbk_create_empty_dialog = 1;

int dbk_channels = 50;

int dbk_consumer_processes = DBK_DEFAULT_NO_CONSUMERS;

struct timeval kz_sock_tv = (struct timeval){0,100000};
struct timeval kz_amqp_tv = (struct timeval){0,100000};
struct timeval kz_qtimeout_tv = (struct timeval){2,0};
struct timeval kz_ack_tv = (struct timeval){0,100000};


str dbk_consumer_event_key = str_init("Event-Category");
str dbk_consumer_event_subkey = str_init("Event-Name");

int dbk_internal_loop_count = 5;
int dbk_consumer_loop_count = 10;
int dbk_consumer_ack_loop_count = 20;
int dbk_include_entity = 0;
int dbk_pua_mode = 0;

int dbk_single_consumer_on_reconnect = 1;
int dbk_consume_messages_on_reconnect = 1;


struct tm_binds tmb;
pua_api_t kz_pua_api;
presence_api_t kz_presence_api;

int startup_time = 0;

int *kz_pipe_fds = NULL;

db1_con_t * shared_db1 = NULL;

/* database connection */
db1_con_t *kz_pa_db = NULL;
db_func_t kz_pa_dbf;
str kz_presentity_table = str_init("presentity");
str kz_db_url = {0,0};

MODULE_VERSION

static tr_export_t mod_trans[] = {
	{ {"kz", sizeof("kz")-1}, kz_tr_parse},
	{ { 0, 0 }, 0 }
};

static pv_export_t kz_mod_pvs[] = {
	{{"kzR", (sizeof("kzR")-1)}, PVT_OTHER, kz_pv_get_last_query_result, 0,	0, 0, 0, 0},
	{{"kzE", (sizeof("kzE")-1)}, PVT_OTHER, kz_pv_get_event_payload, 0,	0, 0, 0, 0},
	{ {0, 0}, 0, 0, 0, 0, 0, 0, 0 }
};

/*
 *  database module interface
 */
static cmd_export_t cmds[] = {
    {"db_bind_api", (cmd_function) db_kazoo_bind_api, 0, 0, 0, 0},
    {"kazoo_publish", (cmd_function) kz_amqp_publish, 3, fixup_kz_amqp, fixup_kz_amqp_free, ANY_ROUTE},
    {"kazoo_query", (cmd_function) kz_amqp_query, 4, fixup_kz_amqp, fixup_kz_amqp_free, ANY_ROUTE},
    {"kazoo_query", (cmd_function) kz_amqp_query_ex, 3, fixup_kz_amqp, fixup_kz_amqp_free, ANY_ROUTE},
    {"kazoo_pua_publish", (cmd_function) kz_pua_publish, 1, 0, 0, ANY_ROUTE},
    {"kazoo_pua_flush", (cmd_function) w_mi_dbk_presentity_flush0, 0, 0, 0, ANY_ROUTE},
    {"kazoo_pua_flush", (cmd_function) w_mi_dbk_presentity_flush1, 1, 0, 0, ANY_ROUTE},
    {"kazoo_pua_flush", (cmd_function) w_mi_dbk_presentity_flush2, 2, 0, 0, ANY_ROUTE},
    {"kazoo_pua_flush", (cmd_function) w_mi_dbk_presentity_flush3, 3, 0, 0, ANY_ROUTE},
/*
    {"kazoo_subscribe", (cmd_function) kz_amqp_subscribe_1, 1, fixup_kz_amqp4, fixup_kz_amqp4_free, ANY_ROUTE},
    {"kazoo_subscribe", (cmd_function) kz_amqp_subscribe_2, 2, fixup_kz_amqp4, fixup_kz_amqp4_free, ANY_ROUTE},
    {"kazoo_subscribe", (cmd_function) kz_amqp_subscribe_3, 3, fixup_kz_amqp4, fixup_kz_amqp4_free, ANY_ROUTE},
*/
    {"kazoo_subscribe", (cmd_function) kz_amqp_subscribe, 1, fixup_kz_amqp4, fixup_kz_amqp4_free, ANY_ROUTE},
    {"kazoo_subscribe", (cmd_function) kz_amqp_subscribe_simple, 4, fixup_kz_amqp4, fixup_kz_amqp4_free, ANY_ROUTE},


    {"kazoo_json", (cmd_function) kz_json_get_field, 3, fixup_kz_json, fixup_kz_json_free, ANY_ROUTE},
    {"kazoo_encode", (cmd_function) kz_amqp_encode, 2, fixup_kz_amqp_encode, fixup_kz_amqp_encode_free, ANY_ROUTE},
    {0, 0, 0, 0, 0, 0}
};

static param_export_t params[] = {
    {"node_hostname", STR_PARAM, &dbk_node_hostname.s},
    {"register_fs_path", STR_PARAM, &dbk_reg_fs_path.s},
    {"auth_wait_timeout", INT_PARAM, &dbk_auth_wait_timeout},
    {"reconn_retries", INT_PARAM, &dbk_reconn_retries},
    {"presentity_hash_size", INT_PARAM, &dbk_presentity_phtable_size},
    {"dialog_expires", INT_PARAM, &dbk_dialog_expires},
    {"presence_expires", INT_PARAM, &dbk_presence_expires},
    {"mwi_expires", INT_PARAM, &dbk_mwi_expires},
    {"create_empty_dialog", INT_PARAM, &dbk_create_empty_dialog},
    {"amqp_connection", STR_PARAM|USE_FUNC_PARAM,(void*)kz_amqp_add_connection},
    {"amqp_max_channels", INT_PARAM, &dbk_channels},
    {"amqp_consumer_ack_timeout_micro", INT_PARAM, &kz_ack_tv.tv_usec},
    {"amqp_consumer_ack_timeout_sec", INT_PARAM, &kz_ack_tv.tv_sec},
    {"amqp_interprocess_timeout_micro", INT_PARAM, &kz_sock_tv.tv_usec},
    {"amqp_interprocess_timeout_sec", INT_PARAM, &kz_sock_tv.tv_sec},
    {"amqp_waitframe_timout_micro", INT_PARAM, &kz_amqp_tv.tv_usec},
    {"amqp_waitframe_timout_sec", INT_PARAM, &kz_amqp_tv.tv_sec},
    {"amqp_consumer_processes", INT_PARAM, &dbk_consumer_processes},
    {"amqp_consumer_event_key", STR_PARAM, &dbk_consumer_event_key.s},
    {"amqp_consumer_event_subkey", STR_PARAM, &dbk_consumer_event_subkey.s},
    {"amqp_query_timout_micro", INT_PARAM, &kz_qtimeout_tv.tv_usec},
    {"amqp_query_timout_sec", INT_PARAM, &kz_qtimeout_tv.tv_sec},
    {"amqp_internal_loop_count", INT_PARAM, &dbk_internal_loop_count},
    {"amqp_consumer_loop_count", INT_PARAM, &dbk_consumer_loop_count},
    {"amqp_consumer_ack_loop_count", INT_PARAM, &dbk_consumer_ack_loop_count},
    {"pua_include_entity", INT_PARAM, &dbk_include_entity},
    {"presentity_table", STR_PARAM, &kz_presentity_table.s},
	{"db_url", STR_PARAM, &kz_db_url.s},
    {"pua_mode", INT_PARAM, &dbk_pua_mode},
    {"single_consumer_on_reconnect", INT_PARAM, &dbk_single_consumer_on_reconnect},
    {"consume_messages_on_reconnect", INT_PARAM, &dbk_consume_messages_on_reconnect},
    {0, 0, 0}
};

static mi_export_t mi_cmds[] = {
//    {"presence_list", mi_dbk_phtable_dump, 0, 0, 0},
//    {"presence_flush", mi_dbk_phtable_flush, 0, 0, 0},
    {"presentity_list", mi_dbk_presentity_dump, 0, 0, 0},
    {"presentity_flush", mi_dbk_presentity_flush, 0, 0, 0},
    {0, 0, 0, 0, 0}
};

struct module_exports exports = {
    "db_kazoo",
    DEFAULT_DLFLAGS,		/* dlopen flags */
    cmds,
    params,			/* module parameters */
    0,				/* exported statistics */
    mi_cmds,			/* exported MI functions */
    kz_mod_pvs,				/* exported pseudo-variables */
    0,				/* extra processes */
    mod_init,			/* module initialization function */
    0,				/* response function */
    mod_destroy,		/* destroy function */
    mod_child_init				/* per-child init function */
};



static int kz_initialize_bindings() {
    LM_DBG("kz_initialize_bindings\n");

    /* load all TM stuff */
    if (load_tm_api(&tmb) == -1) {
    	LM_ERR("Can't load tm functions. Module TM not loaded?\n");
    	return -1;
    }

    /*
    bind_presence_t bind_presence= (bind_presence_t)find_export("bind_presence", 1,0);
    if (!bind_presence) {
        LM_ERR("Can't find presence module\n");
        return -1;
    } else if (bind_presence(&kz_presence_api) < 0) {
        LM_ERR("Can't bind to presence module api\n");
        return -1;
    }

    bind_pua_t bind_pua = (bind_pua_t) find_export("bind_pua", 1, 0);
    if (!bind_pua) {
		LM_ERR("Can't find bind pua\n");
		return -1;
    }
    if (bind_pua(&kz_pua_api) < 0) {
		LM_ERR("Can't bind to pua api\n");
		return -1;
    }
    */

    return 0;
}

static int mod_init(void) {
	int i;
    startup_time = (int) time(NULL);

    if (register_mi_mod(exports.name, mi_cmds) != 0) {
    	LM_ERR("failed to register MI commands\n");
	return -1;
    }

    if (dbk_node_hostname.s == NULL) {
	LM_ERR("You must set the node_hostname parameter\n");
	return -1;
    }
    dbk_node_hostname.len = strlen(dbk_node_hostname.s);

    if (dbk_reg_fs_path.s)
	dbk_reg_fs_path.len = strlen(dbk_reg_fs_path.s);

    dbk_consumer_event_key.len = strlen(dbk_consumer_event_key.s);
   	dbk_consumer_event_subkey.len = strlen(dbk_consumer_event_subkey.s);

    dbk_presentity_initialize();
    kz_amqp_init();

    str _dummy_url = str_init("kazoo://shared_memory/");
    init_shared_connection(&_dummy_url, (void *(*)())db_kazoo_new_shared_connection, DB_POOLING_PERMITTED);

    if(kz_initialize_bindings() == -1) {
   		LM_ERR("Error initializing bindings\n");
   		return -1;
   	}

//    if(kz_initialize_pua() != 0) {
//    	LM_ERR("Can't load pua functions. Module pua not loaded?\n");
//    	return -1;
//    }

	kz_db_url.len = kz_db_url.s ? strlen(kz_db_url.s) : 0;
	LM_DBG("db_url=%s/%d/%p\n", ZSW(kz_db_url.s), kz_db_url.len,kz_db_url.s);
	kz_presentity_table.len = strlen(kz_presentity_table.s);

	if(kz_db_url.len > 0) {

		/* binding to database module  */
		if (db_bind_mod(&kz_db_url, &kz_pa_dbf))
		{
			LM_ERR("Database module not found\n");
			return -1;
		}


		if (!DB_CAPABILITY(kz_pa_dbf, DB_CAP_ALL))
		{
			LM_ERR("Database module does not implement all functions"
					" needed by kazoo module\n");
			return -1;
		}

		kz_pa_db = kz_pa_dbf.init(&kz_db_url);
		if (!kz_pa_db)
		{
			LM_ERR("Connection to database failed\n");
			return -1;
		}

	}


    int total_workers = dbk_consumer_processes + 1;
    int total_pipes = total_workers + 1;
    kz_pipe_fds = (int*) shm_malloc(sizeof(int) * (total_pipes) * 2 );

    for(i=0; i < total_pipes; i++) {
    	kz_pipe_fds[i*2] = kz_pipe_fds[i*2+1] = -1;
		if (pipe(&kz_pipe_fds[i*2]) < 0) {
			LM_ERR("pipe(%d) failed\n", i);
			return -1;
		}
    }

//    dbk_initialize_presence();

    register_procs(total_workers);

    return 0;
}

int mod_register(char *path, int *dlflags, void *p1, void *p2)
{
	if(kz_tr_init_buffers()<0)
	{
		LM_ERR("failed to initialize transformations buffers\n");
		return -1;
	}
	return register_trans_mod(path, mod_trans);
}


static int mod_child_init(int rank)
{
	int pid;
	int i;

	fire_init_event(rank);

	if (rank==PROC_MAIN) {
		pid=fork_process(PROC_NOCHLDINIT, "AMQP Manager", 1);
		if (pid<0)
			return -1; /* error */
		if(pid==0){
			kz_amqp_manager_loop(0);
		}
		else {
			for(i=0; i < dbk_consumer_processes; i++) {
				pid=fork_process(PROC_NOCHLDINIT, "AMQP Consumer", 1);
				if (pid<0)
					return -1; /* error */
				if(pid==0){
					mod_consumer_proc(i+1);
				}
			}
		}
	}

	return 0;
}

static void  mod_consumer_proc(int rank)
{
	kz_amqp_consumer_loop(rank);
}


static int fire_init_event(int rank)
{
	struct sip_msg *fmsg;
	struct run_act_ctx ctx;
	int rtb, rt;

	LM_DBG("rank is (%d)\n", rank);
	if (rank!=PROC_INIT)
		return 0;

	rt = route_get(&event_rt, "kazoo:mod-init");
	if(rt>=0 && event_rt.rlist[rt]!=NULL) {
		LM_DBG("executing event_route[kazoo:mod-init] (%d)\n", rt);
		if(faked_msg_init()<0)
			return -1;
		fmsg = faked_msg_next();
		rtb = get_route_type();
		set_route_type(REQUEST_ROUTE);
		init_run_actions_ctx(&ctx);
		run_top_route(event_rt.rlist[rt], fmsg, &ctx);
		if(ctx.run_flags&DROP_R_F)
		{
			LM_ERR("exit due to 'drop' in event route\n");
			return -1;
		}
		set_route_type(rtb);
	}

	return 0;
}




static void init_shared_connection(const str* url, void* (*new_connection)(), db_pooling_t pooling)
{
	struct db_id* id;
	void* con;
	db1_con_t* res;

	int con_size = sizeof(db1_con_t) + sizeof(void *);
	id = 0;
	res = 0;

	if (!url || !url->s || !new_connection) {
		LM_ERR("invalid parameter value\n");
		return;
	}

	/* this is the root memory for this database connection. */
	res = (db1_con_t*)shm_malloc(con_size);
	if (!res) {
		LM_ERR("no private memory left\n");
		return;
	}
	memset(res, 0, con_size);

	id = new_db_id(url, pooling);
	if (!id) {
		LM_ERR("cannot parse URL '%.*s'\n", url->len, url->s);
		goto err;
	}

	/* Find the connection in the pool */
	con = pool_get(id);
	if (!con) {
		LM_DBG("connection %p not found in pool\n", id);
		/* Not in the pool yet */
		con = new_connection(id);
		if (!con) {
			LM_ERR("could not add connection to the pool");
			goto err;
		}
		pool_insert((struct pool_con*)con);
	} else {
		LM_DBG("connection %p found in pool\n", id);
	}

	res->tail = (unsigned long)con;
	shared_db1 = res;

	return;

 err:
	if (id) free_db_id(id);
	if (res) shm_free(res);
}



db1_con_t *db_kazoo_init(const str * _url) {
	LM_DBG("DBKAZOO INIT %.*s\n", _url->len, _url->s);
	return db_do_init(_url, (void *(*)())db_kazoo_new_connection);
}

/*!
 * \brief Close database when the database is no longer needed
 * \param _h closed connection, as returned from db_kazoo_init
 * \note free all memory and resources
 */
void db_kazoo_close(db1_con_t * _h) {
    db_do_close(_h, (void (*)())db_kazoo_free_connection);
}


/*
 * Store name of table that will be used by
 * subsequent database functions
 */

int db_kazoo_use_table(db1_con_t * _h, const str * _t) {
	LM_DBG("USE TABLE %.*s\n", _t->len, _t->s);
	if(_h == NULL && !strncmp(_t->s, "presentity", 10)) {
		LM_INFO("OUCH!!");
		return db_use_table(shared_db1, _t);
	}
	else {
		return db_use_table(_h, _t);
	}
}

int db_kazoo_free_result(db1_con_t * _h, db1_res_t * _r) {
	LM_DBG("FREE RESULTS");
    return db_free_result(_r);
}

int db_kazoo_raw_query(const db1_con_t * _h, const str * _s, db1_res_t ** _r) {
    return 0;
}

int db_kazoo_replace(const db1_con_t * handle, const db_key_t * keys,
		     const db_val_t * vals, const int n, const int _un,
		     const int _m) {
    return 0;
}

int db_kazoo_bind_api(db_func_t * dbb) {
	LM_DBG("BIND API\n");

    if (dbb == NULL)
	return -1;

    LM_DBG("BIND API OK\n");

    memset(dbb, 0, sizeof(db_func_t));

    dbb->init = db_kazoo_init;
    dbb->use_table = db_kazoo_use_table;
    dbb->close = db_kazoo_close;
    dbb->query = db_kazoo_query;
    dbb->free_result = db_kazoo_free_result;
    dbb->insert = db_kazoo_insert;
    dbb->replace = db_kazoo_replace;
    dbb->insert_update = db_kazoo_insert_update;
    dbb->delete = db_kazoo_delete;
    dbb->update = db_kazoo_update;
    dbb->raw_query = db_kazoo_raw_query;
    dbb->cap = DB_CAP_ALL;

    return 0;
}

static void mod_destroy(void) {
	dbk_presentity_destroy();
	kz_amqp_destroy();
	if(shared_db1) {
		shm_free((void *)shared_db1->tail);
		shm_free(shared_db1);
	}
    shm_free(kz_pipe_fds);
}


