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

#include "dbase.h"
#include "blf.h"

static int mod_init(void);
static void mod_destroy(void);
int db_kazoo_bind_api(db_func_t * dbb);

str dbk_node_hostname = { 0, 0 };
str dbk_reg_fs_path = { 0, 0 };

int dbk_auth_wait_timeout = 3;
int dbk_reconn_retries = 8;

struct tm_binds tmb;

MODULE_VERSION
/*
 *  database module interface
 */
static cmd_export_t cmds[] = {
    {"db_bind_api", (cmd_function) db_kazoo_bind_api, 0, 0, 0},
    {0, 0, 0, 0, 0}
};

static param_export_t params[] = {
    {"node_hostname", STR_PARAM, &dbk_node_hostname.s},
    {"register_fs_path", STR_PARAM, &dbk_reg_fs_path.s},
    {"auth_wait_timeout", INT_PARAM, &dbk_auth_wait_timeout},
    {"reconn_retries", INT_PARAM, &dbk_reconn_retries},
    {0, 0, 0}
};

static mi_export_t mi_cmds[] = {
    {"presence_list", mi_dbk_phtable_dump, 0, 0, 0},
    {"presence_flush", mi_dbk_phtable_flush, 0, 0, 0},
    {0, 0, 0, 0, 0}
};

struct module_exports exports = {
    "db_kazoo",
    DEFAULT_DLFLAGS,		/* dlopen flags */
    cmds,
    params,			/* module parameters */
    0,				/* exported statistics */
    mi_cmds,			/* exported MI functions */
    0,				/* exported pseudo-variables */
    0,				/* extra processes */
    mod_init,			/* module initialization function */
    0,				/* response function */
    mod_destroy,		/* destroy function */
    0				/* per-child init function */
};

static int mod_init(void) {
    if (register_mi_mod(exports.name, mi_cmds) != 0) {
	LM_ERR("failed to register MI commands\n");
	return -1;
    }

    register_procs(DBK_PRES_WORKERS_NO);

    if (dbk_node_hostname.s == NULL) {
	LM_ERR("You must set the node_hostname parameter\n");
	return -1;
    }
    dbk_node_hostname.len = strlen(dbk_node_hostname.s);

    if (dbk_reg_fs_path.s)
	dbk_reg_fs_path.len = strlen(dbk_reg_fs_path.s);

    /* load all TM stuff */
    if (load_tm_api(&tmb) == -1) {
	LM_ERR("Can't load tm functions. Module TM not loaded?\n");
	return -1;
    }

    return 0;
}

db1_con_t *db_kazoo_init(const str * _url) {
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
    return db_use_table(_h, _t);
}

int db_kazoo_free_result(db1_con_t * _h, db1_res_t * _r) {
    return db_free_result(_r);
}

int db_kazoo_delete(const db1_con_t * _h, const db_key_t * _k,
		    const db_op_t * _o, const db_val_t * _v, const int _n) {
    LM_INFO("delete table=%s\n", _h->table->s);
    return 0;
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
    if (dbb == NULL)
	return -1;

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
    dbk_destroy_presence();
}
