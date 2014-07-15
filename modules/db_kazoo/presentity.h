#ifndef _DBK_PRESENTITY_
#define _DBK_PRESENTITY_

#include "../../lib/srdb1/db_id.h"
#include "../../mi/mi_types.h"
#include "dbase.h"
#include "const.h"

typedef struct dbk_presentity {
	str domain;
	str username;
	str event;
	str etag;
	str sender;
	str body;
	int received_time;
	int expires;
    struct dbk_presentity *next;
} dbk_presentity_t;

typedef struct {
    gen_lock_t lock;
    dbk_presentity_t *pu;
    dbk_presentity_t *tail;
} dbk_presentity_htable_t;

int  dbk_presentity_initialize(void);
void dbk_presentity_destroy(void);
int  dbk_presentity_query(const db1_con_t* _h, const db_key_t* _k, const db_op_t * _op, const db_val_t* _v, const db_key_t* _c, int _n, int _nc, const db_key_t _o, db1_res_t** _r);
int  dbk_presentity_new(const db1_con_t* _h, const db_key_t* db_col, const db_val_t* db_val, const int _n);
int  dbk_presentity_delete(const db1_con_t * _h, const db_key_t * _k, const db_op_t * _o, const db_val_t * _v, const int _n);
int  dbk_presentity_update(const db1_con_t * _h, const db_key_t * _k, const db_op_t * _o, const db_val_t * _v, const db_key_t * _uk, const db_val_t * _uv, const int _n, const int _un);


dbk_presentity_t *dbk_presentity_search(unsigned int hash_code, str *event, str *domain, str * username, dbk_presentity_t ** pu_prev_ret,dbk_presentity_t * prev_pu_addr);
int dbk_presentity_htable_insert(str *event, str *domain, str *username, str * etag, str * sender,str * body, int received_time, int expires, unsigned int hash_code);
dbk_presentity_t *dbk_presentity_htable_new(str *event, str *domain, str *username, str * etag, str * sender, str * body, int received_time, int expires);

void dbk_free_presentity(dbk_presentity_t * pu);

struct mi_root * mi_dbk_presentity_dump(struct mi_root *cmd_tree, void *param);
struct mi_root * mi_dbk_presentity_flush(struct mi_root *cmd_tree, void *param);
int dbk_presentity_flush(int flush_all, str *event, str * domain, str * user);

#endif
