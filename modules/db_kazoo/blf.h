#ifndef _DBK_BLF_
#define _DBK_BLF_

#include "../../lib/srdb1/db_id.h"
#include "../../mi/mi_types.h"
#include "dbase.h"
#include "defs.h"
#include "const.h"

int dbk_initialize_presence(void);
void dbk_start_presence_rmqp_consumer_processes(struct db_id* id);
void dbk_destroy_presence(void);
int dbk_presence_query(const db1_con_t* _h, const db_key_t* _k,
		const db_op_t * _op, const db_val_t* _v, const db_key_t* _c, int _n, int _nc,
		const db_key_t _o, db1_res_t** _r);
int dbk_dialoginfo_update(const db1_con_t* _h, const db_key_t* db_col,
				const db_val_t* db_val, const int _n);
struct mi_root * mi_dbk_phtable_dump(struct mi_root *cmd_tree, void *param);
struct mi_root * mi_dbk_phtable_flush(struct mi_root *cmd_tree, void *param);
int dbk_presence_subscribe_new(const db1_con_t* _h, const db_key_t* db_col,
				const db_val_t* db_val, const int _n);
int dbk_presence_subscribe_update(const db1_con_t* _h, const db_key_t* _k,
				const db_val_t* _v, const db_key_t* _uk, const db_val_t* _uv,
				const int _n, const int _un);

#define DBK_PRES_WORKERS_NO 6

#endif
