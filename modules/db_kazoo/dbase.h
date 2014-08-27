#ifndef _DB_KAZOO_DBASE_
#define _DB_KAZOO_DBASE_

#include <amqp.h>

#include "../../lib/srdb1/db.h"
#include "../../lib/srdb1/db_id.h"
#include "../../lib/srdb1/db_pool.h"

#include "../tm/tm_load.h"

extern str dbk_node_hostname;
extern str dbk_reg_fs_path;
extern int dbk_auth_wait_timeout;
extern int dbk_reconn_retries;
extern struct tm_binds tmb;
extern str dbk_consumer_event_key;
extern str dbk_consumer_event_subkey;
extern int dbk_consumer_processes;

typedef struct rmq_conn {
	struct db_id* id;        /**< Connection identifier */
	unsigned int ref;        /**< Reference count */
	struct pool_con* next;   /**< Next element in the pool */

	amqp_connection_state_t conn;
	amqp_socket_t *socket;
	int channel;
	amqp_bytes_t exchange;
	amqp_basic_properties_t props;
        
}rmq_conn_t;

//rmq_conn_t* dbk_dummy_db_conn(struct db_id* id);

/* function that checks for error */
static inline int rmq_error(char const *context, amqp_rpc_reply_t x)
{
	amqp_connection_close_t *mconn;
	amqp_channel_close_t *mchan;

	switch (x.reply_type) {
		case AMQP_RESPONSE_NORMAL:
			return 0;

		case AMQP_RESPONSE_NONE:
			LM_ERR("%s: missing RPC reply type!", context);
			break;

		case AMQP_RESPONSE_LIBRARY_EXCEPTION:
			LM_ERR("%s: %s\n", context,  "(end-of-stream)");
			break;

		case AMQP_RESPONSE_SERVER_EXCEPTION:
			switch (x.reply.id) {
				case AMQP_CONNECTION_CLOSE_METHOD:
					mconn = (amqp_connection_close_t *)x.reply.decoded;
					LM_ERR("%s: server connection error %d, message: %.*s",
							context, mconn->reply_code,
							(int)mconn->reply_text.len,
							(char *)mconn->reply_text.bytes);
					break;
				case AMQP_CHANNEL_CLOSE_METHOD:
						mchan = (amqp_channel_close_t *)x.reply.decoded;
					LM_ERR("%s: server channel error %d, message: %.*s",
							context, mchan->reply_code,
							(int)mchan->reply_text.len,
							(char *)mchan->reply_text.bytes);
					break;
				default:
					LM_ERR("%s: unknown server error, method id 0x%08X",
							context, x.reply.id);
					break;
			}
			break;
	}
	return -1;
}


void* db_kazoo_new_connection(struct db_id* id);

int db_kazoo_query(const db1_con_t* _h, const db_key_t* _k, const db_op_t* _op,
		const db_val_t* _v, const db_key_t* _c, int _n, int _nc,
		const db_key_t _o, db1_res_t** _r);

int db_kazoo_insert (const db1_con_t* _h, const db_key_t* _k,
				const db_val_t* _v, const int _n);

void db_kazoo_free_connection(struct pool_con* con);

int db_kazoo_insert_update (const db1_con_t* _h, const db_key_t* _k,
				const db_val_t* _v, const int _n);

int db_kazoo_update (const db1_con_t* _h, const db_key_t* _k, const db_op_t* _o,
				const db_val_t* _v, const db_key_t* _uk, const db_val_t* _uv,
				const int _n, const int _un);

int db_kazoo_delete(const db1_con_t * _h, const db_key_t * _k,
		    const db_op_t * _o, const db_val_t * _v, const int _n);

int dbk_rmq_wait_for_data(amqp_connection_state_t conn);

void rmq_close(rmq_conn_t* rmq);

int rmqp_open_connection(rmq_conn_t* rmq);



#endif
