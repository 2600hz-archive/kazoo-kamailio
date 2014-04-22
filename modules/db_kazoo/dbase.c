
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <json/json.h>
#include <sys/time.h>
#include <unistd.h>
#include "../../lib/srdb1/db.h"
#include "../../lib/srdb1/db_id.h"
#include "../../lib/srdb1/db_pool.h"
#include "../../dprint.h"
#include "../../mem/mem.h"
#include "../../sr_module.h"

#include "dbase.h"
#include "blf.h"

int connections = 0;

void rmq_close(rmq_conn_t* rmq)
{
  LM_DBG("Close rmq connection\n");
  if (!rmq)
    return;
  
  if (rmq->channel) {
    LM_DBG("close channel: %d rmq(%p)->channel(%d)\n", getpid(), (void *)rmq, rmq->channel);
    rmq_error("closing channel",
              amqp_channel_close(rmq->conn, rmq->channel,
                                 AMQP_REPLY_SUCCESS));
    rmq->channel = 0;
  }
  
  if (rmq->conn) {
    LM_DBG("close connection:  %d rmq(%p)->conn(%p)\n", getpid(), (void *)rmq, rmq->conn);
    rmq_error("closing connection",
              amqp_connection_close(rmq->conn, AMQP_REPLY_SUCCESS));
    
    if (amqp_destroy_connection(rmq->conn) < 0) {
      LM_ERR("cannot destroy connection\n");
    }
    rmq->conn = NULL;
    rmq->socket = NULL;
    rmq->channel = 0;
  } else if (rmq->socket) {
    LM_DBG("close socket: %d rmq(%p)->socket(%p)\n", getpid(), (void *)rmq, (void *)rmq->socket);
    amqp_socket_close(rmq->socket);
    rmq->socket = NULL;
  }
}

int rmqp_open_connection(rmq_conn_t* rmq)
{

  LM_DBG("%d rmq:%p  conn:%p  socket:%p  channel:%d\n", getpid(), (void *)rmq, rmq->conn, (void *)rmq->socket, rmq->channel);

  rmq->socket = amqp_tcp_socket_new();
  if (!rmq->socket) {
    LM_DBG("Failed to create TCP socket to AMQP broker\n");
    goto error;
  }
  
  /* TODO - take as module parameters */
  if ( amqp_socket_open(rmq->socket, rmq->id->host, rmq->id->port) ) {
    LM_DBG("Failed to open TCP socket to AMQP broker\n");
    goto error;
  }

  if (!(rmq->conn = amqp_new_connection())) {
    LM_DBG("Failed to create new AMQP connection\n");
    goto error;
  }
    
  amqp_set_socket(rmq->conn, rmq->socket);
  
  if ( rmq_error("Logging in", amqp_login(rmq->conn,
                                          "/",
                                          0,
                                          131072,
                                          0,
                                          AMQP_SASL_METHOD_PLAIN,
                                          rmq->id->username,
			rmq->id->password)) ) {
    LM_ERR("Login to AMQP broker failed!\n");
    goto error;
  }
  
  rmq->channel = process_no + 1;
  amqp_channel_open(rmq->conn, rmq->channel);
  if (rmq_error("Opening channel", amqp_get_rpc_reply(rmq->conn))) {
    LM_ERR("Failed to open channel AMQP %d!\n", rmq->channel);
    goto error;
  }
  
  return 0;
  
 error:
  rmq_close(rmq);
  return -1;
}

rmq_conn_t* dbk_dummy_db_conn(struct db_id* id)
{
	rmq_conn_t* rmq;
	int db_len = strlen(id->database);
	int size = sizeof(rmq_conn_t) + db_len;

	rmq = (rmq_conn_t*)pkg_malloc(size);
	if (!rmq) {
		LM_ERR("No more private memory\n");
		return NULL;
	}
	memset(rmq, 0, size);
	rmq->id = id;
	rmq->ref = 1;

	LM_DBG("Created new dummy rmq structure %p for %s\n", rmq, id->database);

	rmq->exchange.bytes = (char*)rmq+ sizeof(rmq_conn_t);
	memcpy(rmq->exchange.bytes, id->database, db_len);
	rmq->exchange.len = db_len;

	LM_DBG("Return dummy db conn\n");
	return rmq;
}
	

int presence_initialized = 0;
void* db_kazoo_new_connection(struct db_id* id)
{
  LM_DBG("New db connection to exchange %s for %d process %d\n", id->database, getpid(), process_no);
  if (strncmp(id->database, "dialoginfo", 10) == 0) {
    if (process_no == 0 && !presence_initialized) {
      if(dbk_initialize_presence() < 0) {
        LM_ERR("Failed to initialize db_kazoo for presence");
        return NULL;
      }
      presence_initialized = 1;
    }
    if (process_no == 1) {
      LM_DBG("Start presence rmqp consumer processes\n");
      dbk_start_presence_rmqp_consumer_processes(id);
    }
  }

  return dbk_dummy_db_conn(id);
}


/*!
 * \brief Close the connection and release memory
 * \param connection
 */
void db_kazoo_free_connection(struct pool_con* con)
{
  rmq_conn_t * _c;
  
  LM_DBG("Close connection\n");
  
  if (!con) return;
  _c = (rmq_conn_t*) con;
  
  rmq_close(_c);
  pkg_free(_c);
}

#define KEY_SAFE(C)  ((C >= 'a' && C <= 'z') || \
                      (C >= 'A' && C <= 'Z') || \
                      (C >= '0' && C <= '9') || \
                      (C == '-' || C == '~'  || C == '_'))

#define HI4(C) (C>>4)
#define LO4(C) (C & 0x0F)


#define hexint(C) (C < 10?('0' + C):('A'+ C - 10))

char* amqp_util_encode(const str* key, char* dest)
{
  if ( (key->len == 1) && (key->s[0] == '#' || key->s[0] == '*') ) {
    *dest++ = key->s[0];
    return dest;
  }
  char* p, *end;
  for (p = key->s, end = key->s + key->len; p < end; p++) {
    if (KEY_SAFE(*p)) {
      *dest++= *p;
    } else if (*p == '.' ) {
      memcpy(dest, "\%2E", 3);
      dest+= 3;
    } else if (*p == ' ') {
      *dest++ = '+';
    } else {
      *dest++='%';
      sprintf(dest, "%c%c", hexint(HI4(*p)), hexint(LO4(*p)));
      dest+=2;
    }
  }
  return dest;
}

#if 0
encode(<<"*">>) -> <<"*">>;
encode(<<"#">>) -> <<"#">>;
encode(Bin) -> << <<(encode_char(B))/binary>> || <<B>> <= Bin >>.

-define(HI4(C), (C band 2#11110000) bsr 4).
-define(LO4(C), (C band 2#00001111)).

encode_char(C) when ?KEY_SAFE(C) -> <<C>>;
encode_char($\s) -> <<$+>>;
encode_char($.) -> <<$%, $2, $E>>;
encode_char(C) ->
    Hi4 = ?HI4(C),
    Lo4 = ?LO4(C),
    <<$%, (hexint(Hi4)), (hexint(Lo4))>>.

}
#endif

#define AUTH_ROUTING_KEY_PREFIX "authn.req."
#define AUTH_ROUTING_KEY_PREFIX_LEN sizeof("authn.req.") - 1

#define REG_ROUTING_KEY_PREFIX "registration.success."
#define REG_ROUTING_KEY_PREFIX_LEN sizeof("registration.success.") - 1

static unsigned long rpl_routing_key_count = 0;

db1_res_t* dbk_creds_build_result(char* body, const db_key_t* _c, int _nc) {
  struct json_object *json_obj;	
  struct json_object *init_json_obj = NULL;	
  db1_res_t* db_res = 0;
  str password;
  int col;
  
  db_res = db_new_result();
  if (!db_res) {
    LM_ERR("no memory left\n");
    return NULL;
  }
  RES_ROW_N(db_res) = 0;
  
  json_obj = json_tokener_parse(body);
  if (is_error(json_obj)) {
    LM_ERR("Error parsing json: %s\n",
           json_tokener_errors[-(unsigned long)json_obj]);
    goto error;
  }
  init_json_obj = json_obj;
  
  struct json_object* password_obj = json_object_object_get(json_obj, "Auth-Password");
  if (is_error(json_obj)) {
    LM_ERR("Error parsing json: %s\n",
           json_tokener_errors[-(unsigned long)json_obj]);
    goto error;
  }
  password.s = (char*)json_object_get_string(password_obj);
  if (password.s == NULL) {
    LM_ERR("Json-c error - failed to extract password [%s]\n", body);
    goto error;
  }
  password.len = strlen(password.s);
  LM_DBG("Password: [%s]\n", password.s);
  
  RES_COL_N(db_res) = _nc;
  RES_ROW_N(db_res) = 1;
  if (db_allocate_rows(db_res) < 0) {
    LM_ERR("Could not allocate rows.\n");
    goto error; 
  }
  
  if (db_allocate_columns(db_res, RES_COL_N(db_res)) != 0) {
    LM_ERR("Could not allocate columns\n");
    goto error;
  }
  if (db_allocate_row(db_res, &(RES_ROWS(db_res)[0])) != 0) {
    LM_ERR("Could not allocate row.\n");
    goto error; 
  }
  
  for(col = 0; col < RES_COL_N(db_res); col++) {
    RES_NAMES(db_res)[col] = (str*)pkg_malloc(sizeof(str));
    if (! RES_NAMES(db_res)[col]) {
      LM_ERR("no private memory left\n");
      RES_COL_N(db_res) = col;
      db_free_columns(db_res);
      goto error;
    }
    LM_DBG("Allocated %lu bytes for RES_NAMES[%d] at %p\n",
           (unsigned long)sizeof(str), col, RES_NAMES(db_res)[col]);
    
    RES_NAMES(db_res)[col]->s = _c[col]->s;
    RES_NAMES(db_res)[col]->len = _c[col]->len;
    RES_TYPES(db_res)[col] = DB1_STR;
    
    LM_DBG("RES_NAMES(%p)[%d]=[%.*s]\n", RES_NAMES(db_res)[col], col,
           RES_NAMES(db_res)[col]->len, RES_NAMES(db_res)[col]->s);
  }
  
  
  /* complete the row with the columns */
  for(col = 0; col< _nc; col++) {
    LM_DBG("Col[%d]: %.*s\n", col, _c[col]->len, _c[col]->s);
    RES_ROWS(db_res)[0].values[col].type = DB1_STR;
    if (strncmp(_c[col]->s, "password", _c[col]->len) == 0 ) {
      LM_DBG("Wrote the password in the result\n");
      
      RES_ROWS(db_res)[0].values[col].val.str_val.s = (char*)pkg_malloc(password.len+1);
      if (RES_ROWS(db_res)[0].values[col].val.str_val.s==NULL) {
        PKG_MEM_ERROR;
        goto error;
      }
      strcpy(RES_ROWS(db_res)[0].values[col].val.str_val.s, password.s);
      RES_ROWS(db_res)[0].values[col].val.str_val.len = password.len;
      
      RES_ROWS(db_res)[0].values[col].free  = 1;
      RES_ROWS(db_res)[0].values[col].nul  = 0;
      LM_DBG("Password in result: [%s]\n", RES_ROWS(db_res)[0].values[col].val.string_val);
    } else {
      RES_ROWS(db_res)[0].values[col].val.str_val.s = "";
      RES_ROWS(db_res)[0].values[col].val.str_val.len = 0;
      RES_ROWS(db_res)[0].values[col].free  = 0;
      RES_ROWS(db_res)[0].values[col].nul  = 1;
    } 
  }
  //	RES_ROW_N(db_res) = 1;
  
  /* decrement the reference of the object*/
  json_object_put(init_json_obj);
  
  return db_res;
  
 error:
  if(init_json_obj)
    json_object_put(init_json_obj);
  return db_res;  
}

#define RET_AMQP_ERROR 2

int dbk_credentials_query(const db1_con_t* _h, const db_key_t* _k,
                          const db_val_t* _v, const db_key_t* _c, int _n, int _nc, db1_res_t** _r)
{
  rmq_conn_t* rmq = (rmq_conn_t*)_h->tail;
  static char routingkey[256];
  static char messagebody[2024];
  static char serverid[256];
  amqp_bytes_t amqp_rk;
  amqp_bytes_t amqp_mb;
  amqp_bytes_t rpl_queue= {0, 0};
  str unique_string = {0, 0};
  int ret = -1;

  if (!rmq->conn && rmqp_open_connection(rmq) < 0) {
    LM_WARN("disconnected from AMQP, dropping authn request\n"); 
    return ret;
  }

  LM_DBG("Query creds: %d rmq(%p)->conn(%p) %d\n", getpid(), (void *)rmq, rmq->conn, rmq->channel);
  
  /* routingkey = authn.req._auth_realm_ */
  amqp_rk.bytes = routingkey;
  memcpy(routingkey, AUTH_ROUTING_KEY_PREFIX, AUTH_ROUTING_KEY_PREFIX_LEN);
  amqp_rk.len = amqp_util_encode(&_v[1].val.str_val, routingkey+AUTH_ROUTING_KEY_PREFIX_LEN) - routingkey;
  if (amqp_rk.len < AUTH_ROUTING_KEY_PREFIX_LEN + _v[1].val.str_val.len) {
    LM_ERR("Encoding didn't succeed %.*s\n", (int)amqp_rk.len, (char*)amqp_rk.bytes);
    return ret;
  }
  
  tmb.generate_callid(&unique_string);

  sprintf(serverid, "kamailio@%.*s-<%d>-%lu", _v[1].val.str_val.len, _v[1].val.str_val.s, my_pid(), rpl_routing_key_count++);
  
  /* construct messagebody */
  amqp_mb.len = sprintf(messagebody, "{\"Method\":\"REGISTER\","
                        "\"Auth-Realm\":\"%.*s\","
                        "\"Auth-User\":\"%.*s\","
                        "\"From\":\"%.*s@%.*s\","
                        "\"To\":\"%.*s@%.*s\","
                        "\"Server-ID\":\"%s\","
                        "\"Node\":\"kamailio@%.*s\","
                        "\"Msg-ID\":\"%.*s\","
                        "\"App-Version\":\"%s\","
                        "\"App-Name\":\"%s\","
                        "\"Event-Name\":\"authn_req\","
                        "\"Event-Category\":\"directory\"}",
                        _v[1].val.str_val.len, _v[1].val.str_val.s,
                        _v[0].val.str_val.len, _v[0].val.str_val.s,
                        _v[0].val.str_val.len, _v[0].val.str_val.s,
                        _v[1].val.str_val.len, _v[1].val.str_val.s,
                        _v[0].val.str_val.len, _v[0].val.str_val.s,
                        _v[1].val.str_val.len, _v[1].val.str_val.s,
                        serverid,
                        dbk_node_hostname.len, dbk_node_hostname.s,
                        unique_string.len, unique_string.s,
                        VERSION, NAME);
  amqp_mb.bytes = messagebody;
  
  LM_DBG("AMQP: exchange %.*s\n", (int)rmq->exchange.len, (char*)rmq->exchange.bytes);
  LM_DBG("AMQP: channel %d\n", rmq->channel);
  LM_DBG("AMQP: routing key %.*s\n", (int)amqp_rk.len, (char*)amqp_rk.bytes);
  LM_DBG("AMQP: body: %s\n", messagebody);
  
  /* Declare reply queue and start consumer */
  amqp_bytes_t rpl_routing_key= {strlen(serverid), serverid};
  
  LM_DBG("AMQP: before queue_declare rpl_routing_key: [%.*s]\n", (int)rpl_routing_key.len, (char*)rpl_routing_key.bytes);
  amqp_queue_declare_ok_t *r = amqp_queue_declare(rmq->conn, rmq->channel, amqp_empty_bytes, 0, 0, 1, 1,
								 amqp_empty_table);
  if (rmq_error("Declaring queue", amqp_get_rpc_reply(rmq->conn))) {
    ret = -RET_AMQP_ERROR;
    goto error;
  }
  
  rpl_queue = amqp_bytes_malloc_dup(r->queue);
  if (rpl_queue.bytes == NULL) {
    LM_ERR("Out of memory while copying queue name\n");
    goto error;
  }
  
  amqp_bytes_t rpl_exch = {8, "targeted"};
  if ( amqp_queue_bind(rmq->conn, rmq->channel,
                       rpl_queue, rpl_exch, rpl_routing_key, amqp_empty_table) < 0 || 
       rmq_error("Binding queue", amqp_get_rpc_reply(rmq->conn))) {
    ret = -RET_AMQP_ERROR;
    goto error;
  }
  
  if ( amqp_basic_consume(rmq->conn, rmq->channel, rpl_queue, amqp_empty_bytes, 0, 1, 1, amqp_empty_table) < 0 ||
       rmq_error("Consuming", amqp_get_rpc_reply(rmq->conn))) {
    ret = -RET_AMQP_ERROR;
    goto error;
  }
  
  LM_DBG("AMQP: after queue_bind  rpl_routing_key: [%.*s]\n", (int)rpl_routing_key.len, (char*)rpl_routing_key.bytes);
  LM_DBG("AMQP: after queue_bind  server_id: [%s]\n", serverid);

  /* publish  */
  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(amqp_basic_properties_t));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");
  
  if ( amqp_basic_publish(rmq->conn,
                          rmq->channel,
                          rmq->exchange,
                          amqp_rk,
                          0,
                          0,
                          &props,
                          amqp_mb) < 0 || 
       rmq_error("Publishing",  amqp_get_rpc_reply(rmq->conn)) ) {
    LM_WARN("Failed to publish to AMQP, dropping authn request\n"); 
    rmq_close(rmq);
    ret = -RET_AMQP_ERROR;
    goto error;
  }
  
  LM_DBG("Published with success\n");
  
  amqp_frame_t frame;
  int result;
  
  amqp_basic_deliver_t *d;
  amqp_basic_properties_t *p;
  size_t body_target;
  size_t body_received = 0;
  char body[2048];
  
  while (1) {
    amqp_maybe_release_buffers(rmq->conn);
    if (dbk_rmq_wait_for_data(rmq->conn) < 0 ) {
      LM_ERR("RMQ data not ready - give up\n");
      ret = -RET_AMQP_ERROR;
      goto error;
    }
    result = amqp_simple_wait_frame(rmq->conn, &frame);
    LM_DBG("Result: %d\n", result);
    if (result < 0) {
      rmq_close(rmq);
      ret = -RET_AMQP_ERROR;
      goto error;
    }
    
    LM_DBG("Frame type: %d channel: %d\n", frame.frame_type, frame.channel);
    if (frame.frame_type != AMQP_FRAME_METHOD) {
      continue;
    }
    
    LM_DBG("Method: %s\n", amqp_method_name(frame.payload.method.id));
    if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
      continue;
    }
    
    d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
    LM_DBG("Delivery: %u exchange: %.*s routingkey: %.*s\n",
           (unsigned) d->delivery_tag,
           (int) d->exchange.len, (char *) d->exchange.bytes,
           (int) d->routing_key.len, (char *) d->routing_key.bytes);
    
    if (dbk_rmq_wait_for_data(rmq->conn) < 0 ) {
      LM_ERR("RMQ data not ready - give up\n");
      ret = -RET_AMQP_ERROR;
      goto error;
    }
    result = amqp_simple_wait_frame(rmq->conn, &frame);
    if (result < 0) {
      rmq_close(rmq);
      ret = -RET_AMQP_ERROR;
      goto error;
    }
    
    if (frame.frame_type != AMQP_FRAME_HEADER) {
      LM_ERR("amqp: Expected header!");
      goto error;
    }
    p = (amqp_basic_properties_t *) frame.payload.properties.decoded;
    if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
      LM_DBG("Content-type: %.*s\n",
             (int) p->content_type.len, (char *) p->content_type.bytes);
    }
    
    body_target = frame.payload.properties.body_size;
    body_received = 0;
    
    while (body_received < body_target) {
      if (dbk_rmq_wait_for_data(rmq->conn) < 0 ) {
        LM_ERR("RMQ data not ready - give up\n");
        ret = -RET_AMQP_ERROR;
        goto error;
      }
      result = amqp_simple_wait_frame(rmq->conn, &frame);
      if (result < 0) {
        rmq_close(rmq);
        ret = -RET_AMQP_ERROR;
        goto error;
      }
      
      if (frame.frame_type != AMQP_FRAME_BODY) {
        LM_ERR("amqp: Expected header!");
        goto error;
      }
      
      memcpy(body + body_received, frame.payload.body_fragment.bytes,
             frame.payload.body_fragment.len);
      
      body_received += frame.payload.body_fragment.len;
      
      LM_DBG("%.*s\n", (int)frame.payload.body_fragment.len,
             (char*)frame.payload.body_fragment.bytes);
    }
    
    if (body_received != body_target) {
      LM_CRIT("Unexpected body size: recv %zu, expected %zu\n",
              body_received, body_target);
      goto error;
    }

    /* everything was fine, we can quit now because we received the reply */
    break;
  }
  
  body[body_received] = '\0';
  LM_DBG("Received reply %s\n", body);
  
  /* parse json and extract password */
  db1_res_t* db_res = dbk_creds_build_result(body, _c, _nc);	
  *_r = db_res;
  ret = 0;

 error:
  if(rpl_queue.bytes) {
    /*
    amqp_queue_purge(rmq->conn, rmq->channel, rpl_queue);
    if (rmq_error("Purging reply queue", amqp_get_rpc_reply(rmq->conn))) {
      LM_ERR("Failed to purge reply queue\n");
    }    
    LM_DBG("rpl_queue [%.*s], rpl_exchange [%.*s], rpl_routing_key [%.*s]\n",
           (int)rpl_queue.len, (char*)rpl_queue.bytes, 
           (int)rpl_exch.len, (char*)rpl_exch.bytes, 
           (int)rpl_routing_key.len, (char*)rpl_routing_key.bytes 
           );
    amqp_queue_unbind(rmq->conn, rmq->channel, rpl_queue, rpl_exch,  rpl_routing_key, amqp_empty_table);	
    if (rmq_error("Unbinding reply queue", amqp_get_rpc_reply(rmq->conn))) {
      LM_ERR("Failed to unbind queue\n");
    }
    */
    amqp_queue_delete(rmq->conn, rmq->channel, rpl_queue, 0, 0);
    if (rmq_error("Deleting reply queue", amqp_get_rpc_reply(rmq->conn))) {
      LM_ERR("Failed to delete queue\n");
    }
    LM_DBG("rpl_queue [%.*s]\n", (int)rpl_queue.len, (char*)rpl_queue.bytes);
    
    amqp_bytes_free(rpl_queue);
  }
  
  return ret;
}

db1_res_t* db_empty_result(void)
{
  db1_res_t* db_res = db_new_result();
  if (!db_res) {
    LM_ERR("no memory left\n");
    return NULL;
  }
  RES_ROW_N(db_res) = 0;
  return db_res;
}

/*
 * Query table for specified rows
 * _h: structure representing database connection
 * _k: key names
 * _op: operators
 * _v: values of the keys that must match
 * _c: column names to return
 * _n: number of key=values pairs to compare
 * _nc: number of columns to return
 * _o: order by the specified column
 */
int db_kazoo_query(const db1_con_t* _h, const db_key_t* _k, const db_op_t* _op,
                   const db_val_t* _v, const db_key_t* _c, int _n, int _nc,
                   const db_key_t _o, db1_res_t** _r)
{
  if (!_h || !CON_TABLE(_h) || !_r) {
    LM_ERR("invalid parameter value\n");
    return -1;
  }
  LM_DBG("query table=%s\n", _h->table->s);
  
  if(strncmp(_h->table->s, "subscriber", 10)== 0 ) {
    return dbk_credentials_query(_h, _k, _v, _c, _n, _nc, _r);
  } else if(strncmp(_h->table->s, "presentity", 10)== 0 ) {
    return dbk_presence_query(_h, _k, _v, _c, _n, _nc, _r);
  } else {
    LM_DBG("Not supported\n");
    *_r = db_empty_result();
    return 0;
  }
}

int amqp_register_notice(const db1_con_t* _h, const db_key_t* _k,
                         const db_val_t* _v, const int _n) {  
  rmq_conn_t* rmq = (rmq_conn_t*)_h->tail;
  static char routingkey[256];
  static char messagebody[2024];
  static char fspath_buf[64];
  static char recv_buf[64];
  amqp_bytes_t amqp_rk;
  amqp_bytes_t amqp_mb;
  int col;
  str unique_string;
  str fs_path;
  
  str user= {0,0};
  str contact = {0, 0};
  int expires = 0;
  
  str callid = {0, 0};
  str user_agent = {0, 0};
  str network_ip = {0, 0};
  str network_port = {0, 0};
  str network_proto = {0, 0};
  str host = {0, 0};
  str received = {0, 0};

  if (!rmq->conn && rmqp_open_connection(rmq) < 0) {
    LM_WARN("disconnected from AMQP, dropping register success\n"); 
    goto error;
  }

  /* construct messagebody */  
  for (col = 0; col < _n; col ++) {
    if (!user.len && strncmp(_k[col]->s, "username", 8) == 0) {
      user = _v[col].val.str_val;
    } else if (!contact.len && strncmp(_k[col]->s, "contact", 7) == 0) {
      contact = _v[col].val.str_val;
    } else if (!expires && strncmp(_k[col]->s, "expires", 7) == 0) {
      expires= _v[col].val.int_val - (int)time(NULL);
    } else if (!callid.len && strncmp(_k[col]->s, "callid", 6) == 0) {
      callid = _v[col].val.str_val;
    } else if (!user_agent.len && strncmp(_k[col]->s, "user_agent", 10) == 0) {
      user_agent = _v[col].val.str_val;
    } else if (!host.len && strncmp(_k[col]->s, "domain", 6) == 0) {
      host = _v[col].val.str_val;
    } else if (!network_ip.len && strncmp(_k[col]->s, "socket", 6) == 0) {
      if (!_v[col].nul) {	
        char* dp = memchr( _v[col].val.str_val.s, ':',  _v[col].val.str_val.len);
        if (dp == NULL) {
          LM_ERR("Wrong formated socket uri %.*s\n",
                 _v[col].val.str_val.len, _v[col].val.str_val.s);
          goto error;
        }		
        network_proto.s = _v[col].val.str_val.s;	
        network_proto.len = dp - network_proto.s;	
	
        dp++;
        network_ip.s = dp;	
        dp = memchr(dp, ':',  _v[col].val.str_val.len- network_proto.len -1);
        if (dp == NULL) {
          LM_ERR("Wrong formated socket uri %.*s\n",
                 _v[col].val.str_val.len, _v[col].val.str_val.s);
          goto error;
        }		
        network_ip.len = dp - network_ip.s;
        
        network_port.s = dp + 1;
        network_port.len = _v[col].val.str_val.s + _v[col].val.str_val.len - network_port.s;
      } else {
        LM_DBG("Null socket -> can not get the network IP and port\n");
      }
    } else if (!received.len && strncmp(_k[col]->s, "received", 8) == 0) {
      if (!_v[col].nul) {
        received = _v[col].val.str_val;
      } else {
        LM_DBG("NULL received - construct from contact\n");
      }
    }
  }
  
  if (received.len == 0) {
    struct sip_uri uri;
    if (parse_uri(contact.s, contact.len, &uri) < 0) {
      LM_ERR("Failed to parse contact URI\n");
      goto error;
    }
    if (uri.user.len == 0) {
      received = contact;
    } else {
      str port = uri.port;
      if (port.len == 0) {
        port.s = "5060";
        port.len = 4;
      }
      received.s = recv_buf;
      received.len = sprintf(received.s, "sip:%.*s:%.*s",
                             uri.host.len, uri.host.s, port.len, port.s);
    }
    LM_DBG("Constructed received [%.*s]\n", received.len, received.s);
  }
  
  LM_DBG("user = [%.*s]\n", user.len, user.s);
  LM_DBG("host = [%.*s]\n", host.len, host.s);
  LM_DBG("contact = [%.*s]\n", contact.len, contact.s);
  LM_DBG("received = [%.*s]\n", received.len, received.s);
  
  /* take fs_path either from the module parameter or construct is as
   * IP:PORT where the message was received */	
  if (dbk_reg_fs_path.s) {
    fs_path = dbk_reg_fs_path;
  } else {
    fs_path.s = fspath_buf;
    fs_path.len = sprintf(fs_path.s, "%.*s:%.*s", network_ip.len, network_ip.s,
                          network_port.len, network_port.s );
  }
  LM_DBG("fs_path = [%.*s]\n", fs_path.len, fs_path.s);
  
  /*
   * {"Network-Port":"5065","Network-IP":"178.79.172.28","FreeSWITCH-Nodename":"freeswitch@kazoo2.anca-kazoo.com","FreeSWITCH-Hostname":"kazoo2.anca-kazoo.com","RPid":"unknown","To-Host":"sip.anca-kazoo.com","To-User":"twinkle","From-Host":"sip.anca-kazoo.com","From-User":"twinkle","Presence-Hosts":"n/a","Profile-Name":"sipinterface_1","Call-ID":"msbmplhjhlbrtrg@anca-vaio","User-Agent":"Twinkle/1.4.2","Status":"Registered(UDP)","Realm":"sip.anca-kazoo.com","Username":"twinkle","Expires":"3600","Contact":"\"anca\" <sip:twinkle@109.103.86.242:62038;fs_path=<sip:178.79.172.28:5065;lr;received='sip:109.103.86.242:62038'>>","Event-Timestamp":63534134448,"Server-ID":"","Node":"ecallmgr@kazoo2.anca-kazoo.com","Msg-ID":"75535250c50683a1056811c0931c4915","App-Version":"0.8.0","App-Name":"ecallmgr","Event-Name":"reg_success","Event-Category":"directory"}
   *
   *
   */
  tmb.generate_callid(&unique_string);
  
  amqp_mb.len = sprintf(messagebody, "{\"Network-Port\":\"%.*s\","
                        "\"Network-IP\":\"%.*s\","
                        "\"Kamailio-Hostname\":\"%.*s\","
                        "\"RPid\":\"unknown\","
                        "\"To-Host\":\"%.*s\","
                        "\"To-User\":\"%.*s\","
                        "\"From-Host\":\"%.*s\","
                        "\"From-User\":\"%.*s\","
                        "\"Presence-Hosts\":\"n/a\","
                        "\"Profile-Name\":\"sipinterface_1\","
                        "\"Call-ID\":\"%.*s\","
                        "\"User-Agent\":\"%.*s\","
                        "\"Status\":\"Registered(%.*s)\","
                        "\"Realm\":\"%.*s\","
                        "\"Username\":\"%.*s\","
                        "\"Expires\":\"%d\","
                        "\"Contact\":\"\\\"%.*s\\\" <sip:%.*s@%.*s;fs_path=<sip:%.*s;lr;received='%.*s'>>\","
                        "\"Event-Timestamp\":\"%ld\","
                        "\"Server-ID\":\"\","
                        "\"Node\":\"kamailio@%.*s\","
                        "\"Msg-ID\":\"%.*s\","
                        "\"App-Version\":\"%s\","
                        "\"App-Name\":\"%s\","
                        "\"Event-Name\":\"reg_success\","
                        "\"Event-Category\":\"directory\"} ",
                        network_port.len, network_port.s,
                        network_ip.len, network_ip.s,
                        dbk_node_hostname.len, dbk_node_hostname.s,
                        host.len, host.s,
                        user.len, user.s,
                        host.len, host.s,
                        user.len, user.s,
                        callid.len, callid.s,
                        user_agent.len, user_agent.s,
                        network_proto.len, network_proto.s,
                        host.len, host.s,
                        user.len, user.s,
                        expires,
                        user.len, user.s,
                        user.len, user.s, received.len-4, received.s+4,
                        fs_path.len, fs_path.s,
                        //network_ip.len, network_ip.s, network_port.len, network_port.s,
                        received.len, received.s,
                        62167219200 + (int)time(NULL),
                        dbk_node_hostname.len, dbk_node_hostname.s,
                        unique_string.len, unique_string.s,
                        VERSION, NAME );
  amqp_mb.bytes = messagebody;
  amqp_rk.bytes = routingkey;
  memcpy(routingkey, REG_ROUTING_KEY_PREFIX, REG_ROUTING_KEY_PREFIX_LEN);
  amqp_rk.len = amqp_util_encode(&host, routingkey+REG_ROUTING_KEY_PREFIX_LEN) - routingkey;
  routingkey[amqp_rk.len++]='.';
  amqp_rk.len = amqp_util_encode(&user, routingkey+amqp_rk.len) - routingkey;
  if (amqp_rk.len < REG_ROUTING_KEY_PREFIX_LEN + host.len + user.len) {
    LM_ERR("Encoding didn't succeed %.*s\n", (int)amqp_rk.len, (char*)amqp_rk.bytes);
    return -1;
  }
  
  LM_DBG("AMQP: exchange [%.*s]\n", (int)rmq->exchange.len, (char*)rmq->exchange.bytes);
  LM_DBG("AMQP: channel %d\n", rmq->channel);
  LM_DBG("AMQP: routing key [%.*s]\n", (int)amqp_rk.len, (char*)amqp_rk.bytes);
  LM_DBG("AMQP: body: %s\n", messagebody);
  
  /* publish */
  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(amqp_basic_properties_t));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");

  if (amqp_basic_publish(rmq->conn,
                         rmq->channel,
                         rmq->exchange,
                         amqp_rk,
                         0,
                         0,
                         &props,
                         amqp_mb) < 0) {
    LM_WARN("Failed to publish to AMQP, dropping registration success\n");
    rmq_close(rmq);
    goto error;
  }

  LM_DBG("Published with success\n");

  return 0;
  
 error:
  return -1;
}

int db_kazoo_insert (const db1_con_t* _h, const db_key_t* _k,
                     const db_val_t* _v, const int _n)
{
  if (!_h || !CON_TABLE(_h) ) {
    LM_ERR("invalid parameter value\n");
    return -1;
  }
  LM_DBG("insert into table=%s\n", _h->table->s);
  
  if(strncmp(_h->table->s, "location", _h->table->len)  == 0) {
    return amqp_register_notice(_h, _k, _v, _n);
  } else if(strncmp(_h->table->s, "active_watchers", _h->table->len)  == 0) {
    return dbk_presence_subscribe_new(_h, _k, _v, _n);
  } else {
    LM_DBG("Not supported\n");
    return 0;
  }
}


int db_kazoo_insert_update (const db1_con_t* _h, const db_key_t* _k,
                            const db_val_t* _v, const int _n)
{
  if (!_h || !CON_TABLE(_h) ) {
    LM_ERR("invalid parameter value\n");
    return -1;
  }
  LM_DBG("insert into table=%s\n", _h->table->s);
  
  if(strncmp(_h->table->s, "dialoginfo", _h->table->len)  == 0) {
    LM_DBG("Insert update called for dialoginfo table\n");
    return dbk_dialoginfo_update(_h, _k, _v, _n);
  } else {
    LM_DBG("Not supported\n");
    return 0;
  }
}

int db_kazoo_update (const db1_con_t* _h, const db_key_t* _k, const db_op_t* _o,
                     const db_val_t* _v, const db_key_t* _uk, const db_val_t* _uv,
                     const int _n, const int _un) {
  if(strncmp(_h->table->s, "active_watchers", _h->table->len)  == 0) {
    LM_DBG("Update called for active_watchers table\n");
    return dbk_presence_subscribe_update(_h, _k, _v, _uk, _uv, _n, _un);
  } else {
    LM_DBG("Not supported\n");
    return 0;
  }
}

int dbk_rmq_wait_for_data(amqp_connection_state_t conn)
{
  struct timeval timeout;
  int sock;
  fd_set read_flags;
  int ret;
  
  if (amqp_data_in_buffer(conn)  || amqp_frames_enqueued(conn) ) {
    return 0;
  }
  
  sock = amqp_get_sockfd(conn);
  FD_ZERO(&read_flags);
  FD_SET(sock, &read_flags);
  
  timeout.tv_sec = dbk_auth_wait_timeout;
  timeout.tv_usec = 0;
  
  ret = select(sock+1, &read_flags, NULL, NULL, &timeout);
  if (ret == -1) {
    LM_ERR("select: %s\n", strerror(errno));
    return -1;
  }
  else if (ret == 0) {
    LM_INFO("select: timeout\n");
    return -1;
  }
  if (FD_ISSET(sock, &read_flags)) {
    LM_INFO("select: data received\n");
  }
  
  return 0;
}
