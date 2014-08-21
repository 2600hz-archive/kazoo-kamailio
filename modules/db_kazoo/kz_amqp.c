#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <json/json.h>
#include "../../mem/mem.h"
#include "../../timer_proc.h"
#include "../../sr_module.h"
#include "../../pvar.h"
#include "../../mod_fix.h"
#include "../../lvalue.h"

#include "kz_amqp.h"

#define RET_AMQP_ERROR 2


kz_amqp_conn_pool_ptr kz_pool = NULL;
static unsigned long rpl_query_routing_key_count = 0;

typedef struct json_object *json_obj_ptr;

kz_amqp_channel_ptr channels = NULL;
int channel_index = 0;
extern int *kz_pipe_fds;

extern struct timeval kz_sock_tv;
extern struct timeval kz_amqp_tv;

static char *kz_amqp_str_dup(str *src)
{
	char *res;

	if (!src || !src->s)
		return NULL;
	if (!(res = (char *) shm_malloc(src->len + 1)))
		return NULL;
	strncpy(res, src->s, src->len);
	res[src->len] = 0;
	return res;
}

static char *kz_amqp_string_dup(char *src)
{
	char *res;
	int sz;
	if (!src )
		return NULL;

	sz = strlen(src);
	if (!(res = (char *) shm_malloc(sz + 1)))
		return NULL;
	strncpy(res, src, sz);
	res[sz] = 0;
	return res;
}

/*
static void kz_amqp_bytes_free(amqp_bytes_t *bytes)
{
  free(bytes->bytes);
  free(bytes);
}

static amqp_bytes_t* kz_amqp_bytes_ptr_dup_from_string(char *src)
{
	int sz = strlen(src);
	amqp_bytes_t* result = (amqp_bytes_t*) malloc(sizeof(amqp_bytes_t));
	memset(result, 0, sizeof(amqp_bytes_t));
	result->bytes = malloc(sz);
	if (result->bytes != NULL) {
		memcpy(result->bytes, src, sz);
		result->len = sz;
	}
	return result;
}
*/
static amqp_bytes_t kz_amqp_bytes_dup_from_string(char *src)
{
	return amqp_bytes_malloc_dup(amqp_cstring_bytes(src));
}

static amqp_bytes_t kz_amqp_bytes_dup_from_str(str *src)
{
	return amqp_bytes_malloc_dup(amqp_cstring_bytes(src->s));
}

void kz_amqp_free_reply(kz_amqp_reply_ptr reply)
{
	if(reply == NULL)
		return;
	if(reply->exchange.bytes)
		amqp_bytes_free(reply->exchange);
	if(reply->queue.bytes)
		amqp_bytes_free(reply->queue);
	if(reply->routing_key.bytes)
		amqp_bytes_free(reply->routing_key);
	shm_free(reply);
}

void kz_amqp_free_pipe_cmd(kz_amqp_cmd_ptr cmd)
{
	if(cmd == NULL)
		return;
	if (cmd->exchange)
		shm_free(cmd->exchange);
	if (cmd->exchange_type)
		shm_free(cmd->exchange_type);
	if (cmd->queue)
		shm_free(cmd->queue);
	if (cmd->routing_key)
		shm_free(cmd->routing_key);
	if (cmd->reply_routing_key)
		shm_free(cmd->reply_routing_key);
	if (cmd->payload)
		shm_free(cmd->payload);
	if (cmd->return_payload)
		shm_free(cmd->return_payload);
	lock_release(&cmd->lock);
	lock_destroy(&cmd->lock);
	shm_free(cmd);
}

void kz_amqp_init_pool() {
	if(kz_pool == NULL) {
		kz_pool = (kz_amqp_conn_pool_ptr) shm_malloc(sizeof(kz_amqp_conn_pool));
		memset(kz_pool, 0, sizeof(kz_amqp_conn_pool));
		//lock_init(&kz_pool->lock);
	}
}

int kz_amqp_add_connection(modparam_t type, void* val)
{
	kz_amqp_init_pool(); // find a better way
//	lock_get(&kz_pool->lock);

	kz_amqp_conn_ptr newConn = shm_malloc(sizeof(kz_amqp_conn));
	memset(newConn, 0, sizeof(kz_amqp_conn));
//	lock_init(&newConn->lock);

	if(kz_pool->head == NULL)
		kz_pool->head = newConn;

	if(kz_pool->tail != NULL)
		kz_pool->tail->next = newConn;

	kz_pool->tail = newConn;

    amqp_parse_url((char*)val, &newConn->info);

//   	lock_release(&kz_pool->lock);

	return 0;
}

void kz_amqp_connection_close(kz_amqp_conn_ptr rmq) {
    LM_INFO("Close rmq connection\n");
    if (!rmq)
    	return;

    if (rmq->conn) {
		LM_INFO("close connection:  %d rmq(%p)->conn(%p)\n", getpid(), (void *)rmq, rmq->conn);
		rmq_error("closing connection", amqp_connection_close(rmq->conn, AMQP_REPLY_SUCCESS));
		if (amqp_destroy_connection(rmq->conn) < 0) {
			LM_ERR("cannot destroy connection\n");
		}
		rmq->conn = NULL;
		rmq->socket = NULL;
		rmq->channel_count = 0;

//	   	lock_release(&kz_pool->lock);

    }

}

void kz_amqp_channel_close(kz_amqp_conn_ptr rmq, amqp_channel_t channel) {
    LM_INFO("Close rmq channel\n");
    if (!rmq)
    	return;

	LM_INFO("close channel: %d rmq(%p)->channel(%d)\n", getpid(), (void *)rmq, channel);
	rmq_error("closing channel", amqp_channel_close(rmq->conn, channel, AMQP_REPLY_SUCCESS));
}

int kz_amqp_connection_open(kz_amqp_conn_ptr rmq) {
	rmq->channel_count = rmq->channel_counter = 0;
    if (!(rmq->conn = amqp_new_connection())) {
    	LM_INFO("Failed to create new AMQP connection\n");
    	goto error;
    }

    rmq->socket = amqp_tcp_socket_new(rmq->conn);
    if (!rmq->socket) {
    	LM_INFO("Failed to create TCP socket to AMQP broker\n");
    	goto error;
    }

    if (amqp_socket_open(rmq->socket, rmq->info.host, rmq->info.port)) {
    	LM_INFO("Failed to open TCP socket to AMQP broker\n");
    	goto error;
    }

    if (rmq_error("Logging in", amqp_login(rmq->conn,
					   "/", //rmq->info.vhost,
					   0,
					   131072,
					   0,
					   AMQP_SASL_METHOD_PLAIN,
					   rmq->info.user,
					   rmq->info.password))) {

    	LM_ERR("Login to AMQP broker failed!\n");
    	goto error;
    }

    return 0;

 error:
    kz_amqp_connection_close(rmq);
    return -1;
}

int kz_amqp_channel_open(kz_amqp_conn_ptr rmq, amqp_channel_t channel) {
	if(rmq == NULL) {
		LM_INFO("rmq == NULL \n");
		return -1;
	}

    amqp_channel_open(rmq->conn, channel);
    if (rmq_error("Opening channel", amqp_get_rpc_reply(rmq->conn))) {
    	LM_ERR("Failed to open channel AMQP %d!\n", channel);
    	return -1;
    }

    return 0;
}

kz_amqp_conn_ptr kz_amqp_get_connection() {
	kz_amqp_conn_ptr ptr = NULL;
	if(kz_pool == NULL) {
		return NULL;
	}
//	lock_get(&kz_pool->lock);

	ptr = kz_pool->head;

	if(kz_pool->current != NULL) {
		ptr = kz_pool->current;
	}

	if(ptr->socket == NULL )
	{
	while(ptr != NULL) {
		if(kz_amqp_connection_open(ptr) == 0) {
			kz_pool->current = ptr;
			break;
		}
		ptr = ptr->next;
	}
	}

//	lock_release(&kz_pool->lock);

   	return ptr;
}

kz_amqp_conn_ptr kz_amqp_get_next_connection() {
	kz_amqp_conn_ptr ptr = NULL;
	if(kz_pool == NULL) {
		return NULL;
	}

	if(kz_pool->current != NULL) {
		ptr = kz_pool->current->next;
	}

	if(ptr == NULL) {
		ptr = kz_pool->head;
	}

	while(ptr != NULL) {
		if(kz_amqp_connection_open(ptr) == 0) {
			kz_pool->current = ptr;
			break;
		}
		ptr = ptr->next;
	}


   	return ptr;
}

int kz_amqp_consume_error(amqp_connection_state_t conn)
{
	amqp_frame_t frame;
	int ret = 0;
	amqp_rpc_reply_t reply;

	if (AMQP_STATUS_OK != amqp_simple_wait_frame_noblock(conn, &frame, &kz_amqp_tv)) {
		// should i ignore this or close the connection?
		LM_ERR("ERROR ON SIMPLE_WAIT_FRAME\n");
		return ret;
	}

	if (AMQP_FRAME_METHOD == frame.frame_type) {
		switch (frame.payload.method.id) {
		case AMQP_BASIC_ACK_METHOD:
			/* if we've turned publisher confirms on, and we've published a message
			 * here is a message being confirmed
			 */
			ret = 1;
			break;
		case AMQP_BASIC_RETURN_METHOD:
			/* if a published message couldn't be routed and the mandatory flag was set
			 * this is what would be returned. The message then needs to be read.
			 */
			{
				ret = 1;
			amqp_message_t message;
			reply = amqp_read_message(conn, frame.channel, &message, 0);
			if (AMQP_RESPONSE_NORMAL != reply.reply_type) {
				LM_ERR("AMQP_BASIC_RETURN_METHOD read_message\n");
				break;
			}

			LM_INFO("Received this message : %.*s\n", (int) message.body.len, (char*)message.body.bytes);
			amqp_destroy_message(&message);
			}
			break;

		case AMQP_CHANNEL_CLOSE_METHOD:
			/* a channel.close method happens when a channel exception occurs, this
			 * can happen by publishing to an exchange that doesn't exist for example
			 *
			 * In this case you would need to open another channel redeclare any queues
			 * that were declared auto-delete, and restart any consumers that were attached
			 * to the previous channel
			 */
			LM_ERR("AMQP_CHANNEL_CLOSE_METHOD\n");
			if(frame.channel > 0)
				channels[frame.channel-1].state = KZ_AMQP_CLOSED;
			break;

		case AMQP_CONNECTION_CLOSE_METHOD:
			/* a connection.close method happens when a connection exception occurs,
			 * this can happen by trying to use a channel that isn't open for example.
			 *
			 * In this case the whole connection must be restarted.
			 */
			break;

		default:
			LM_ERR("An unexpected method was received %d\n", frame.payload.method.id);
			break;
		}
	};

	return ret;
}

void kz_amqp_add_payload_common_properties(json_obj_ptr json_obj, char* server_id, str* unique) {
    char node_name[512];


    json_object_object_add(json_obj, BLF_JSON_APP_NAME,
			   json_object_new_string(NAME));
    json_object_object_add(json_obj, BLF_JSON_APP_VERSION,
			   json_object_new_string(VERSION));
    sprintf(node_name, "kamailio@%.*s", dbk_node_hostname.len, dbk_node_hostname.s);
    json_object_object_add(json_obj, BLF_JSON_NODE,
			   json_object_new_string(node_name));
    json_object_object_add(json_obj, BLF_JSON_SERVERID,
			   json_object_new_string(server_id));
    json_object_object_add(json_obj, BLF_JSON_MSG_ID,
			   json_object_new_string_len(unique->s, unique->len));

}

int kz_amqp_pipe_send(str *str_exchange, str *str_routing_key, str *str_payload)
{
	int ret = 1;
    json_obj_ptr json_obj = NULL;
    kz_amqp_cmd_ptr cmd = NULL;

    str unique_string = { 0, 0 };
    char serverid[512];

    tmb.generate_callid(&unique_string);
    sprintf(serverid, "kamailio@%.*s-<%d>-script-%lu", dbk_node_hostname.len, dbk_node_hostname.s, my_pid(), rpl_query_routing_key_count++);


    /* parse json  and add extra fields */
    json_obj = json_tokener_parse(str_payload->s);
    if (is_error(json_obj))
    {
		LM_ERR("Error parsing json: %s\n",json_tokener_errors[-(unsigned long)json_obj]);
		LM_ERR("%s\n", str_payload->s);
		goto error;
    }

    kz_amqp_add_payload_common_properties(json_obj, serverid, &unique_string);

    char *payload = (char *)json_object_to_json_string(json_obj);

	cmd = (kz_amqp_cmd_ptr)shm_malloc(sizeof(kz_amqp_cmd));
	if(cmd == NULL) {
		LM_ERR("failed to allocate kz_amqp_cmd in process %d\n", getpid());
		goto error;
	}
	memset(cmd, 0, sizeof(kz_amqp_cmd));
	cmd->exchange = kz_amqp_str_dup(str_exchange);
	cmd->routing_key = kz_amqp_str_dup(str_routing_key);
	cmd->payload = kz_amqp_string_dup(payload);
	if(cmd->payload == NULL || cmd->routing_key == NULL || cmd->exchange == NULL) {
		LM_ERR("failed to allocate kz_amqp_cmd parameters in process %d\n", getpid());
		goto error;
	}
	if(lock_init(&cmd->lock)==NULL)
	{
		LM_ERR("cannot init the lock for publishing in process %d\n", getpid());
		lock_dealloc(&cmd->lock);
		goto error;
	}
	lock_get(&cmd->lock);
	cmd->type = KZ_AMQP_PUBLISH;
	cmd->consumer = getpid();
	if (write(kz_pipe_fds[1], &cmd, sizeof(cmd)) != sizeof(cmd)) {
		LM_ERR("failed to publish message to amqp in process %d, write to command pipe: %s\n", getpid(), strerror(errno));
	} else {
		lock_get(&cmd->lock);
		ret = 1;//cmd->return_code;
	}

	error:

	if(cmd)
		kz_amqp_free_pipe_cmd(cmd);

    if(json_obj)
    	json_object_put(json_obj);

	return ret;
}

int kz_amqp_pipe_send_receive(str *str_exchange, str *str_routing_key, str *str_payload, json_obj_ptr* json_ret )
{
	int ret = 1;
    json_obj_ptr json_obj = NULL;
    kz_amqp_cmd_ptr cmd = NULL;
    json_obj_ptr json_body = NULL;

    str unique_string = { 0, 0 };
    char serverid[512];

    tmb.generate_callid(&unique_string);
    sprintf(serverid, "kamailio@%.*s-<%d>-script-%lu", dbk_node_hostname.len, dbk_node_hostname.s, my_pid(), rpl_query_routing_key_count++);


    /* parse json  and add extra fields */
    json_obj = json_tokener_parse(str_payload->s);
    if (is_error(json_obj))
    {
		LM_ERR("Error parsing json: %s\n",json_tokener_errors[-(unsigned long)json_obj]);
		LM_ERR("%s\n", str_payload->s);
		goto error;
    }

    kz_amqp_add_payload_common_properties(json_obj, serverid, &unique_string);

    char *payload = (char *)json_object_to_json_string(json_obj);

	cmd = (kz_amqp_cmd_ptr)shm_malloc(sizeof(kz_amqp_cmd));
	if(cmd == NULL) {
		LM_ERR("failed to allocate kz_amqp_cmd in process %d\n", getpid());
		goto error;
	}
	memset(cmd, 0, sizeof(kz_amqp_cmd));
	cmd->exchange = kz_amqp_str_dup(str_exchange);
	cmd->routing_key = kz_amqp_str_dup(str_routing_key);
	cmd->reply_routing_key = kz_amqp_string_dup(serverid);
	cmd->payload = kz_amqp_string_dup(payload);
	if(cmd->payload == NULL || cmd->routing_key == NULL || cmd->exchange == NULL) {
		LM_ERR("failed to allocate kz_amqp_cmd parameters in process %d\n", getpid());
		goto error;
	}
	if(lock_init(&cmd->lock)==NULL)
	{
		LM_ERR("cannot init the lock for publishing in process %d\n", getpid());
		lock_dealloc(&cmd->lock);
		goto error;
	}
	lock_get(&cmd->lock);
	cmd->type = KZ_AMQP_CALL;
	cmd->consumer = getpid();
	if (write(kz_pipe_fds[1], &cmd, sizeof(cmd)) != sizeof(cmd)) {
		LM_ERR("failed to publish message to amqp in process %d, write to command pipe: %s\n", getpid(), strerror(errno));
	} else {
		lock_get(&cmd->lock);
		switch(cmd->return_code) {
		case AMQP_RESPONSE_NORMAL:
			json_body = json_tokener_parse(cmd->return_payload);
		    if (is_error(json_body))
		    {
				LM_ERR("Error parsing body json: %s\n",json_tokener_errors[-(unsigned long)json_body]);
				LM_ERR("JSON : %s\n", cmd->return_payload);
				goto error;
		    }
		    *json_ret = json_body;
		    ret = 0;
		    break;

		default:
			ret = cmd->return_code;
			break;
		}
	}

 error:
	if(cmd)
		kz_amqp_free_pipe_cmd(cmd);

    if(json_obj)
    	json_object_put(json_obj);

    return ret;
}

int kz_amqp_publish(struct sip_msg* msg, char* exchange, char* routing_key, char* payload)
{
	  str json_s;
	  str exchange_s;
	  str routing_key_s;

		if (fixup_get_svalue(msg, (gparam_p)exchange, &exchange_s) != 0) {
			LM_ERR("cannot get exchange string value\n");
			return -1;
		}

		if (fixup_get_svalue(msg, (gparam_p)routing_key, &routing_key_s) != 0) {
			LM_ERR("cannot get routing_key string value\n");
			return -1;
		}

		if (fixup_get_svalue(msg, (gparam_p)payload, &json_s) != 0) {
			LM_ERR("cannot get json string value\n");
			return -1;
		}

		struct json_object *j = json_tokener_parse(json_s.s);

		if (is_error(j)) {
			LM_ERR("empty or invalid JSON payload\n");
			return -1;
		}

		json_object_put(j);

		return kz_amqp_pipe_send(&exchange_s, &routing_key_s, &json_s );


};

int kz_amqp_query(struct sip_msg* msg, char* exchange, char* routing_key, char* payload, char* dst)
{
	  str json_s;
	  str exchange_s;
	  str routing_key_s;
	  pv_spec_t *dst_pv;
	  pv_value_t dst_val;

		if (fixup_get_svalue(msg, (gparam_p)exchange, &exchange_s) != 0) {
			LM_ERR("cannot get exchange string value\n");
			return -1;
		}

		if (fixup_get_svalue(msg, (gparam_p)routing_key, &routing_key_s) != 0) {
			LM_ERR("cannot get routing_key string value\n");
			return -1;
		}

		if (fixup_get_svalue(msg, (gparam_p)payload, &json_s) != 0) {
			LM_ERR("cannot get json string value\n");
			return -1;
		}

		struct json_object *j = json_tokener_parse(json_s.s);

		if (is_error(j)) {
			LM_ERR("empty or invalid JSON payload\n");
			return -1;
		}

		json_object_put(j);

		json_obj_ptr ret = NULL;
		int res = kz_amqp_pipe_send_receive(&exchange_s, &routing_key_s, &json_s, &ret );

		if(res != 0) {
			return res;
		}

		dst_pv = (pv_spec_t *)dst;
		if(ret != NULL) {
			char *value = (char*)json_object_to_json_string(ret);
			dst_val.rs.s = value;
			dst_val.rs.len = strlen(value);
			dst_val.flags = PV_VAL_STR;
			dst_pv->setf(msg, &dst_pv->pvp, (int)EQ_T, &dst_val);
			json_object_put(ret);
		}

		return 1;

};

#define KEY_SAFE(C)  ((C >= 'a' && C <= 'z') || \
                      (C >= 'A' && C <= 'Z') || \
                      (C >= '0' && C <= '9') || \
                      (C == '-' || C == '~'  || C == '_'))

#define HI4(C) (C>>4)
#define LO4(C) (C & 0x0F)

#define hexint(C) (C < 10?('0' + C):('A'+ C - 10))

char *kz_amqp_util_encode(const str * key, char *dest) {
    if ((key->len == 1) && (key->s[0] == '#' || key->s[0] == '*')) {
	*dest++ = key->s[0];
	return dest;
    }
    char *p, *end;
    for (p = key->s, end = key->s + key->len; p < end; p++) {
	if (KEY_SAFE(*p)) {
	    *dest++ = *p;
	} else if (*p == '.') {
	    memcpy(dest, "\%2E", 3);
	    dest += 3;
	} else if (*p == ' ') {
	    *dest++ = '+';
	} else {
	    *dest++ = '%';
	    sprintf(dest, "%c%c", hexint(HI4(*p)), hexint(LO4(*p)));
	    dest += 2;
	}
    }
    *dest = '\0';
    return dest;
}

int kz_amqp_encode(struct sip_msg* msg, char* unencoded, char* encoded)
{
	char routing_key_buff[256];
    str unencoded_s;
	pv_spec_t *dst_pv;
	pv_value_t dst_val;
	dst_pv = (pv_spec_t *)encoded;

	if (fixup_get_svalue(msg, (gparam_p)unencoded, &unencoded_s) != 0) {
		LM_ERR("cannot get unencoded string value\n");
		return -1;
	}

	memset(routing_key_buff,0, sizeof(routing_key_buff));
	kz_amqp_util_encode(&unencoded_s, routing_key_buff);
	dst_val.rs.s = routing_key_buff;
	dst_val.rs.len = strlen(routing_key_buff);
	dst_val.flags = PV_VAL_STR;
	dst_pv->setf(msg, &dst_pv->pvp, (int)EQ_T, &dst_val);

	return 1;

}

int get_channel_index() {
	int n;
	for(n=channel_index; n < dbk_channels; n++)
		if(channels[n].state == KZ_AMQP_FREE) {
			channel_index = n+1;
			return n;
		}
	if(channel_index == 0) {
		LM_ERR("max channels (%d) reached. please exit kazoo and change db_kazoo amqp_max_channels param", dbk_channels);
		return -1;
	}
	channel_index = 0;
	return get_channel_index();
}

int kz_amqp_unbind_channel(kz_amqp_conn_ptr kz_conn, int idx )
{
    kz_amqp_reply_ptr reply = channels[idx].reply;
    int ret = 0;
	if(reply == NULL) {
		LM_ERR("unbinding channel NULL??\n");
		ret = -1;
		goto error;
	}

    if (amqp_basic_cancel(kz_conn->conn, channels[idx].channel, amqp_empty_bytes) < 0
	    || rmq_error("Canceling", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    if (amqp_queue_unbind(kz_conn->conn, channels[idx].channel, reply->queue, reply->exchange, reply->routing_key, amqp_empty_table) < 0
	    || rmq_error("Unbinding queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    amqp_queue_delete(kz_conn->conn, channels[idx].channel, reply->queue, 0, 0);

    kz_amqp_free_reply(reply);
    channels[idx].reply = NULL;
    channels[idx].state = KZ_AMQP_FREE;

error:
	return ret;
}

int kz_amqp_bind_channel_ex(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd, int idx )
{
    kz_amqp_reply_ptr reply = NULL;
    amqp_queue_declare_ok_t *r = NULL;

    str rpl_exch = str_init("targeted");

    int ret = -1;

    reply = (kz_amqp_reply_ptr)shm_malloc(sizeof(kz_amqp_reply));
	if(reply == NULL) {
		LM_ERR("error allocation memory for reply\n");
		goto error;
	}
	memset(reply, 0, sizeof(kz_amqp_reply));

	reply->exchange = kz_amqp_bytes_dup_from_str(&rpl_exch);
	reply->routing_key = kz_amqp_bytes_dup_from_string(cmd->reply_routing_key);

    if (reply->exchange.bytes == NULL || reply->routing_key.bytes == NULL) {
		LM_ERR("Out of memory allocating for exchange/routing_key\n");
		goto error;
    }

    if(idx == -1) {
    	idx = get_channel_index();
    }

    r = amqp_queue_declare(kz_conn->conn, channels[idx].channel, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
    if (rmq_error("Declaring queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		goto error;
    }

    reply->queue = amqp_bytes_malloc_dup(r->queue);
    if (reply->queue.bytes == NULL)
    {
		LM_ERR("Out of memory while copying queue name\n");
		goto error;
    }


    if (amqp_queue_bind(kz_conn->conn, channels[idx].channel, reply->queue, reply->exchange, reply->routing_key, amqp_empty_table) < 0
	    || rmq_error("Binding queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		goto error;
    }

    if (amqp_basic_consume(kz_conn->conn, channels[idx].channel, reply->queue, amqp_empty_bytes, 0, 1, 1, amqp_empty_table) < 0
	    || rmq_error("Consuming", amqp_get_rpc_reply(kz_conn->conn)))
    {
		goto error;
    }

    channels[idx].reply = reply;
    channels[idx].state = KZ_AMQP_BINDED;
	channels[idx].cmd = cmd;
    return idx;
 error:
	kz_amqp_free_reply(reply);

    return ret;
}

int kz_amqp_bind_channel(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd )
{
	return kz_amqp_bind_channel_ex( kz_conn, cmd, -1 );
}

int kz_amqp_bind_consumer_ex(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd, int idx )
{
    int ret = -1;

    amqp_bytes_t exchange = kz_amqp_bytes_dup_from_string(cmd->exchange);
    amqp_bytes_t type = kz_amqp_bytes_dup_from_string(cmd->exchange_type);
    amqp_bytes_t queue = kz_amqp_bytes_dup_from_string(cmd->queue);

    if (exchange.bytes == NULL || type.bytes == NULL || queue.bytes == NULL) {
		LM_ERR("Out of memory allocating for consumer\n");
		goto error;
    }

    if(idx == -1) {
    	idx = get_channel_index();
    }

    amqp_queue_declare(kz_conn->conn, channels[idx].channel, queue, 0, 0, 0, 1, amqp_empty_table);
    if (rmq_error("Declaring queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    LM_INFO("EXCHANGE DECLARE\n");
	amqp_exchange_declare(kz_conn->conn, channels[idx].channel, exchange, type, 0, 0, amqp_empty_table);

    LM_INFO("QUEUE BIND\n");
    if (amqp_queue_bind(kz_conn->conn, channels[idx].channel, queue, exchange, queue, amqp_empty_table) < 0
	    || rmq_error("Binding queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    LM_INFO("BASIC CONSUME\n");
    if (amqp_basic_consume(kz_conn->conn, channels[idx].channel, queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table) < 0
	    || rmq_error("Consuming", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    channels[idx].state = KZ_AMQP_CONSUMING;
	channels[idx].cmd = cmd;
    ret = idx;
 error:
 	amqp_bytes_free(exchange);
 	amqp_bytes_free(type);
 	amqp_bytes_free(queue);

    return ret;
}

int kz_amqp_bind_consumer(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd)
{
	return kz_amqp_bind_consumer_ex(kz_conn, cmd, -1);
}

int kz_amqp_send_ex(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd, kz_amqp_channel_state state, int idx)
{
	amqp_bytes_t exchange;
	amqp_bytes_t routing_key;
	amqp_bytes_t payload;
	int ret = 1;

    exchange = kz_amqp_bytes_dup_from_string(cmd->exchange);
    routing_key = kz_amqp_bytes_dup_from_string(cmd->routing_key);
    payload = kz_amqp_bytes_dup_from_string(cmd->payload);

	amqp_basic_properties_t props;
	memset(&props, 0, sizeof(amqp_basic_properties_t));
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");

	if(idx == -1) {
		idx = get_channel_index();
		if(idx == -1) {
			LM_ERR("Failed to get channel index to publish\n");
			goto error;
		}
	}
	channels[idx].state = state;

	amqp_basic_publish(kz_conn->conn, channels[idx].channel, exchange, routing_key, 0, 0, &props, payload);

	if ( rmq_error("Publishing",  amqp_get_rpc_reply(kz_conn->conn)) ) {
		LM_ERR("Failed to publish\n");
		ret = 0;
	}

	ret = idx;

	error:

	amqp_bytes_free(exchange);
	amqp_bytes_free(routing_key);
	amqp_bytes_free(payload);

	return ret;
}

int kz_amqp_send(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd)
{
	return kz_amqp_send_ex(kz_conn, cmd, KZ_AMQP_PUBLISHING , -1);
}


int kz_amqp_send_receive_ex(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd, int idx )
{
	int newidx = kz_amqp_bind_channel_ex(kz_conn, cmd, idx);
	if(newidx >= 0)
		return kz_amqp_send_ex(kz_conn, cmd, KZ_AMQP_CALLING, newidx);
	return newidx;
}

int kz_amqp_send_receive(kz_amqp_conn_ptr kz_conn, kz_amqp_cmd_ptr cmd )
{
	return kz_amqp_send_receive_ex(kz_conn, cmd, -1 );
}


void kz_amqp_presence_consumer_loop(int child_no)
{
	LM_INFO("starting consumer %d\n", child_no);
	close(kz_pipe_fds[child_no*2+1]);
	int data_pipe = kz_pipe_fds[child_no*2];

	fd_set fdset;
    int selret;

    char blf_queue_buffer[512];
    str unique_string = { 0, 0 };
    tmb.generate_callid(&unique_string);

	str exchange = str_init("dialoginfo");
	str exchange_type = str_init("direct");
	str queue;


	queue.s = blf_queue_buffer;
    queue.len = sprintf(blf_queue_buffer, "BLF-%.*s-%.*s",
			    dbk_node_hostname.len, dbk_node_hostname.s,
			    unique_string.len, unique_string.s);


	kz_amqp_cmd_ptr cmd = (kz_amqp_cmd_ptr)shm_malloc(sizeof(kz_amqp_cmd));
	if(cmd == NULL) {
		LM_ERR("error allocation memory for cmd in consumer %d\n", child_no);
		goto error;
	}
	memset(cmd, 0, sizeof(kz_amqp_cmd));
	cmd->consumer = child_no;
	cmd->exchange = kz_amqp_str_dup(&exchange);
	cmd->exchange_type = kz_amqp_str_dup(&exchange_type);
	cmd->queue = kz_amqp_str_dup(&queue);
	if(cmd->exchange == NULL || cmd->exchange_type == NULL || cmd->queue == NULL ) {
		LM_ERR("error allocation memory for cmd parameters in consumer %d\n", child_no);
		goto error;
	}
	cmd->type = KZ_AMQP_CONSUME;
	if(lock_init(&cmd->lock) == NULL) {
		LM_ERR("error initializing lock for cmd in consumer %d\n", child_no);
		goto error;
	}
	lock_get(&cmd->lock);
	if (write(kz_pipe_fds[1], &cmd, sizeof(cmd)) != sizeof(cmd)) {
		LM_ERR("failed to start_consumer %d, write to command pipe: %s\n", child_no, strerror(errno));
		kz_amqp_free_pipe_cmd(cmd);
	} else {
		lock_get(&cmd->lock);
		LM_INFO("starting consumer loop %d\n", child_no);
		while(1) {
    		FD_ZERO(&fdset);
    		FD_SET(data_pipe, &fdset);

    		selret = select(FD_SETSIZE, &fdset, NULL, NULL, &kz_sock_tv);

    		if (selret < 0) {
    			LM_ERR("select() failed: %s\n", strerror(errno));
    		} else if (!selret) {
    		} else {
				if(FD_ISSET(data_pipe, &fdset)) {
					char *payload;
	    			LM_INFO("trying to read\n");
					if(read(data_pipe, &payload, sizeof(payload)) == sizeof(payload)) {
						LM_INFO("consumer %d received payload %s\n", child_no, payload);
						rmqp_pres_update_handle(payload);
						shm_free(payload);
					}
				}
    		}
    	}
	}
error:
	LM_INFO("exiting consumer %d\n", child_no);
}

/*
void kz_amqp_generic_consumer_loop(int child_no)
{
	close(kz_pipe_fds[child_no*2+1]);
	int data_pipe = kz_pipe_fds[child_no*2];

}
*/

void kz_amqp_manager_loop(int child_no)
{
	LM_INFO("starting manager %d\n", child_no);
	close(kz_pipe_fds[child_no*2+1]);
	int data_pipe = kz_pipe_fds[child_no*2];
    fd_set fdset;
    int i, idx;
    int selret;
	int INTERNAL_READ, CONSUME, OK;
	int consumer;
	char* payload;
	int channel_res;
    kz_amqp_conn_ptr kzconn;
	kz_amqp_cmd_ptr cmd;
    channels = pkg_malloc(dbk_channels * sizeof(kz_amqp_channel));
	for(i=0; i < dbk_channels; i++) {
		channels[i].channel = i+1;
	}

    while(1) {

    	OK = 1;

    	while(1) {
    		kzconn = kz_amqp_get_next_connection();
    		if(kzconn != NULL)
    			break;
    		LM_INFO("Connection failed");
    	}
    	for(i=0,channel_res=0; i < dbk_channels && channel_res == 0; i++) {
    		channel_res = kz_amqp_channel_open(kzconn, channels[i].channel);
    		cmd = channels[i].cmd;
    		if(channels[i].reply != NULL) {
    			kz_amqp_free_reply(channels[i].reply);
    			channels[i].reply = NULL;
    		}
    		if(channel_res == 0) {
				switch(channels[i].state) {
					case KZ_AMQP_PUBLISHING:
						idx = kz_amqp_send_ex(kzconn, cmd, KZ_AMQP_PUBLISHING, i);
						if(idx >= 0) {
							cmd->return_code = AMQP_RESPONSE_NORMAL;
							channels[idx].state = KZ_AMQP_FREE;
							lock_release(&cmd->lock);
						} else {
							cmd->return_code = idx;
							OK = INTERNAL_READ = CONSUME = 0;
						}
						break;
					case KZ_AMQP_CALLING:
					case KZ_AMQP_BINDED:
						if(kz_amqp_send_receive_ex(kzconn, cmd, i) < 0) {
							OK = INTERNAL_READ = CONSUME = 0;
						}
						break;
					case KZ_AMQP_CONSUMING:
						if(kz_amqp_bind_consumer_ex(kzconn, cmd, i) < 0) {
							OK = INTERNAL_READ = CONSUME = 0;
						}
						break;
					default:
						channels[i].state = KZ_AMQP_FREE;
						break;
					}
    		}
    	}

    	while(OK) {
        	INTERNAL_READ = 1;
    		CONSUME = 1;

        	while(INTERNAL_READ) {
				FD_ZERO(&fdset);
				FD_SET(data_pipe, &fdset);
				selret = select(FD_SETSIZE, &fdset, NULL, NULL, &kz_sock_tv);
				if (selret < 0) {
					LM_ERR("select() failed: %s\n", strerror(errno));
					break;
				} else if (!selret) {
					INTERNAL_READ=0;
				} else {
					if(FD_ISSET(data_pipe, &fdset)) {
						if(read(data_pipe, &cmd, sizeof(cmd)) == sizeof(cmd)) {
							switch (cmd->type) {
							case KZ_AMQP_PUBLISH:
								idx = kz_amqp_send(kzconn, cmd);
								if(idx >= 0) {
									cmd->return_code = AMQP_RESPONSE_NORMAL;
									channels[idx].state = KZ_AMQP_FREE;
									lock_release(&cmd->lock);
								} else {
									cmd->return_code = idx;
									OK = INTERNAL_READ = CONSUME = 0;
								}
								break;
							case KZ_AMQP_CALL:
								if(kz_amqp_send_receive(kzconn, cmd) < 0) {
									OK = INTERNAL_READ = CONSUME = 0;
								}
								break;
							case KZ_AMQP_CONSUME:
								if(kz_amqp_bind_consumer(kzconn, cmd) >= 0) {
									lock_release(&cmd->lock);
								} else {
									OK = INTERNAL_READ = CONSUME = 0;
								}
								break;
							default:
								LM_INFO("unknown pipe cmd %d\n", cmd->type);
								break;
							}
						}
					}
				}
        	}


    	    while(CONSUME) {
        		consumer = 0;
        		payload = NULL;

				amqp_envelope_t envelope;
				amqp_maybe_release_buffers(kzconn->conn);
				amqp_rpc_reply_t reply = amqp_consume_message(kzconn->conn, &envelope, &kz_amqp_tv, 0);
				switch(reply.reply_type) {
				case AMQP_RESPONSE_LIBRARY_EXCEPTION:
					switch(reply.library_error) {
					case AMQP_STATUS_HEARTBEAT_TIMEOUT:
						LM_ERR("AMQP_STATUS_HEARTBEAT_TIMEOUT\n");
						OK = CONSUME = 0;
						break;
					case AMQP_STATUS_TIMEOUT:
						CONSUME = 0;
						break;
					case AMQP_STATUS_UNEXPECTED_STATE:
						LM_INFO("AMQP_STATUS_UNEXPECTED_STATE\n");
						OK = CONSUME = kz_amqp_consume_error(kzconn->conn);
						break;
					default:
						OK = CONSUME = 0;
						break;
					};
					break;

				case AMQP_RESPONSE_NORMAL:
					switch(channels[envelope.channel-1].state) {
					case KZ_AMQP_CALLING:
						channels[envelope.channel-1].cmd->return_payload = kz_amqp_string_dup((char*)envelope.message.body.bytes);
						channels[envelope.channel-1].cmd->return_code = AMQP_RESPONSE_NORMAL;
						lock_release(&channels[envelope.channel-1].cmd->lock);
						channels[envelope.channel-1].state = KZ_AMQP_FREE;
						if(kz_amqp_unbind_channel(kzconn, envelope.channel-1) < 0) {
							OK = CONSUME = 0;
						}
						break;
					case KZ_AMQP_CONSUMING:
						consumer = channels[envelope.channel-1].cmd->consumer;
						payload = kz_amqp_string_dup((char*)envelope.message.body.bytes);
						if (write(kz_pipe_fds[consumer*2+1], &payload, sizeof(payload)) != sizeof(payload)) {
							LM_ERR("failed to send payload to consumer %d : %s\nPayload %s\n", consumer, strerror(errno), payload);
						};
						break;
					default:
						break;
					}
					break;
				case AMQP_RESPONSE_SERVER_EXCEPTION:
					LM_ERR("AMQP_RESPONSE_SERVER_EXCEPTION in consume\n");
					OK = CONSUME = 0;
					break;

				default:
					OK = CONSUME = 0;
					break;


				};
				amqp_destroy_envelope(&envelope);
    	    }
    	}

    	kz_amqp_connection_close(kzconn);
    }
}
