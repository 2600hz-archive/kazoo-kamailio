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

void kz_amqp_init_pool() {
	if(kz_pool == NULL) {
		kz_pool = (kz_amqp_conn_pool_ptr) shm_malloc(sizeof(kz_amqp_conn_pool));
		memset(kz_pool, 0, sizeof(kz_amqp_conn_pool));
		lock_init(&kz_pool->lock);
	}
}

int kz_amqp_add_connection(modparam_t type, void* val)
{
	kz_amqp_init_pool(); // find a better way
	lock_get(&kz_pool->lock);

	kz_amqp_conn_ptr newConn = shm_malloc(sizeof(kz_amqp_conn));
	memset(newConn, 0, sizeof(kz_amqp_conn));
	lock_init(&newConn->lock);

	if(kz_pool->head == NULL)
		kz_pool->head = newConn;

	if(kz_pool->tail != NULL)
		kz_pool->tail->next = newConn;

	kz_pool->tail = newConn;

    amqp_parse_url((char*)val, &newConn->info);

   	lock_release(&kz_pool->lock);

	return 0;
}


void kz_amqp_connection_close(kz_amqp_conn_ptr rmq) {
    LM_DBG("Close rmq connection\n");
    if (!rmq)
    	return;

    if (rmq->conn) {
		LM_DBG("close connection:  %d rmq(%p)->conn(%p)\n", getpid(), (void *)rmq, rmq->conn);
		rmq_error("closing connection", amqp_connection_close(rmq->conn, AMQP_REPLY_SUCCESS));
		if (amqp_destroy_connection(rmq->conn) < 0) {
			LM_ERR("cannot destroy connection\n");
		}
		rmq->conn = NULL;
		rmq->socket = NULL;
		rmq->channel_count = 0;
    }
}

void kz_amqp_channel_close(kz_amqp_conn_ptr rmq, amqp_channel_t channel) {
    LM_DBG("Close rmq channel\n");
    if (!rmq)
    	return;

	LM_DBG("close channel: %d rmq(%p)->channel(%d)\n", getpid(), (void *)rmq, channel);
	rmq_error("closing channel", amqp_channel_close(rmq->conn, channel, AMQP_REPLY_SUCCESS));
}

int kz_amqp_connection_open(kz_amqp_conn_ptr rmq) {
    if (!(rmq->conn = amqp_new_connection())) {
    	LM_DBG("Failed to create new AMQP connection\n");
    	goto error;
    }

    rmq->socket = amqp_tcp_socket_new(rmq->conn);
    if (!rmq->socket) {
    	LM_DBG("Failed to create TCP socket to AMQP broker\n");
    	goto error;
    }

    if (amqp_socket_open(rmq->socket, rmq->info.host, rmq->info.port)) {
    	LM_DBG("Failed to open TCP socket to AMQP broker\n");
    	goto error;
    }

    if (rmq_error("Logging in", amqp_login(rmq->conn,
					   "/",
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


amqp_channel_t kz_amqp_channel_open(kz_amqp_conn_ptr rmq) {
	int ret = 0;
	if(rmq == NULL) {
		LM_DBG("rmq == NULL \n");
		return 0;
	}

	lock_get(&rmq->lock);
	amqp_channel_t channel = rmq->channel_count + 1;
    amqp_channel_open(rmq->conn, channel);
    if (rmq_error("Opening channel", amqp_get_rpc_reply(rmq->conn))) {
    	LM_ERR("Failed to open channel AMQP %d!\n", channel);
    } else {
    	ret = channel;
    	rmq->channel_count = channel;
    }
   	lock_release(&rmq->lock);
    return ret;
}

kz_amqp_conn_ptr kz_amqp_get_connection() {
	kz_amqp_conn_ptr ptr = NULL;
	if(kz_pool == NULL) {
		return NULL;
	}
	lock_get(&kz_pool->lock);

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
   	lock_release(&kz_pool->lock);
   	return ptr;
}

void kz_amqp_consume_error(amqp_rpc_reply_t ret, amqp_connection_state_t conn)
{
	amqp_frame_t frame;

	if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
		if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
				AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
			if (AMQP_STATUS_OK != amqp_simple_wait_frame(conn, &frame)) {
				return;
			}

			if (AMQP_FRAME_METHOD == frame.frame_type) {
				switch (frame.payload.method.id) {
				case AMQP_BASIC_ACK_METHOD:
					/* if we've turned publisher confirms on, and we've published a message
					 * here is a message being confirmed
					 */

					break;
				case AMQP_BASIC_RETURN_METHOD:
					/* if a published message couldn't be routed and the mandatory flag was set
					 * this is what would be returned. The message then needs to be read.
					 */
				{
					amqp_message_t message;
					ret = amqp_read_message(conn, frame.channel, &message, 0);
					if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
						return;
					}

					LM_DBG("Received this message : %.*s\n", (int) message.body.len, (char*)message.body.bytes);
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
					return;

				case AMQP_CONNECTION_CLOSE_METHOD:
					/* a connection.close method happens when a connection exception occurs,
					 * this can happen by trying to use a channel that isn't open for example.
					 *
					 * In this case the whole connection must be restarted.
					 */
					return;

				default:
					LM_ERR("An unexpected method was received %d\n", frame.payload.method.id);
					return;
				}
			}
		}
	}
}



int kz_amqp_send(str *str_exchange, str *str_routing_key, str *str_payload)
{
	amqp_bytes_t exchange;
	amqp_bytes_t routing_key;
	amqp_bytes_t amqp_mb;
	int ret = -1;

    exchange = amqp_cstring_bytes(str_exchange->s);
    routing_key = amqp_cstring_bytes(str_routing_key->s);
    amqp_mb = amqp_cstring_bytes(str_payload->s);

	/* send to rabbitmq */

	amqp_basic_properties_t props;
	memset(&props, 0, sizeof(amqp_basic_properties_t));
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
	props.content_type = amqp_cstring_bytes("application/json");

	kz_amqp_conn_ptr kz_conn = kz_amqp_get_connection();
	amqp_channel_t channel = kz_amqp_channel_open(kz_conn);

	amqp_basic_publish(kz_conn->conn, channel, exchange, routing_key, 0, 0, &props, amqp_mb);

	if ( rmq_error("Publishing",  amqp_get_rpc_reply(kz_conn->conn)) ) {
		LM_ERR("Failed to publish\n");
		ret = 0;
	}

	kz_amqp_channel_close(kz_conn, channel);
	kz_amqp_connection_close(kz_conn); // for now

	return ret;
}

typedef struct json_object *json_obj_ptr;

int kz_amqp_send_receive(str *str_exchange, str *str_routing_key, str *str_payload, json_obj_ptr* json_ret )
{
    char serverid[512];
    char node_name[512];
    amqp_bytes_t amqp_rk;
    amqp_bytes_t amqp_mb;
	amqp_bytes_t exchange;
    amqp_bytes_t rpl_queue = { 0, 0 };
    str unique_string = { 0, 0 };
    int ret = -1;
    json_obj_ptr json_obj = NULL;
    kz_amqp_conn_ptr kz_conn = NULL;
    amqp_channel_t channel = 0;
    amqp_message_t msg;
    amqp_envelope_t envelope;

    struct timeval timeout;
    timeout.tv_sec = dbk_auth_wait_timeout;
    timeout.tv_usec = 0;

    memset(&envelope, 0, sizeof(amqp_envelope_t));
    memset(&msg, 0, sizeof(amqp_envelope_t));

    /* extract info from json and construct xml */
    json_obj = json_tokener_parse(str_payload->s);
    if (is_error(json_obj))
    {
		LM_ERR("Error parsing json: %s\n",json_tokener_errors[-(unsigned long)json_obj]);
		LM_ERR("%s\n", str_payload->s);
		goto error;
    }

    exchange = amqp_cstring_bytes(str_exchange->s);
    amqp_rk = amqp_cstring_bytes(str_routing_key->s);
    tmb.generate_callid(&unique_string);
    sprintf(serverid, "kamailio@%.*s-<%d>-script-%lu", dbk_node_hostname.len, dbk_node_hostname.s, my_pid(), rpl_query_routing_key_count++);

    json_object_object_add(json_obj, BLF_JSON_APP_NAME,
			   json_object_new_string(NAME));
    json_object_object_add(json_obj, BLF_JSON_APP_VERSION,
			   json_object_new_string(VERSION));
    sprintf(node_name, "kamailio@%.*s", dbk_node_hostname.len, dbk_node_hostname.s);
    json_object_object_add(json_obj, BLF_JSON_NODE,
			   json_object_new_string(node_name));
    json_object_object_add(json_obj, BLF_JSON_SERVERID,
			   json_object_new_string(serverid));
    json_object_object_add(json_obj, BLF_JSON_MSG_ID,
			   json_object_new_string_len(unique_string.s, unique_string.len));

    amqp_mb.bytes = (char *)json_object_to_json_string(json_obj);
    if (amqp_mb.bytes == NULL)
    {
		LM_ERR("Failed to get json string\n");
		goto error;
    }
    amqp_mb.len = strlen(amqp_mb.bytes);
    LM_DBG("AMQP: body: %.*s\n", (int)amqp_mb.len, (char *)amqp_mb.bytes);

    /* Declare reply queue and start consumer */
    amqp_bytes_t rpl_routing_key = amqp_cstring_bytes(serverid);

    kz_conn = kz_amqp_get_connection();
	if(kz_conn == NULL) {
		goto error;
	}
	channel = kz_amqp_channel_open(kz_conn);
	if(channel == 0) {
		goto error;
	}

    amqp_queue_declare_ok_t *r = amqp_queue_declare(kz_conn->conn, channel, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);

    if (rmq_error("Declaring queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    rpl_queue = amqp_bytes_malloc_dup(r->queue);
    if (rpl_queue.bytes == NULL)
    {
		LM_ERR("Out of memory while copying queue name\n");
		goto error;
    }

    amqp_bytes_t rpl_exch = {8, "targeted"};

    if (amqp_queue_bind(kz_conn->conn, channel, rpl_queue, rpl_exch, rpl_routing_key, amqp_empty_table) < 0
	    || rmq_error("Binding queue", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    if (amqp_basic_consume(kz_conn->conn, channel, rpl_queue, amqp_empty_bytes, 0, 1, 1, amqp_empty_table) < 0
	    || rmq_error("Consuming", amqp_get_rpc_reply(kz_conn->conn)))
    {
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    /* publish  */
    amqp_basic_properties_t props;
    memset(&props, 0, sizeof(amqp_basic_properties_t));
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
    props.content_type = amqp_cstring_bytes("application/json");
    if (amqp_basic_publish(kz_conn->conn, channel, exchange, amqp_rk, 0, 0, &props, amqp_mb) < 0
	    || rmq_error("Publishing", amqp_get_rpc_reply(kz_conn->conn)))
    {
		LM_WARN("Failed to publish to AMQP, dropping request\n");
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    amqp_maybe_release_buffers(kz_conn->conn);
    amqp_rpc_reply_t reply = amqp_consume_message_on_channel(kz_conn->conn, channel, &envelope, &timeout, 0);
    if (AMQP_RESPONSE_NORMAL != reply.reply_type)
    {
    	kz_amqp_consume_error(reply, kz_conn->conn);
		ret = -RET_AMQP_ERROR;
		goto error;
    }

    json_obj_ptr json_body = json_tokener_parse((char*)envelope.message.body.bytes);
	amqp_destroy_envelope(&envelope);
    if (is_error(json_body))
    {
		LM_ERR("Error parsing body json: %s\n",json_tokener_errors[-(unsigned long)json_body]);
		LM_ERR("%s\n", (char*) msg.body.bytes);
		goto error;
    }

    *json_ret = json_body;
    ret = 0;
 error:
	if(kz_conn != NULL && channel != 0) {
		if (rpl_queue.bytes) {
			amqp_queue_delete(kz_conn->conn, channel, rpl_queue, 0, 0);
			if (rmq_error("Deleting reply queue", amqp_get_rpc_reply(kz_conn->conn))) {
				LM_ERR("Failed to delete queue\n");
			}
			LM_DBG("rpl_queue [%.*s]\n", (int)rpl_queue.len, (char *)rpl_queue.bytes);
			amqp_bytes_free(rpl_queue);
		}
		kz_amqp_channel_close(kz_conn, channel);
		kz_amqp_connection_close(kz_conn); // for now

	}

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

		return kz_amqp_send(&exchange_s, &routing_key_s, &json_s );


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
		int res = kz_amqp_send_receive(&exchange_s, &routing_key_s, &json_s, &ret );

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

