/*
 * kz_amqp.h
 *
 *  Created on: Jul 29, 2014
 *      Author: root
 */

#ifndef KZ_AMQP_H_
#define KZ_AMQP_H_

#include "../../sr_module.h"

#include "const.h"
#include "defs.h"
#include "dbase.h"
#include "blf.h"

typedef struct amqp_connection_info kz_amqp_connection_info;
typedef kz_amqp_connection_info *kz_amqp_connection_info_ptr;

extern int dbk_channels;

typedef struct kz_amqp_conn_t {
	kz_amqp_connection_info info;
	amqp_connection_state_t conn;
	amqp_socket_t *socket;
	amqp_channel_t channel_count;
	amqp_channel_t channel_counter;
//    gen_lock_t lock;
    struct kz_amqp_conn_t* next;
} kz_amqp_conn, *kz_amqp_conn_ptr;

typedef struct {
	kz_amqp_conn_ptr current;
	kz_amqp_conn_ptr head;
	kz_amqp_conn_ptr tail;
//    gen_lock_t lock;
} kz_amqp_conn_pool, *kz_amqp_conn_pool_ptr;


#define AMQP_KZ_CMD_PUBLISH       1
#define AMQP_KZ_CMD_CALL          2
#define AMQP_KZ_CMD_CONSUME       3

typedef enum {
	KZ_AMQP_PUBLISH     = 1,
	KZ_AMQP_CALL    = 2,
	KZ_AMQP_CONSUME = 3
} kz_amqp_pipe_cmd_type;

typedef enum {
	KZ_AMQP_CLOSED     = 0,
	KZ_AMQP_FREE     = 1,
	KZ_AMQP_PUBLISHING    = 2,
	KZ_AMQP_BINDED = 3,
	KZ_AMQP_CALLING    = 4,
	KZ_AMQP_CONSUMING = 5
} kz_amqp_channel_state;

typedef struct {
    gen_lock_t lock;
	kz_amqp_pipe_cmd_type type;
	char* exchange;
	char* exchange_type;
	char* routing_key;
	char* reply_routing_key;
	char* queue;
	char* payload;
	char* return_payload;
	int   return_code;
	int   consumer;
} kz_amqp_cmd, *kz_amqp_cmd_ptr;

typedef struct {
	amqp_bytes_t exchange;
	amqp_bytes_t routing_key;
	amqp_bytes_t queue;
} kz_amqp_reply, *kz_amqp_reply_ptr;

typedef struct {
	kz_amqp_cmd_ptr cmd;
	kz_amqp_reply_ptr reply;
	amqp_channel_t channel;
	kz_amqp_channel_state state;
} kz_amqp_channel, *kz_amqp_channel_ptr;

int kz_amqp_add_connection(modparam_t type, void* val);

int kz_amqp_publish(struct sip_msg* msg, char* exchange, char* routing_key, char* payload);
int kz_amqp_query(struct sip_msg* msg, char* exchange, char* routing_key, char* payload, char* dst);
int kz_amqp_encode(struct sip_msg* msg, char* unencoded, char* encoded);
void kz_amqp_presence_consumer_loop(int child_no);
//void kz_amqp_generic_consumer_loop(int child_no);
void kz_amqp_manager_loop(int child_no);

#endif /* KZ_AMQP_H_ */
