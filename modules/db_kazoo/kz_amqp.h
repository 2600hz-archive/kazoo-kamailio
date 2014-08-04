/*
 * kz_amqp.h
 *
 *  Created on: Jul 29, 2014
 *      Author: root
 */

#ifndef KZ_AMQP_H_
#define KZ_AMQP_H_

#include "const.h"
#include "defs.h"
#include "dbase.h"

typedef struct amqp_connection_info kz_amqp_connection_info;
typedef kz_amqp_connection_info *kz_amqp_connection_info_ptr;

typedef struct kz_amqp_conn_t {
	kz_amqp_connection_info info;
	amqp_connection_state_t conn;
	amqp_socket_t *socket;
	amqp_channel_t channel_count;
	amqp_channel_t channel_counter;
    gen_lock_t lock;
    struct kz_amqp_conn_t* next;
} kz_amqp_conn, *kz_amqp_conn_ptr;

typedef struct {
	kz_amqp_conn_ptr current;
	kz_amqp_conn_ptr head;
	kz_amqp_conn_ptr tail;
    gen_lock_t lock;
} kz_amqp_conn_pool, *kz_amqp_conn_pool_ptr;

int kz_amqp_add_connection(modparam_t type, void* val);

int kz_amqp_publish(struct sip_msg* msg, char* exchange, char* routing_key, char* payload);
int kz_amqp_query(struct sip_msg* msg, char* exchange, char* routing_key, char* payload, char* dst);
int kz_amqp_encode(struct sip_msg* msg, char* unencoded, char* encoded);


#endif /* KZ_AMQP_H_ */
