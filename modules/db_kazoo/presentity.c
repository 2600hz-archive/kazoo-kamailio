#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <json/json.h>
#include <libxml/parser.h>
#include "../../parser/parse_to.h"
#include "../../dprint.h"
#include "../../mem/mem.h"
#include "../../timer_proc.h"
#include "../../sr_module.h"
#include "../../lib/kmi/mi.h"
#include "../presence/bind_presence.h"
#include "../pua_dialoginfo/bind_dialoginfo.h"
#include "../presence_dialoginfo/bind_pres_dialoginfo.h"
#include "../../pvar.h"

#include "../pua/pua_bind.h"
#include "../pua/send_publish.h"
#include "../../parser/contact/contact.h"
#include "../../parser/contact/parse_contact.h"

#include "presentity.h"


static dbk_presentity_htable_t *dbk_presentity_phtable = NULL;
extern int dbk_presentity_phtable_size;
extern int dbk_create_empty_dialog;

void dbk_free_presentity(dbk_presentity_t * pu) {
    shm_free(pu);
}

int dbk_presentity_initialize(void) {
    int i;

    dbk_presentity_phtable =
	(dbk_presentity_htable_t *) shm_malloc(dbk_presentity_phtable_size *
					 sizeof(dbk_presentity_htable_t));
    if (dbk_presentity_phtable == NULL) {
	LM_ERR("dbk_initialize_presentity_htable : No more shared memory\n");
	return -1;
    }
    memset(dbk_presentity_phtable, 0, dbk_presentity_phtable_size * sizeof(dbk_presentity_htable_t));

    for (i = 0; i < dbk_presentity_phtable_size; i++) {
	lock_init(&dbk_presentity_phtable[i].lock);
    }

    return 0;
}


void dbk_presentity_destroy(void) {
	dbk_presentity_t *pu, *pu_next;
	int i;

	if (dbk_presentity_phtable == NULL ) {
		return;
	}

	for (i = 0; i < dbk_presentity_phtable_size; i++) {
		lock_destroy(&dbk_presentity_phtable[i].lock);
		pu = dbk_presentity_phtable[i].pu;
		while (pu) {
			pu_next = pu->next;
			dbk_free_presentity(pu);
			pu = pu_next;
		}

	}
	shm_free(dbk_presentity_phtable);
}

dbk_presentity_t *dbk_presentity_search_expired(unsigned int hash_code, int expires, dbk_presentity_t ** pu_prev_ret) {
	dbk_presentity_t *pu, *pu_prev;
//	pu = ( (*pu_prev_ret) == NULL ? dbk_presentity_phtable[hash_code].pu : (*pu_prev_ret)->next);
//	pu_prev = *pu_prev_ret;

	pu = dbk_presentity_phtable[hash_code].pu;
	pu_prev = NULL;

	for (; pu; pu = pu->next) {
		if (pu->expires < expires) {
			break;
		}
		pu_prev = pu;
	}
	if (pu_prev_ret) {
		*pu_prev_ret = pu_prev;
	}

	return pu;
}

dbk_presentity_t *dbk_presentity_search(unsigned int hash_code, str *event,
		str *domain, str * username, dbk_presentity_t ** pu_prev_ret,
		dbk_presentity_t * prev_pu_addr) {
	dbk_presentity_t *pu, *pu_prev = NULL;

	for (pu = dbk_presentity_phtable[hash_code].pu; pu; pu = pu->next) {
		if (pu->domain.len == domain->len
				&& memcmp(pu->domain.s, domain->s, domain->len) == 0
				&& pu->username.len == username->len
				&& memcmp(pu->username.s, username->s, username->len) == 0
				&& pu->event.len == event->len
				&& memcmp(pu->event.s, event->s, event->len) == 0) {
			break;
		}
		pu_prev = pu;
	}
	if (pu_prev_ret) {
		*pu_prev_ret = pu_prev;
	}

	return pu;
}

dbk_presentity_t *dbk_presentity_htable_new(str *event, str *domain, str *username,
		str * etag, str * sender, str * body, int received_time, int expires) {
	int size = event->len + domain->len + username->len + etag->len + sender->len
			+ body->len + 6;
	dbk_presentity_t *pu = (dbk_presentity_t *) shm_malloc(
			sizeof(dbk_presentity_t) + size);

	if (pu == NULL ) {
		LM_ERR("No more shared memory\n");
		return NULL ;
	}
	memset(pu, 0, sizeof(dbk_presentity_t) + size);
	char *p = (char *) pu + sizeof(dbk_presentity_t);

	pu->event.s = p;
	memcpy(pu->event.s, event->s, event->len);
	pu->event.len = event->len;
	p += event->len;
	*p='\0';
	p++;

	pu->domain.s = p;
	memcpy(pu->domain.s, domain->s, domain->len);
	pu->domain.len = domain->len;
	p += domain->len;
	*p='\0';
	p++;

	pu->username.s = p;
	memcpy(pu->username.s, username->s, username->len);
	pu->username.len = username->len;
	p += username->len;
	*p='\0';
	p++;

	pu->etag.s = p;
	memcpy(pu->etag.s, etag->s, etag->len);
	pu->etag.len = etag->len;
	p += etag->len;
	*p='\0';
	p++;

	if (sender->len) {
		pu->sender.s = p;
		memcpy(pu->sender.s, sender->s, sender->len);
		pu->sender.len = sender->len;
		p += sender->len;
		*p='\0';
		p++;
	}

	if (body->len) {
		pu->body.s = p;
		memcpy(pu->body.s, body->s, body->len);
		pu->body.len = body->len;
		p += body->len;
		*p='\0';
		p++;
	}

	pu->received_time = received_time;
	pu->expires = expires;

	return pu;
}

int dbk_presentity_htable_insert(str *event, str *domain, str *username, str * etag,
		str * sender, str * body, int received_time, int expires,
		unsigned int hash_code) {
	dbk_presentity_t *pu = dbk_presentity_htable_new(event, domain, username, etag,
			sender, body, received_time, expires);

	if (pu == NULL ) {
		LM_ERR("Failed to create new pres user\n");
		return -1;
	}

	lock_get(&dbk_presentity_phtable[hash_code].lock);
	if(dbk_presentity_phtable[hash_code].tail != NULL) {
		dbk_presentity_phtable[hash_code].tail->next = pu;
	}
	if(dbk_presentity_phtable[hash_code].pu == NULL) {
		dbk_presentity_phtable[hash_code].pu = pu;
	}
	dbk_presentity_phtable[hash_code].tail = pu;
	lock_release(&dbk_presentity_phtable[hash_code].lock);
	return 0;
}

int dbk_presentity_htable_delete(str *event, str *domain, str *username, str * etag,
		str * sender, str * body, int received_time, int expires,
		unsigned int hash_code) {

	dbk_presentity_t *pu, *pu_prev = NULL;

	lock_get(&dbk_presentity_phtable[hash_code].lock);

	for (pu = dbk_presentity_phtable[hash_code].pu; pu; pu = pu->next) {
		if (pu->domain.len == domain->len
				&& strncmp(pu->domain.s, domain->s, domain->len) == 0
				&& pu->username.len == username->len
				&& strncmp(pu->username.s, username->s, username->len) == 0
				&& pu->event.len == event->len
				&& strncmp(pu->event.s, event->s, event->len) == 0
				&& pu->sender.len == sender->len
				&& strncmp(pu->sender.s, sender->s, sender->len) == 0
				) {
			break;
		}
		pu_prev = pu;
	}
	if(pu) {
		if(pu_prev) {
			pu_prev->next = pu->next;
		} else {
			dbk_presentity_phtable[hash_code].pu = pu->next;
		}
		if(pu == dbk_presentity_phtable[hash_code].tail)
			dbk_presentity_phtable[hash_code].tail = pu_prev;

		dbk_free_presentity(pu);
	}
	lock_release(&dbk_presentity_phtable[hash_code].lock);
	return 0;
}

int dbk_presentity_flush(int flush_all, str *event, str * domain, str * user) {
	dbk_presentity_t *pu;

	if (flush_all) {
		int i;
		dbk_presentity_t *pu_next;
		for (i = 0; i < dbk_presentity_phtable_size; i++) {
			lock_get(&dbk_presentity_phtable[i].lock);
			pu = dbk_presentity_phtable[i].pu;
			for (; pu; pu = pu_next) {
				pu_next = pu->next;
				dbk_free_presentity(pu);
			}
			dbk_presentity_phtable[i].pu = dbk_presentity_phtable[i].tail = NULL;
			lock_release(&dbk_presentity_phtable[i].lock);
		}
	} else {
		int hash_code;
		dbk_presentity_t *pu_prev = NULL;

		// TODO
		hash_code = core_hash(domain, user, dbk_presentity_phtable_size);
		lock_get(&dbk_presentity_phtable[hash_code].lock);
		pu = dbk_presentity_search(hash_code, event, domain, user, &pu_prev, 0);
		if (pu == NULL ) {
			LM_DBG("FLUSH: No record found for user %.*s\n", user->len, user->s);
			lock_release(&dbk_presentity_phtable[hash_code].lock);
		} else {
			LM_DBG("FLUSH: Delete record for user %.*s\n", user->len, user->s);
			if (pu_prev) {
				pu_prev->next = pu->next;
			} else {
				dbk_presentity_phtable[hash_code].pu = pu->next;
			}
			lock_release(&dbk_presentity_phtable[hash_code].lock);
			dbk_free_presentity(pu);
		}

	}
	return 0;
}



int w_mi_dbk_presentity_flush(struct sip_msg* msg)
{
	if (dbk_presentity_flush(1, NULL, NULL, NULL) < 0) {
		LM_ERR("Presence htable flushing failed\n");
		return -1;
	}
	return 1;
}

struct mi_root *mi_dbk_presentity_flush(struct mi_root *cmd, void *param) {
	struct mi_node *node = NULL;
	str type;
	str user, event, domain;
	int flush_all = 0;

	node = cmd->node.kids;
	if (node == NULL ) {
		LM_ERR("Null command- missing parameters\n");
		return 0;
	}

	LM_DBG("Get type\n");

	/* Get type */
	type = node->value;
	if (type.s == NULL || type.len == 0) {
		LM_ERR("first parameter empty\n");
		return init_mi_tree(404, "Missing parameter ('all' or 'user')", 35);
	}

	LM_DBG("Flush type=[%.*s]\n", type.len, type.s);

	if (type.len == 3 && strncmp(type.s, "all", 3) == 0) {
		LM_DBG("Flush all\n");
		flush_all = 1;
	} else {
		node = node->next;
		if (node == NULL )
			return 0;
		event = node->value;
		if (event.s == NULL || event.len == 0) {
			LM_ERR("No event provided\n");
			return init_mi_tree(404, "No event provided", 20);
		}

		if(node->next != NULL) {
			node = node->next;
			domain = node->value;
		}

		if(node->next != NULL) {
			node = node->next;
			user = node->value;
		}
	}

	if (dbk_presentity_flush(flush_all, &event, &domain, &user) < 0) {
		LM_ERR("Presence htable flushing failed\n");
		return init_mi_tree(500, MI_SSTR(MI_INTERNAL_ERR));
	}
	return init_mi_tree(200, MI_SSTR(MI_OK));

}

typedef struct dbk_presentity_print {
	str event;
	str domain;
	str username;
	struct mi_node *event_node;
	struct mi_node *domain_node;
	struct mi_node *username_node;
	dbk_presentity_htable_t *root;
} dbk_presentity_print_t, *dbk_presentity_print_p;

int dbk_mi_print_presentity(struct mi_node *rpl, dbk_presentity_t * pu, dbk_presentity_print_p hier) {
    struct mi_node *node = NULL;

    if(hier->event_node == NULL || strncmp(hier->event.s, pu->event.s, pu->event.len)) {
    	hier->event_node = add_mi_node_child(rpl, MI_DUP_VALUE, "Event", 5, pu->event.s, pu->event.len);
    	hier->event = pu->event;
    	hier->domain = str_null_string;
    	hier->username = str_null_string;
    	if (hier->event_node == 0) {
    		LM_ERR("Failed to add Event node\n");
    		goto error;
    	}
    }

    if(hier->domain_node == NULL || strncmp(hier->domain.s, pu->domain.s, pu->domain.len)) {
    	hier->domain_node = add_mi_node_child(hier->event_node, MI_DUP_VALUE, "Domain", 6, pu->domain.s, pu->domain.len);
    	hier->domain = pu->domain;
    	hier->username = str_null_string;
    	if (hier->domain_node == 0) {
    		LM_ERR("Failed to add domain node\n");
    		goto error;
    	}
    }

    if(hier->username_node == NULL || strncmp(hier->username.s, pu->username.s, pu->username.len)) {
    	hier->username_node = add_mi_node_child(hier->domain_node, MI_DUP_VALUE, "User", 4, pu->username.s, pu->username.len);
    	hier->username = pu->username;
    	if (hier->username_node == 0) {
    		LM_ERR("Failed to add username node\n");
    		goto error;
    	}

    	str null = str_init("NULL"), head = {0,0}, tail = {0,0};
    	head = hier->root->pu == NULL ? null : hier->root->pu->etag;
    	if (add_mi_attr(hier->username_node, MI_DUP_VALUE, "head", 4, head.s , head.len) == 0) {
        	LM_ERR("Failed to add head attribute\n");
        	goto error;
        }
    	tail = hier->root->tail == NULL ? null : hier->root->tail->etag;
    	if (add_mi_attr(hier->username_node, MI_DUP_VALUE, "tail", 4, tail.s , tail.len) == 0) {
        	LM_ERR("Failed to add tail attribute\n");
        	goto error;
        }
    }

	if ((node = add_mi_node_child(hier->username_node, MI_DUP_VALUE, "etag", 4, pu->etag.s, pu->etag.len)) == 0) {
	    LM_ERR("Failed to add etag node\n");
	    goto error;
	}
    if (addf_mi_attr(node, 0, "expires", 7, "%d", pu->expires) == 0) {
    	LM_ERR("Failed to add expires attribute\n");
    	goto error;
    }

	/*
	if (add_mi_node_child(node, 0, "body", 4, pu->body.s, pu->body.len) == 0) {
	    LM_ERR("Failed to add body\n");
	    goto error;
	}
	*/

    return 0;
 error:
    return -1;
}

struct mi_root *mi_dbk_presentity_dump(struct mi_root *cmd_tree, void *param) {
	int i;
	dbk_presentity_t *pu;
	struct mi_root *rpl_tree = NULL;
	struct mi_node *rpl = NULL;
	dbk_presentity_print_t hier;
	memset(&hier, 0, sizeof(dbk_presentity_print_t));

	rpl_tree = init_mi_tree(200, MI_SSTR(MI_OK));
	if (rpl_tree == 0)
		return 0;
	rpl = &rpl_tree->node;

	for (i = 0; i < dbk_presentity_phtable_size; i++) {
		lock_get(&dbk_presentity_phtable[i].lock);
		hier.root = &dbk_presentity_phtable[i];
		for (pu = dbk_presentity_phtable[i].pu; pu; pu = pu->next) {
			if (dbk_mi_print_presentity(rpl, pu, &hier) != 0)
				goto error;
		}
		lock_release(&dbk_presentity_phtable[i].lock);
	}

	return rpl_tree;

	error:
	lock_release(&dbk_presentity_phtable[i].lock);
	if(rpl_tree)
		free_mi_tree(rpl_tree);
	LM_ERR("Failed to print presentity htable\n");
	return 0;
}
#define DBK_DBOP(i) (_op && _op[i] ? _op[i] : "=")

int copy_column(str* Col, str* StrValue) {
	Col->s = (char *) pkg_malloc(StrValue->len + 1);
	if (Col->s == NULL ) {
		LM_ERR("No more package memory\n");
		return 0;
	}
	memcpy(Col->s, StrValue->s, StrValue->len);
	Col->len = StrValue->len;
	Col->s[Col->len] = '\0';
	return 1;
}

int dbk_presentity_query_expired(db1_res_t ** _r, int expires) {
    db1_res_t *db_res = db_new_result();
	dbk_presentity_t *pu_prev = NULL;
	dbk_presentity_t *pu;
	int i;

	if (db_res == NULL) {
    	LM_ERR("no memory left\n");
    	return -1;
    }


	for (i = 0; i < dbk_presentity_phtable_size; i++) {
		lock_get(&dbk_presentity_phtable[i].lock);
		while( (pu = dbk_presentity_search_expired(i, expires, &pu_prev)) != NULL) {
			if (pu_prev) {
				pu_prev->next = pu->next;
			} else {
				dbk_presentity_phtable[i].pu = pu->next;
			}
			if(pu == dbk_presentity_phtable[i].tail)
				dbk_presentity_phtable[i].tail = pu_prev;
			LM_DBG("deleting expired presentity %.*s , %.*s, %.*s, %.*s, %d , %d\n",
					pu->event.len, pu->event.s,
					pu->domain.len, pu->domain.s,
					pu->username.len, pu->username.s,
					pu->sender.len, pu->sender.s,
					pu->received_time, pu->expires
					);
			dbk_free_presentity(pu);
		}
		lock_release(&dbk_presentity_phtable[i].lock);
	}


    RES_ROW_N(db_res) = 0;
    *_r = db_res;
    return 0;
}




int dbk_presentity_query(const db1_con_t * _h, const db_key_t * _k,
		const db_op_t * _op, const db_val_t * _v, const db_key_t * _c, int _n, int _nc,
		const db_key_t _o, db1_res_t ** _r) {

	str username = { 0, 0 };
	str user = {0, 0};
	str domain = { 0, 0 };
	str event = { 0, 0 }, emptyString = {0, 0}, body = { 0, 0 };
	int i;
	unsigned int hash_code;
	str pres_uri;
	char pres_uri_buf[1024];
	char pres_body_buf[1024];
	char user_buf[120];
	dbk_presentity_t *pu, *pu_iterator, *pu_empty = NULL;
	db1_res_t *db_res = NULL;
	int col;
	int row_cnt = 0;
	int expires = 0;

	for (i = 0; i < _n; i++) {
		if (_k[i]->len == str_username_col.len
				&& strncmp(_k[i]->s, str_username_col.s, str_username_col.len) == 0) {
			username = _v[i].val.str_val;
		} else if (_k[i]->len == str_domain_col.len
				&& strncmp(_k[i]->s, str_domain_col.s, str_domain_col.len) == 0) {
			domain = _v[i].val.str_val;
		} else if (_k[i]->len == str_event_col.len
				&& strncmp(_k[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = _v[i].val.str_val;
		} else if (_k[i]->len == str_expires_col.len
				&& strncmp(_k[i]->s, str_expires_col.s, str_expires_col.len) == 0
				&& _op && !strncmp(_op[i], "<", 1)
				) {
	        expires = _v[i].val.int_val;
	    }


		if (_v[i].type == DB1_STR) {
		    LM_DBG("presence query field %s %s %.*s ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
			   _v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_BLOB) {
		    LM_DBG("presence query field %s %s %.*s ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
			   _v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_INT) {
		    LM_DBG("presence query field %s %s %i ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
			   _v[i].val.int_val);
		} else {
		    LM_DBG("presence query other field %s %s %i ", _k[i]->s, (_op && _op[i] ? _op[i] : "="), _v[i].type);
		}
	}

	for (i = 0; i < _nc; i++) {
	    LM_DBG("presence query return field %s", _c[i]->s);
	}

	if (_n == 2 && _k[0]->len == str_expires_col.len
			&& strncmp(_k[0]->s, str_expires_col.s, str_expires_col.len) == 0) {
		LM_DBG("dbk_presence_query goes to dbk_presence_query_expired\n");
		return dbk_presentity_query_expired(_r, expires);
	}

	if (!username.len || !domain.len) {
		LM_ERR("Unsupported query - expected a query after username and domain\n");
		return -1;
	}

	sprintf(pres_uri_buf, "%.*s:%.*s@%.*s", event.len, event.s, username.len, username.s, domain.len, domain.s);
	pres_uri.s = pres_uri_buf;
	pres_uri.len = strlen(pres_uri_buf);

	db_res = db_new_result();
	if (db_res == NULL ) {
		LM_ERR("no memory left\n");
		return -1;
	}
	RES_ROW_N(db_res) = 0;

	/* search in hash_table */
	hash_code = core_hash(&pres_uri, NULL, dbk_presentity_phtable_size);
	lock_get(&dbk_presentity_phtable[hash_code].lock);
	pu = dbk_presentity_search(hash_code, &event, &domain, &username, 0, 0);

	if (pu == NULL ) {
		if(dbk_create_empty_dialog != 0 && strncmp(event.s, str_event_dialog.s, event.len) == 0) {
			sprintf(user_buf, "sip:%.*s@%*.s",  username.len, username.s,domain.len, domain.s);
			user.s = user_buf;
			user.len = strlen(user_buf);
			sprintf(pres_body_buf, DIALOGINFO_EMPTY_BODY, user.len, user.s);
			body.s = pres_body_buf;
			body.len = strlen(pres_body_buf);
			pu = pu_empty = dbk_presentity_htable_new(&event, &domain, &username, &emptyString, &emptyString, &body, 0, 0);
		} else {
			LM_DBG("No dialog info found for user [%.*s]\n", pres_uri.len, pres_uri.s);
			lock_release(&dbk_presentity_phtable[hash_code].lock);
			*_r = db_res;
			return 0;
		}
	}

	LM_DBG("Found presentity record\n");

	for (pu_iterator = pu,row_cnt = 0 ; pu_iterator; pu_iterator = pu_iterator->next, row_cnt++);

	LM_DBG("The presentity %.*s has %d active records\n", pres_uri.len, pres_uri.s, row_cnt);

	RES_COL_N(db_res) = _nc;
	RES_ROW_N(db_res) = row_cnt;

	if (db_allocate_rows(db_res) < 0) {
		LM_ERR("Could not allocate rows.\n");
		goto error1;
	}

	if (db_allocate_columns(db_res, RES_COL_N(db_res)) != 0) {
		LM_ERR("Could not allocate columns\n");
		goto error1;
	}

	for (col = 0; col < RES_COL_N(db_res); col++) {
		RES_NAMES(db_res)[col] = (str *) pkg_malloc(sizeof(str));

		if (!RES_NAMES(db_res)[col]) {
			LM_ERR("no private memory left\n");
			RES_COL_N(db_res) = col;
			db_free_columns(db_res);
			goto error1;
		}

		LM_DBG("Allocated %lu bytes for RES_NAMES[%d] at %p\n",
				(unsigned long)sizeof(str), col, RES_NAMES(db_res)[col]);

		RES_NAMES(db_res)[col]->s = _c[col]->s;
		RES_NAMES(db_res)[col]->len = _c[col]->len;
		RES_TYPES(db_res)[col] = DB1_STR;

		LM_DBG("RES_NAMES(%p)[%d]=[%.*s]\n", RES_NAMES(db_res)[col], col,
				RES_NAMES(db_res)[col]->len, RES_NAMES(db_res)[col]->s);
	}

	for (i = 0, pu_iterator = pu; pu_iterator; i++, pu_iterator = pu_iterator->next) {
		if (db_allocate_row(db_res, &(RES_ROWS(db_res)[i])) != 0) {
			LM_ERR("presentity query Could not allocate row.\n");
			RES_ROW_N(db_res) = i;
			goto error1;
		}

		/* complete the row with the columns */
		for (col = 0; col < _nc; col++) {

			RES_ROWS(db_res)[i].values[col].type = DB1_STR;

			if (strncmp(_c[col]->s, "body", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->body))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else if (strncmp(_c[col]->s, "domain", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->domain))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else if (strncmp(_c[col]->s, "username", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->username))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else if (strncmp(_c[col]->s, "event", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->event))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else if (strncmp(_c[col]->s, "etag", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->etag))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else if (strncmp(_c[col]->s, "sender", _c[col]->len) == 0) {
				if(!copy_column(&RES_ROWS(db_res)[i].values[col].val.str_val, &pu_iterator->sender))
					goto error1;
				RES_ROWS(db_res)[i].values[col].free = 1;
				RES_ROWS(db_res)[i].values[col].nul = 0;
			} else {
				RES_ROWS(db_res)[i].values[col].val.str_val.s = "";
				RES_ROWS(db_res)[i].values[col].val.str_val.len = 0;
				RES_ROWS(db_res)[i].values[col].free = 0;
				RES_ROWS(db_res)[i].values[col].nul = 1;
			}

			LM_DBG("queryres row %d, %.*s => %.*s\n",i,
					RES_NAMES(db_res)[col]->len,
					RES_NAMES(db_res)[col]->s,
					RES_ROWS(db_res)[i].values[col].val.str_val.len,
					RES_ROWS(db_res)[i].values[col].val.str_val.s);
		}
	}

	lock_release(&dbk_presentity_phtable[hash_code].lock);
	LM_DBG("Returned [%d] rows\n", row_cnt);
	*_r = db_res;
	if(pu_empty)
		dbk_free_presentity(pu_empty);
	return 0;

	error1:
	if(pu_empty)
		dbk_free_presentity(pu_empty);

	lock_release(&dbk_presentity_phtable[hash_code].lock);

	db_free_result(db_res);

	return -1;
}

int dbk_presentity_new(const db1_con_t * _h, const db_key_t * db_col,
		const db_val_t * db_val, const int _n) {

	str event = {0, 0}, domain = {0, 0} , username = {0, 0}, etag = {0, 0};
	str sender = {0, 0}, pres_uri = {0, 0}, body = {0, 0};
	int i, expires = 0, received_time = 0;
	char pres_uri_buf[1024];
	unsigned int hash_code;

	for (i = 0; i < _n; i++) {

		if (db_col[i]->len == str_username_col.len
				&& strncmp(db_col[i]->s, str_username_col.s, str_username_col.len) == 0) {
			username = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_domain_col.len
				&& strncmp(db_col[i]->s, str_domain_col.s, str_domain_col.len) == 0) {
			domain = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_event_col.len
				&& strncmp(db_col[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_etag_col.len
				&& strncmp(db_col[i]->s, str_etag_col.s, str_etag_col.len) == 0) {
			etag = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_body_col.len
				&& strncmp(db_col[i]->s, str_body_col.s, str_body_col.len) == 0) {
			body = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_sender_col.len
				&& strncmp(db_col[i]->s, str_sender_col.s, str_sender_col.len) == 0) {
			sender = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_expires_col.len
				&& strncmp(db_col[i]->s, str_expires_col.s, str_expires_col.len) == 0) {
			expires = db_val[i].val.int_val;
		} else if (db_col[i]->len == str_received_time_col.len
				&& strncmp(db_col[i]->s, str_received_time_col.s, str_received_time_col.len) == 0) {
			received_time = db_val[i].val.int_val;
		}


		if (db_val[i].type == DB1_STR) {
			LM_DBG("presentity new field %s = %.*s ", db_col[i]->s, db_val[i].val.str_val.len, db_val[i].val.str_val.s);
		} else if (db_val[i].type == DB1_BLOB) {
			LM_DBG("presentity new field %s = %.*s ", db_col[i]->s, db_val[i].val.str_val.len, db_val[i].val.str_val.s);
		} else if (db_val[i].type == DB1_INT) {
			LM_DBG("presentity new field %s = %i ", db_col[i]->s, db_val[i].val.int_val);
		} else {
			LM_DBG("presence new other field %s = %i ", db_col[i]->s,db_val[i].type);
		}
	}

	sprintf(pres_uri_buf, "%.*s:%.*s@%.*s", event.len, event.s, username.len, username.s, domain.len, domain.s);
	pres_uri.s = pres_uri_buf;
	pres_uri.len = strlen(pres_uri_buf);

	hash_code = core_hash(&pres_uri, NULL, dbk_presentity_phtable_size);
	LM_DBG("inserting presentity %.*s , %.*s, %.*s, %.*s, %d, %d \n",
			event.len, event.s,
			domain.len, domain.s,
			username.len, username.s,
			sender.len, sender.s,
			received_time, expires
			);

	dbk_presentity_htable_delete(&event, &domain, &username, &etag, &sender, &body, received_time, expires, hash_code);
	dbk_presentity_htable_insert(&event, &domain, &username, &etag, &sender, &body, received_time, expires, hash_code);

	return 0;
}

int dbk_presentity_new_ex(str *event, str *domain, str* username, str* etag, str* sender, str* body, int expires)
{

	str pres_uri = {0, 0};
	int received_time = (int) time(NULL);
	char pres_uri_buf[1024];
	unsigned int hash_code;


	sprintf(pres_uri_buf, "%.*s:%.*s@%.*s", event->len, event->s, username->len, username->s, domain->len, domain->s);
	pres_uri.s = pres_uri_buf;
	pres_uri.len = strlen(pres_uri_buf);

	hash_code = core_hash(&pres_uri, NULL, dbk_presentity_phtable_size);
	LM_DBG("inserting presentity %.*s , %.*s, %.*s, %.*s, %d, %d \n",
			event->len, event->s,
			domain->len, domain->s,
			username->len, username->s,
			sender->len, sender->s,
			received_time, expires
			);

	dbk_presentity_htable_delete(event, domain, username, etag, sender, body, received_time, received_time+expires, hash_code);
	dbk_presentity_htable_insert(event, domain, username, etag, sender, body, received_time, received_time+expires, hash_code);

	return 0;
}

int dbk_presentity_update(const db1_con_t * _h, const db_key_t * _k, const db_op_t * _op, const db_val_t * _v, const db_key_t * _uk, const db_val_t * _uv, const int _n, const int _un)
{
	str event = {0, 0}, domain = {0, 0} , username = {0, 0}, etag = {0, 0};
	str sender = {0, 0}, pres_uri = {0, 0}, body = {0, 0};
	int i, expires = 0, received_time = 0;
	char pres_uri_buf[1024];
	unsigned int hash_code;

	for (i = 0; i < _n; i++) {

		if (_k[i]->len == str_username_col.len
				&& strncmp(_k[i]->s, str_username_col.s, str_username_col.len) == 0) {
			username = _v[i].val.str_val;
		} else if (_k[i]->len == str_domain_col.len
				&& strncmp(_k[i]->s, str_domain_col.s, str_domain_col.len) == 0) {
			domain = _v[i].val.str_val;
		} else if (_k[i]->len == str_event_col.len
				&& strncmp(_k[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = _v[i].val.str_val;
		} else if (_k[i]->len == str_etag_col.len
				&& strncmp(_k[i]->s, str_etag_col.s, str_etag_col.len) == 0) {
			etag = _v[i].val.str_val;
		} else if (_k[i]->len == str_body_col.len
				&& strncmp(_k[i]->s, str_body_col.s, str_body_col.len) == 0) {
			body = _v[i].val.str_val;
		} else if (_k[i]->len == str_sender_col.len
				&& strncmp(_k[i]->s, str_sender_col.s, str_sender_col.len) == 0) {
			sender = _v[i].val.str_val;
		} else if (_k[i]->len == str_expires_col.len
				&& strncmp(_k[i]->s, str_expires_col.s, str_expires_col.len) == 0) {
			expires = _v[i].val.int_val;
		} else if (_k[i]->len == str_received_time_col.len
				&& strncmp(_k[i]->s, str_received_time_col.s, str_received_time_col.len) == 0) {
			received_time = _v[i].val.int_val;
		}


		if (_v[i].type == DB1_STR) {
			LM_DBG("presentity update key %s %s %.*s ", _k[i]->s, DBK_DBOP(i), _v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_BLOB) {
			LM_DBG("presentity update key %s %s %.*s ", _k[i]->s, DBK_DBOP(i), _v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_INT) {
			LM_DBG("presentity update key %s %s %i ", _k[i]->s, DBK_DBOP(i), _v[i].val.int_val);
		} else {
			LM_DBG("presence update other key %s %s %i ", _k[i]->s, DBK_DBOP(i),_v[i].type);
		}
	}

	for (i = 0; i < _un; i++) {

		if (_uk[i]->len == str_username_col.len
				&& strncmp(_uk[i]->s, str_username_col.s, str_username_col.len) == 0) {
			username = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_domain_col.len
				&& strncmp(_uk[i]->s, str_domain_col.s, str_domain_col.len) == 0) {
			domain = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_event_col.len
				&& strncmp(_uk[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_etag_col.len
				&& strncmp(_uk[i]->s, str_etag_col.s, str_etag_col.len) == 0) {
			etag = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_body_col.len
				&& strncmp(_uk[i]->s, str_body_col.s, str_body_col.len) == 0) {
			body = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_sender_col.len
				&& strncmp(_uk[i]->s, str_sender_col.s, str_sender_col.len) == 0) {
			sender = _uv[i].val.str_val;
		} else if (_uk[i]->len == str_expires_col.len
				&& strncmp(_uk[i]->s, str_expires_col.s, str_expires_col.len) == 0) {
			expires = _uv[i].val.int_val;
		} else if (_uk[i]->len == str_received_time_col.len
				&& strncmp(_uk[i]->s, str_received_time_col.s, str_received_time_col.len) == 0) {
			received_time = _uv[i].val.int_val;
		}


		if (_uv[i].type == DB1_STR) {
			LM_DBG("presentity update field %s = %.*s ", _uk[i]->s, _uv[i].val.str_val.len, _uv[i].val.str_val.s);
		} else if (_uv[i].type == DB1_BLOB) {
			LM_DBG("presentity update field %s = %.*s ", _uk[i]->s, _uv[i].val.str_val.len, _uv[i].val.str_val.s);
		} else if (_uv[i].type == DB1_INT) {
			LM_DBG("presentity update field %s = %i ", _uk[i]->s, _uv[i].val.int_val);
		} else {
			LM_DBG("presence update other field %s = %i ", _uk[i]->s,_uv[i].type);
		}
	}

	sprintf(pres_uri_buf, "%.*s:%.*s@%.*s", event.len, event.s, username.len, username.s, domain.len, domain.s);
	pres_uri.s = pres_uri_buf;
	pres_uri.len = strlen(pres_uri_buf);

	hash_code = core_hash(&pres_uri, NULL, dbk_presentity_phtable_size);
	LM_DBG("updating presentity %.*s , %.*s, %.*s, %.*s, %d , %d\nBody: %.*s\n",
			event.len, event.s,
			domain.len, domain.s,
			username.len, username.s,
			sender.len, sender.s,
			received_time, expires,
			body.len, body.s
			);

//	dbk_presentity_htable_delete(&event, &domain, &username, &etag, &sender, &body, received_time, expires, hash_code);
//	dbk_presentity_htable_insert(&event, &domain, &username, &etag, &sender, &body, received_time, expires, hash_code);

	return 0;

}

int dbk_presentity_delete(const db1_con_t * _h, const db_key_t * _k,
		const db_op_t * _op, const db_val_t * _v, const int _n) {

	int i;
	int expires = 0;
	dbk_presentity_t *pu_prev = NULL;
	dbk_presentity_t *pu;

	for (i = 0; i < _n; i++) {

        if (_k[i]->len == str_expires_col.len
			&& strncmp(_k[i]->s, str_expires_col.s, str_expires_col.len) == 0
			&& _op && !strncmp(_op[i], "<", 1) ) {
        		expires = _v[i].val.int_val;
        }

		if (_v[i].type == DB1_STR) {
			LM_DBG("presentity delete field %s %s %.*s ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
					_v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_BLOB) {
			LM_DBG("presentity delete field %s %s %.*s ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
					_v[i].val.str_val.len, _v[i].val.str_val.s);
		} else if (_v[i].type == DB1_INT) {
			LM_DBG("presentity delete field %s %s %i ", _k[i]->s, (_op && _op[i] ? _op[i] : "="),
					_v[i].val.int_val);
		} else {
			LM_DBG("presence delete other field %s %s %i ", _k[i]->s, (_op && _op[i] ? _op[i] : "="), _v[i].type);
		}

	}

	if(expires > 0) {
		for (i = 0; i < dbk_presentity_phtable_size; i++) {
			lock_get(&dbk_presentity_phtable[i].lock);
			while( (pu = dbk_presentity_search_expired(i, expires, &pu_prev)) != NULL) {
				if (pu_prev) {
					pu_prev->next = pu->next;
				} else {
					dbk_presentity_phtable[i].pu = pu->next;
				}
				if(pu == dbk_presentity_phtable[i].tail)
					dbk_presentity_phtable[i].tail = pu_prev;

				LM_DBG("deleting presentity %.*s , %.*s, %.*s, %.*s, %d , %d\n",
						pu->event.len, pu->event.s,
						pu->domain.len, pu->domain.s,
						pu->username.len, pu->username.s,
						pu->sender.len, pu->sender.s,
						pu->received_time, pu->expires
						);

				dbk_free_presentity(pu);
			}
			lock_release(&dbk_presentity_phtable[i].lock);
		}
	}

	return 0;
}
