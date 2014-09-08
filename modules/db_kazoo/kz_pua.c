#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <json/json.h>
#include "../../mem/mem.h"
#include "../../timer_proc.h"
#include "../../sr_module.h"
#include "../../lib/kmi/mi.h"
#include "../presence/bind_presence.h"
#include "../../pvar.h"

#include "../pua/pua.h"
#include "../pua/pua_bind.h"
#include "../pua/send_publish.h"

#include "kz_pua.h"
#include "defs.h"
#include "const.h"
#include "dbase.h"
#include "presentity.h"

extern pua_api_t kz_pua_api;

extern int dbk_dialog_expires;
extern int dbk_presence_expires;
extern int dbk_mwi_expires;
extern int dbk_include_entity;
extern int dbk_pua_mode;

extern db1_con_t *kz_pa_db;
extern db_func_t kz_pa_dbf;
extern str kz_presentity_table;


int kz_pua_update_presentity(str* event, str* realm, str* user, str* etag, str* sender, str* body, int expires, int reset)
{
	db_key_t query_cols[12];
	db_op_t  query_ops[12];
	db_val_t query_vals[12];
	int n_query_cols = 0;
	int ret = -1;
	int use_replace = 1;

	query_cols[n_query_cols] = &str_event_col;
	query_ops[n_query_cols] = OP_EQ;
	query_vals[n_query_cols].type = DB1_STR;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *event;
	n_query_cols++;

	query_cols[n_query_cols] = &str_domain_col;
	query_ops[n_query_cols] = OP_EQ;
	query_vals[n_query_cols].type = DB1_STR;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *realm;
	n_query_cols++;

	query_cols[n_query_cols] = &str_username_col;
	query_ops[n_query_cols] = OP_EQ;
	query_vals[n_query_cols].type = DB1_STR;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *user;
	n_query_cols++;

	query_cols[n_query_cols] = &str_etag_col;
	query_ops[n_query_cols] = OP_EQ;
	query_vals[n_query_cols].type = DB1_STR;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *etag;
	n_query_cols++;

	query_cols[n_query_cols] = &str_sender_col;
	query_vals[n_query_cols].type = DB1_STR;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *sender;
	n_query_cols++;

	query_cols[n_query_cols] = &str_body_col;
	query_vals[n_query_cols].type = DB1_BLOB;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.str_val = *body;
	n_query_cols++;

	query_cols[n_query_cols] = &str_received_time_col;
	query_vals[n_query_cols].type = DB1_INT;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.int_val = (int)time(NULL);
	n_query_cols++;

	query_cols[n_query_cols] = &str_expires_col;
	query_vals[n_query_cols].type = DB1_INT;
	query_vals[n_query_cols].nul = 0;
	query_vals[n_query_cols].val.int_val = expires+(int)time(NULL);
	n_query_cols++;

	if (kz_pa_dbf.use_table(kz_pa_db, &kz_presentity_table) < 0)
	{
		LM_ERR("unsuccessful use_table\n");
		goto error;
	}

	if (kz_pa_dbf.replace == NULL || reset > 0)
	{
		use_replace = 0;
		LM_DBG("using delete/insert instead of replace\n");
	}

	if (kz_pa_dbf.start_transaction)
	{
		if (kz_pa_dbf.start_transaction(kz_pa_db, DB_LOCKING_WRITE) < 0)
		{
			LM_ERR("in start_transaction\n");
			goto error;
		}
	}

	if(use_replace) {
		if (kz_pa_dbf.replace(kz_pa_db, query_cols, query_vals, n_query_cols, 4, 0) < 0)
		{
			LM_ERR("replacing record in database\n");
			if (kz_pa_dbf.abort_transaction)
			{
				if (kz_pa_dbf.abort_transaction(kz_pa_db) < 0)
					LM_ERR("in abort_transaction\n");
			}
			goto error;
		}
	} else {
		if (kz_pa_dbf.delete(kz_pa_db, query_cols, query_ops, query_vals, 4-reset) < 0)
		{
			LM_ERR("deleting record in database\n");
			if (kz_pa_dbf.abort_transaction)
			{
				if (kz_pa_dbf.abort_transaction(kz_pa_db) < 0)
					LM_ERR("in abort_transaction\n");
			}
			goto error;
		}
		if (kz_pa_dbf.insert(kz_pa_db, query_cols, query_vals, n_query_cols) < 0)
		{
			LM_ERR("replacing record in database\n");
			if (kz_pa_dbf.abort_transaction)
			{
				if (kz_pa_dbf.abort_transaction(kz_pa_db) < 0)
					LM_ERR("in abort_transaction\n");
			}
			goto error;
		}
	}

	if (kz_pa_dbf.end_transaction)
	{
		if (kz_pa_dbf.end_transaction(kz_pa_db) < 0)
		{
			LM_ERR("in end_transaction\n");
			goto error;
		}
	}

error:

	return ret;
}



int kz_pua_publish_presence(struct json_object *json_obj) {
    int ret = 1;
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str state = { 0, 0 };
    str direction = { 0, 0 };
    char body[4096];
    str presence_body = { 0, 0 };
    str activity = str_init("");
    str note = str_init("Idle");
    str status = str_presence_status_online;
    int expires = dbk_presence_expires;

    json_extract_field(BLF_JSON_FROM, from_user);
    json_extract_field(BLF_JSON_TO, to_user);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

    struct json_object* ExpiresObj = json_object_object_get(json_obj, BLF_JSON_EXPIRES);
    if(ExpiresObj != NULL) {
    	expires = json_object_get_int(ExpiresObj);
    }

    if (!from_user.len || !to_user.len || !state.len) {
    	LM_ERR("missing one of From / To / State\n");
    	goto error;
    }

    if (!strcmp(state.s, "early")) {
    	note = str_presence_note_busy;
    	activity = str_presence_act_busy;

    } else if (!strcmp(state.s, "confirmed")) {
    	note = str_presence_note_otp;
    	activity = str_presence_act_otp;

    } else if (!strcmp(state.s, "offline")) {
    	note = str_presence_note_offline;
    	status = str_presence_status_offline;

    } else {
    	note = str_presence_note_idle;
    }


    sprintf(body, PRESENCE_BODY, from_user.s, callid.s, status.s, note.s, activity.s, note.s);

    presence_body.s = body;
    presence_body.len = strlen(body);


    publ_info_t publ1;
    memset(&publ1, 0, sizeof(publ_info_t));
	publ1.pres_uri = &from_user;
    publ1.id = from_user;
    publ1.body = &presence_body;
    publ1.event = PRESENCE_EVENT;
    publ1.flag |= UPDATE_TYPE;
    publ1.expires = dbk_presence_expires;

    ret = kz_pua_api.send_publish(&publ1);

    if (ret < 0) {
    	LM_ERR("Failed to process dialoginfo update command\n");
    	ret = -1;
    }

    return ret;

 error:
    return -1;

}

int kz_pua_publish_mwi(struct json_object *json_obj) {
    int ret = 1;
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str mwi_user = { 0, 0 }, mwi_waiting = { 0, 0 },
        mwi_new = { 0, 0 }, mwi_saved = { 0, 0 },
        mwi_urgent = { 0, 0 }, mwi_urgent_saved = { 0, 0 },
        mwi_account = { 0, 0 }, mwi_body = { 0, 0 };

    char *body = (char *)pkg_malloc(MWI_BODY_BUFFER_SIZE);
    if(body == NULL) {
    	LM_ERR("Error allocating buffer for publish\n");
    	ret = -1;
    	goto error;
    }

    json_extract_field(BLF_JSON_FROM, from_user);
    json_extract_field(BLF_JSON_TO, to_user);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);

    json_extract_field(MWI_JSON_TO, mwi_user);
    json_extract_field(MWI_JSON_WAITING, mwi_waiting);
    json_extract_field(MWI_JSON_NEW, mwi_new);
    json_extract_field(MWI_JSON_SAVED, mwi_saved);
    json_extract_field(MWI_JSON_URGENT, mwi_urgent);
    json_extract_field(MWI_JSON_URGENT_SAVED, mwi_urgent_saved);
    json_extract_field(MWI_JSON_ACCOUNT, mwi_account);

    sprintf(body, MWI_BODY, mwi_waiting.len, mwi_waiting.s,
	    mwi_account.len, mwi_account.s, mwi_new.len, mwi_new.s,
	    mwi_saved.len, mwi_saved.s, mwi_urgent.len, mwi_urgent.s,
	    mwi_urgent_saved.len, mwi_urgent_saved.s);

    mwi_body.s = body;
    mwi_body.len = strlen(body);


    int size = sizeof(publ_info_t) + (sizeof(str) * 3 ) + ((mwi_body.len + to_user.len + 2) * sizeof(char));
    publ_info_t * publ1 = (publ_info_t*)pkg_malloc(size);
    if(publ1 == NULL) {
    	LM_ERR("Error allocating memory to publish");
    	ret = -1;
    	goto error;
    }

    memset(publ1, 0, size);

    str* str1 = (str*) (publ1 + sizeof(publ_info_t));
    str* str2 = (str*) (publ1 + sizeof(publ_info_t) + sizeof(str));
    char *ptr = (char*) (publ1 + sizeof(publ_info_t) + (sizeof(str)*2));

    str1->s = ptr;
    str1->len = to_user.len;
    strncpy(ptr, to_user.s, to_user.len);
    ptr += to_user.len;
    *ptr = '\0';
    ptr++;
    str2->s = ptr;
    str2->len = mwi_body.len;
    strncpy(ptr, mwi_body.s, mwi_body.len);
    ptr += mwi_body.len;
    *ptr = '\0';

   	publ1->pres_uri = str1;
   	publ1->id = *str1;

    publ1->body = str2;


    /* send_publish */
    publ1->event = MSGSUM_EVENT;
    publ1->flag |= UPDATE_TYPE;
    publ1->expires = dbk_mwi_expires;

    ret = kz_pua_api.send_publish(publ1);

    if (ret < 0) {
    	LM_ERR("Failed to process dialoginfo update command\n");
    	ret = -1;
    }

 error:

   if(body)
	  pkg_free(body);

   if(publ1)
	  pkg_free(publ1);

   return ret;
}

int kz_pua_publish_dialoginfo(struct json_object *json_obj) {
    int ret = 1;
    str from = { 0, 0 }, to = { 0, 0 };
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str from_realm = { 0, 0 }, to_realm = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str state = { 0, 0 };
    str direction = { 0, 0 };
    str extra_headers = {0, 0};
    str header_name = str_init("Sender");
    char sender_buf[1024];
    char extra_buf[1024];
    str sender = {0, 0};
    str dialoginfo_body = {0 , 0};
    int expires = dbk_dialog_expires;

    char *body = (char *)pkg_malloc(DIALOGINFO_BODY_BUFFER_SIZE);
    if(body == NULL) {
    	LM_ERR("Error allocating buffer for publish\n");
    	ret = -1;
    	goto error;
    }


    json_extract_field(BLF_JSON_FROM, from);
    json_extract_field(BLF_JSON_FROM_USER, from_user);
    json_extract_field(BLF_JSON_FROM_REALM, from_realm);
    json_extract_field(BLF_JSON_TO, to);
    json_extract_field(BLF_JSON_TO_USER, to_user);
    json_extract_field(BLF_JSON_TO_REALM, to_realm);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

    struct json_object* ExpiresObj = json_object_object_get(json_obj, BLF_JSON_EXPIRES);
    if(ExpiresObj != NULL) {
    	expires = json_object_get_int(ExpiresObj);
    }

    if (!from_user.len || !to_user.len || !state.len) {
    	LM_ERR("missing one of From / To / State\n");
		goto error;
    }

    if(callid.len) {

    	if(dbk_include_entity) {
        sprintf(body, DIALOGINFO_BODY,
        		from.len, from.s,
        		callid.len, callid.s,
        		callid.len, callid.s,
        		fromtag.len, fromtag.s,
        		totag.len, totag.s,
        		direction.len, direction.s,
        		state.len, state.s,
        		from_user.len, from_user.s,
        		from.len, from.s,
        		to_user.len, to_user.s,
        		to.len, to.s
        		);
    	} else {

        sprintf(body, DIALOGINFO_BODY_2,
        		from.len, from.s,
        		callid.len, callid.s,
        		callid.len, callid.s,
        		fromtag.len, fromtag.s,
        		totag.len, totag.s,
        		direction.len, direction.s,
        		state.len, state.s,
        		from_user.len, from_user.s,
        		to_user.len, to_user.s
        		);
    	}

    } else {
    	sprintf(body, DIALOGINFO_EMPTY_BODY, from_user.len, from_user.s);
    }

    dialoginfo_body.s = body;
    dialoginfo_body.len = strlen(body);

	/* build extra_headers with Sender*/
	/* this is needed to keep presentity */
	/* for each call-id */

    sprintf(sender_buf, "sip:%s",callid.s);
    sender.s = sender_buf;
    sender.len = strlen(sender_buf);

   	extra_headers.s= extra_buf;
   	memcpy(extra_headers.s, header_name.s, header_name.len);
   	extra_headers.len= header_name.len;
   	memcpy(extra_headers.s+extra_headers.len,": ",2);
   	extra_headers.len+= 2;
   	memcpy(extra_headers.s+ extra_headers.len, sender.s, sender.len);
   	extra_headers.len+= sender.len;
   	memcpy(extra_headers.s+ extra_headers.len, CRLF, CRLF_LEN);
   	extra_headers.len+= CRLF_LEN;

   	/* end build extra_headers with Sender*/


    int size = sizeof(publ_info_t) + (sizeof(str) * 3 ) + ((dialoginfo_body.len + from.len + extra_headers.len + 3) * sizeof(char));
    publ_info_t * publ1 = (publ_info_t*)pkg_malloc(size);
    if(publ1 == NULL) {
    	LM_ERR("Error allocating memory to publish");
    	ret = -1;
    	goto error;
    }

    memset(publ1, 0, size);

    str* str1 = (str*) (publ1 + sizeof(publ_info_t));
    str* str2 = (str*) (publ1 + sizeof(publ_info_t) + sizeof(str));
    str* str3 = (str*) (publ1 + sizeof(publ_info_t) + (sizeof(str)*2));
    char *ptr = (char*) (publ1 + sizeof(publ_info_t) + (sizeof(str)*3));

    str1->s = ptr;
    str1->len = from.len;
    strncpy(ptr, from.s, from.len);
    ptr += from.len;
    *ptr = '\0';
    ptr++;
    str2->s = ptr;
    str2->len = dialoginfo_body.len;
    strncpy(ptr, dialoginfo_body.s, dialoginfo_body.len);
    ptr += dialoginfo_body.len;
    *ptr = '\0';
    ptr++;
    str3->s = ptr;
    str3->len = extra_headers.len;
    strncpy(ptr, extra_headers.s, extra_headers.len);
    ptr += extra_headers.len;
    *ptr = '\0';

   	publ1->pres_uri = str1;
   	publ1->id = *str1;

    publ1->body = str2;
    publ1->event = DIALOG_EVENT;
    publ1->expires = expires;
    publ1->flag |= UPDATE_TYPE;
   	publ1->extra_headers = str3;

    ret = kz_pua_api.send_publish(publ1);

    if (ret < 0) {
    	LM_ERR("Failed to process dialoginfo update command\n");
    	ret = -1;
    }


 error:

   if(body)
	  pkg_free(body);

   if(publ1)
	  pkg_free(publ1);

 return ret;

}

int kz_pua_publish_presence_to_presentity(struct json_object *json_obj) {
    int ret = 1;
    str from = { 0, 0 }, to = { 0, 0 };
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str from_realm = { 0, 0 }, to_realm = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str state = { 0, 0 };
    str direction = { 0, 0 };
    str event = str_init("presence");
    str presence_body = { 0, 0 };
    str activity = str_init("");
    str note = str_init("Idle");
    str status = str_presence_status_online;
    int expires = dbk_presence_expires;

    char *body = (char *)pkg_malloc(PRESENCE_BODY_BUFFER_SIZE);
    if(body == NULL) {
    	LM_ERR("Error allocating buffer for publish\n");
    	ret = -1;
    	goto error;
    }

    json_extract_field(BLF_JSON_FROM, from);
    json_extract_field(BLF_JSON_FROM_USER, from_user);
    json_extract_field(BLF_JSON_FROM_REALM, from_realm);
    json_extract_field(BLF_JSON_TO, to);
    json_extract_field(BLF_JSON_TO_USER, to_user);
    json_extract_field(BLF_JSON_TO_REALM, to_realm);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

    struct json_object* ExpiresObj = json_object_object_get(json_obj, BLF_JSON_EXPIRES);
    if(ExpiresObj != NULL) {
    	expires = json_object_get_int(ExpiresObj);
    }

    if (!from_user.len || !to_user.len || !state.len) {
    	LM_ERR("missing one of From / To / State\n");
    	goto error;
    }

    if (!strcmp(state.s, "early")) {
    	note = str_presence_note_busy;
    	activity = str_presence_act_busy;

    } else if (!strcmp(state.s, "confirmed")) {
    	note = str_presence_note_otp;
    	activity = str_presence_act_otp;

    } else if (!strcmp(state.s, "offline")) {
    	note = str_presence_note_offline;
    	status = str_presence_status_offline;

    } else {
    	note = str_presence_note_idle;
    }


    sprintf(body, PRESENCE_BODY, from_user.s, callid.s, status.s, note.s, activity.s, note.s);

    presence_body.s = body;
    presence_body.len = strlen(body);

    if(dbk_pua_mode == 0) {
    	dbk_presentity_new_ex(&event, &from_realm, &from_user, &callid, &from, &presence_body, expires);
    } if(dbk_pua_mode == 1) {
    	kz_pua_update_presentity(&event, &from_realm, &from_user, &callid, &from, &presence_body, expires, 1);
    }

 error:

 if(body)
	  pkg_free(body);

 return ret;

}

int kz_pua_publish_mwi_to_presentity(struct json_object *json_obj) {
    int ret = 1;
    str event = str_init("message-summary");
    str from = { 0, 0 }, to = { 0, 0 };
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str from_realm = { 0, 0 }, to_realm = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str mwi_user = { 0, 0 }, mwi_waiting = { 0, 0 },
        mwi_new = { 0, 0 }, mwi_saved = { 0, 0 },
        mwi_urgent = { 0, 0 }, mwi_urgent_saved = { 0, 0 },
        mwi_account = { 0, 0 }, mwi_body = { 0, 0 };
    int expires = dbk_mwi_expires;

    char *body = (char *)pkg_malloc(MWI_BODY_BUFFER_SIZE);
    if(body == NULL) {
    	LM_ERR("Error allocating buffer for publish\n");
    	ret = -1;
    	goto error;
    }

    json_extract_field(BLF_JSON_FROM, from);
    json_extract_field(BLF_JSON_FROM_USER, from_user);
    json_extract_field(BLF_JSON_FROM_REALM, from_realm);
    json_extract_field(BLF_JSON_TO, to);
    json_extract_field(BLF_JSON_TO_USER, to_user);
    json_extract_field(BLF_JSON_TO_REALM, to_realm);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);

    json_extract_field(MWI_JSON_TO, mwi_user);
    json_extract_field(MWI_JSON_WAITING, mwi_waiting);
    json_extract_field(MWI_JSON_NEW, mwi_new);
    json_extract_field(MWI_JSON_SAVED, mwi_saved);
    json_extract_field(MWI_JSON_URGENT, mwi_urgent);
    json_extract_field(MWI_JSON_URGENT_SAVED, mwi_urgent_saved);
    json_extract_field(MWI_JSON_ACCOUNT, mwi_account);

    struct json_object* ExpiresObj = json_object_object_get(json_obj, BLF_JSON_EXPIRES);
    if(ExpiresObj != NULL) {
    	expires = json_object_get_int(ExpiresObj);
    }

    sprintf(body, MWI_BODY, mwi_waiting.len, mwi_waiting.s,
	    mwi_account.len, mwi_account.s, mwi_new.len, mwi_new.s,
	    mwi_saved.len, mwi_saved.s, mwi_urgent.len, mwi_urgent.s,
	    mwi_urgent_saved.len, mwi_urgent_saved.s);

    mwi_body.s = body;
    mwi_body.len = strlen(body);

    if(dbk_pua_mode == 0) {
    	dbk_presentity_new_ex(&event, &from_realm, &from_user, &callid, &from, &mwi_body, expires);
    } if(dbk_pua_mode == 1) {
    	kz_pua_update_presentity(&event, &from_realm, &from_user, &callid, &from, &mwi_body, expires, 1);
    }

 error:

   if(body)
	  pkg_free(body);


   return ret;
}

int kz_pua_publish_dialoginfo_to_presentity(struct json_object *json_obj) {
    int ret = 1;
    str from = { 0, 0 }, to = { 0, 0 };
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str from_realm = { 0, 0 }, to_realm = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str state = { 0, 0 };
    str direction = { 0, 0 };
    char sender_buf[1024];
    str sender = {0, 0};
    str dialoginfo_body = {0 , 0};
    int expires = dbk_dialog_expires;
    str event = str_init("dialog");
    int reset = 0;

    char *body = (char *)pkg_malloc(DIALOGINFO_BODY_BUFFER_SIZE);
    if(body == NULL) {
    	LM_ERR("Error allocating buffer for publish\n");
    	ret = -1;
    	goto error;
    }


    json_extract_field(BLF_JSON_FROM, from);
    json_extract_field(BLF_JSON_FROM_USER, from_user);
    json_extract_field(BLF_JSON_FROM_REALM, from_realm);
    json_extract_field(BLF_JSON_TO, to);
    json_extract_field(BLF_JSON_TO_USER, to_user);
    json_extract_field(BLF_JSON_TO_REALM, to_realm);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

    struct json_object* ExpiresObj = json_object_object_get(json_obj, BLF_JSON_EXPIRES);
    if(ExpiresObj != NULL) {
    	expires = json_object_get_int(ExpiresObj);
    }

    ExpiresObj = json_object_object_get(json_obj, "Flush-Level");
    if(ExpiresObj != NULL) {
    	reset = json_object_get_int(ExpiresObj);
    }

    if (!from_user.len || !to_user.len || !state.len) {
    	LM_ERR("missing one of From / To / State\n");
		goto error;
    }

    if(callid.len) {

    	if(dbk_include_entity) {
        sprintf(body, DIALOGINFO_BODY,
        		from.len, from.s,
        		callid.len, callid.s,
        		callid.len, callid.s,
        		fromtag.len, fromtag.s,
        		totag.len, totag.s,
        		direction.len, direction.s,
        		state.len, state.s,
        		from_user.len, from_user.s,
        		from.len, from.s,
        		to_user.len, to_user.s,
        		to.len, to.s
        		);
    	} else {

        sprintf(body, DIALOGINFO_BODY_2,
        		from.len, from.s,
        		callid.len, callid.s,
        		callid.len, callid.s,
        		fromtag.len, fromtag.s,
        		totag.len, totag.s,
        		direction.len, direction.s,
        		state.len, state.s,
        		from_user.len, from_user.s,
        		to_user.len, to_user.s
        		);
    	}

    } else {
    	sprintf(body, DIALOGINFO_EMPTY_BODY, from_user.len, from_user.s);
    }

    sprintf(sender_buf, "sip:%s",callid.s);
    sender.s = sender_buf;
    sender.len = strlen(sender_buf);

    dialoginfo_body.s = body;
    dialoginfo_body.len = strlen(body);

    if(dbk_pua_mode == 0) {
    	dbk_presentity_new_ex(&event, &from_realm, &from_user, &callid, &sender, &dialoginfo_body, expires);
    } if(dbk_pua_mode == 1) {
    	kz_pua_update_presentity(&event, &from_realm, &from_user, &callid, &sender, &dialoginfo_body, expires, reset);
    }

 error:

   if(body)
	  pkg_free(body);


 return ret;

}

int kz_pua_publish(struct sip_msg* msg, char *json) {
    str event_name = { 0, 0 }, event_package = { 0, 0 };
    struct json_object *json_obj;
    int ret = 1;

    /* extract info from json and construct xml */
    json_obj = json_tokener_parse(json);
    if (is_error(json_obj)) {
	LM_ERR("Error parsing json: %s\n",
	       json_tokener_errors[-(unsigned long)json_obj]);
	LM_ERR("%s\n", json);
	goto error;
    }

    json_extract_field(BLF_JSON_EVENT_NAME, event_name);


    if (event_name.len == 6 && strncmp(event_name.s, "update", 6) == 0) {
    	json_extract_field(BLF_JSON_EVENT_PKG, event_package);
    	if (event_package.len == str_event_dialog.len
    			&& strncmp(event_package.s, str_event_dialog.s, event_package.len) == 0) {
    		ret = kz_pua_publish_dialoginfo_to_presentity(json_obj);
    	} else if (event_package.len == str_event_message_summary.len
    			&& strncmp(event_package.s, str_event_message_summary.s, event_package.len) == 0) {
    		ret = kz_pua_publish_mwi_to_presentity(json_obj);
    	} else if (event_package.len == str_event_presence.len
    			&& strncmp(event_package.s, str_event_presence.s, event_package.len) == 0) {
    		ret = kz_pua_publish_presence_to_presentity(json_obj);
    	}
    }

    json_object_put(json_obj);
    return ret;
 error:
    return -1;
}

