#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <json/json.h>
#include "../../mem/mem.h"
#include "../../timer_proc.h"
#include "../../sr_module.h"
#include "../../lib/kmi/mi.h"
#include "../presence/bind_presence.h"
#include "../pua_dialoginfo/bind_dialoginfo.h"
#include "../presence_dialoginfo/bind_pres_dialoginfo.h"
#include "../../pvar.h"
#include "../pua/pua.h"

#include "../pua/pua_bind.h"
#include "../pua/send_publish.h"

#include "kz_pua.h"
#include "defs.h"
#include "const.h"
#include "dbase.h"

pua_api_t kz_pua_api;

extern int dbk_dialog_expires;
extern int dbk_presence_expires;
extern int dbk_mwi_expires;
extern int dbk_include_entity;


int kz_initialize_pua() {
    LM_INFO("kz_initialize_pua\n");

    /* bind to pua module */
    bind_pua_t bind_pua = (bind_pua_t) find_export("bind_pua", 1, 0);
    if (!bind_pua) {
		LM_ERR("Can't find bind pua\n");
		return -1;
    }
    if (bind_pua(&kz_pua_api) < 0) {
		LM_ERR("Can't bind to pua api\n");
		return -1;
    }

    return 0;
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

    json_extract_field(BLF_JSON_FROM, from_user);
    json_extract_field(BLF_JSON_TO, to_user);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

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
    if (!strcmp(direction.s, "inbound") ||
    		!strcmp(direction.s, "initiator")) {

	publ1.pres_uri = &from_user;
    publ1.id = from_user;

    } else {
	publ1.pres_uri = &to_user;
    publ1.id = to_user;

    }

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
    char body[1024];
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str mwi_user = { 0, 0 }, mwi_waiting = { 0, 0 },
        mwi_new = { 0, 0 }, mwi_saved = { 0, 0 },
        mwi_urgent = { 0, 0 }, mwi_urgent_saved = { 0, 0 },
        mwi_account = { 0, 0 }, mwi_body = { 0, 0 };

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

    /* send_publish */
    publ_info_t publ;
    memset(&publ, 0, sizeof(publ_info_t));
    publ.id = to_user;
    publ.pres_uri = &to_user;
    publ.body = &mwi_body;
    publ.event = MSGSUM_EVENT;
    publ.flag |= UPDATE_TYPE;
    publ.expires = dbk_mwi_expires;

    ret = kz_pua_api.send_publish(&publ);

    return ret;

}

int kz_pua_publish_dialoginfo(struct json_object *json_obj) {
    int ret = 1;
    str from = { 0, 0 }, to = { 0, 0 };
    str from_user = { 0, 0 }, to_user = { 0, 0 };
    str from_realm = { 0, 0 }, to_realm = { 0, 0 };
    str callid = { 0, 0 }, fromtag = { 0, 0 }, totag = { 0, 0 };
    str state = { 0, 0 };
    char body[4096];
    str direction = { 0, 0 };
    str extra_headers = {0, 0};
    str header_name = str_init("Sender");
    char sender_buf[1024];
    char extra_buf[1024];
    str sender = {0, 0};
    str dialoginfo_body = {0 , 0};
    int expires = dbk_dialog_expires;
    
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

    publ_info_t publ1;
    memset(&publ1, 0, sizeof(publ_info_t));
   	publ1.pres_uri = &from;
   	publ1.id = from;

    publ1.body = &dialoginfo_body;
    publ1.event = DIALOG_EVENT;
    publ1.expires = expires;
    publ1.flag |= UPDATE_TYPE;

    sprintf(sender_buf, "sip:%s",callid.s);
    sender.s = sender_buf;
    sender.len = strlen(sender_buf);

	/* build extra_headers with Sender*/
   	extra_headers.s= extra_buf;
   	memcpy(extra_headers.s, header_name.s, header_name.len);
   	extra_headers.len= header_name.len;
   	memcpy(extra_headers.s+extra_headers.len,": ",2);
   	extra_headers.len+= 2;
   	memcpy(extra_headers.s+ extra_headers.len, sender.s, sender.len);
   	extra_headers.len+= sender.len;
   	memcpy(extra_headers.s+ extra_headers.len, CRLF, CRLF_LEN);
   	extra_headers.len+= CRLF_LEN;

   	publ1.extra_headers = &extra_headers;


    ret = kz_pua_api.send_publish(&publ1);

    if (ret < 0) {
	LM_ERR("Failed to process dialoginfo update command\n");
	ret = -1;
    }

    return ret;

 error:
    return -1;

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
    		ret = kz_pua_publish_dialoginfo(json_obj);
    	} else if (event_package.len == str_event_message_summary.len
    			&& strncmp(event_package.s, str_event_message_summary.s, event_package.len) == 0) {
    		ret = kz_pua_publish_mwi(json_obj);
    	} else if (event_package.len == str_event_presence.len
    			&& strncmp(event_package.s, str_event_presence.s, event_package.len) == 0) {
    		ret = kz_pua_publish_presence(json_obj);
    	}
    }

    json_object_put(json_obj);
    return ret;
 error:
    return -1;
}

