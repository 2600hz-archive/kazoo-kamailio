
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


#include "blf.h"
#include "dbase.h"

static presence_api_t presence_api;
static pua_dialoginfo_api_t pua_dialoginfo_api;
static pres_dialoginfo_api_t pres_dialoginfo_api;
static pua_api_t pua_api;

int rmqp_pres_update_handle(char* req);
void start_presence_timer_processes(void);
void start_presence_rmqp_consumer_processes(struct db_id* id);
void rmqp_consumer_loop(struct db_id* id);
int dbk_initialize_pres_htable(void);

#define BLF_MAX_DIALOGS 8
#define BLF_JSON_FROM      	"From"
#define BLF_JSON_TO        	"To"
#define BLF_JSON_CALLID    	"Call-ID"
#define BLF_JSON_TOTAG     	"To-Tag"
#define BLF_JSON_FROMTAG   	"From-Tag"
#define BLF_JSON_STATE     	"State"
#define BLF_JSON_USER      	"User"
#define BLF_JSON_FROM     	"From"
#define BLF_JSON_QUEUE     	"Queue"
#define BLF_JSON_EXPIRES	"Expires"
#define BLF_JSON_APP_NAME       "App-Name"
#define BLF_JSON_APP_VERSION    "App-Version"
#define BLF_JSON_NODE           "Node"
#define BLF_JSON_SERVERID       "Server-ID"
#define BLF_JSON_EVENT_CATEGORY "Event-Category"
#define BLF_JSON_EVENT_NAME     "Event-Name"
#define BLF_JSON_TYPE           "Type"
#define BLF_JSON_MSG_ID         "Msg-ID"
#define BLF_JSON_DIRECTION      "Direction"

#define BLF_JSON_CONTACT   	"Contact"
#define BLF_JSON_EVENT_PKG     "Event-Package"
#define MWI_JSON_WAITING   "Messages-Waiting"
#define MWI_JSON_NEW   "Messages-New"
#define MWI_JSON_SAVED   "Messages-Saved"
#define MWI_JSON_URGENT   "Messages-Urgent"
#define MWI_JSON_URGENT_SAVED "Messages-Urgent-Saved"
#define MWI_JSON_ACCOUNT "Message-Account"
#define MWI_JSON_FROM      	"From"
#define MWI_JSON_TO        	"To"

#define MWI_BODY "Messages-Waiting: %.*s\r\nMessage-Account: %.*s\r\nVoice-Message: %.*s/%.*s (%.*s/%.*s)\r\n"


static char blf_queue_name_buffer[128];
static amqp_bytes_t blf_queue;

str sht_name = str_init("$sht(dbkp=>$ci)");
pv_spec_t sht_spec;
char node_name[128];


str str_event_message_summary = str_init("message-summary");
str str_event_dialog = str_init("dialog");


str str_username_col = str_init("username");
str str_domain_col = str_init("domain");
str str_body_col = str_init("body");
str str_expires_col = str_init("expires");
str str_presentity_uri_col = str_init("presentity_uri");
char* pres_uri_buf = NULL;
int pres_uri_size = 0;

str str_event_col = str_init("event");
str str_contact_col = str_init("contact");
str str_callid_col = str_init("callid");
str str_from_tag_col = str_init("from_tag");
str str_to_tag_col = str_init("to_tag");



int dbk_initialize_presence()
{
  str unique_string;

  LM_DBG("dbk_initialize_presence\n");
  /* bind to presence module */
  bind_presence_t bind_presence= (bind_presence_t)find_export("bind_presence", 1,0);
  if (!bind_presence) {
    LM_ERR("Can't find presence module\n");
    return -1;
  }
  if (bind_presence(&presence_api) < 0) {
    LM_ERR("Can't bind to presence module api\n");
    return -1;
  }
  /* bind to pua_dialoginfo module */
  bind_pua_dialoginfo_t bind_pua_dialoginfo= (bind_pua_dialoginfo_t)find_export("bind_pua_dialoginfo", 1,0);
  if (!bind_pua_dialoginfo) {
    LM_ERR("Can't find pua_dialoginfo module\n");
    return -1;
  }
  if (bind_pua_dialoginfo(&pua_dialoginfo_api) < 0) {
    LM_ERR("Can't bind to pua_dialoginfo module api\n");
    return -1;
  }

  /* bind to presence_dialoginfo module */
  bind_pres_dialoginfo_t bind_pres_dialoginfo= (bind_pres_dialoginfo_t)find_export("bind_pres_dialoginfo", 1,0);
  if (!bind_pres_dialoginfo) {
    LM_ERR("Can't find pres_dialoginfo module\n");
    return -1;
  }

  if (bind_pres_dialoginfo(&pres_dialoginfo_api) < 0) {
    LM_ERR("Can't bind to pres_dialoginfo module api\n");
    return -1;
  }

  /* bind to pua module */
  bind_pua_t bind_pua= (bind_pua_t)find_export("bind_pua", 1,0);
  if (!bind_pua) {
	LM_ERR("Can't find bind pua\n");
	return -1;
  }
  if (bind_pua(&pua_api) < 0) {
	LM_ERR("Can't bind to pua api\n");
	return -1;
  }

  if ( dbk_initialize_pres_htable() < 0) {
    LM_ERR("Failed to initialize presence htable\n");
    return -1;
  }
  LM_DBG("Initialized pres_htable\n");

  if ( pv_parse_spec(&sht_name, &sht_spec) == NULL ) {
    LM_ERR("Failed to parse sht spec\n");
    return -1;
  }

  tmb.generate_callid(&unique_string);

  blf_queue.bytes = blf_queue_name_buffer;
  blf_queue.len = sprintf(blf_queue.bytes, "BLF-%.*s-%.*s",
                          dbk_node_hostname.len, dbk_node_hostname.s, unique_string.len, unique_string.s);

  sprintf(node_name, "kamailio@%.*s", dbk_node_hostname.len, dbk_node_hostname.s);
  return 0;
}


void dbk_start_presence_rmqp_consumer_processes(struct db_id* id)
{
  int i;
  for (i= 0; i< DBK_PRES_WORKERS_NO; i++) {
    int newpid = fork_process(PROC_NOCHLDINIT, "RMQP PRESENCE WORKER", 0);
    if(newpid < 0) {
      LM_ERR("Failed to start AMQP presence worker\n");
      return;
    } else if(newpid == 0) {
      // child - this will loop forever
      LM_INFO("Created dbk AMQP presence worker %d\n", newpid);
      rmqp_consumer_loop(id);
    } else {
      LM_INFO("Created dbk AMQP presence worker %d\n", newpid);
    }
  }
}

void rmqp_consumer_loop(struct db_id* id)
{
  amqp_frame_t frame;
  int result;
  amqp_basic_deliver_t *d;
  amqp_basic_properties_t *p;
  size_t body_target;
  size_t body_received = 0;
  char body[2048];
  rmq_conn_t * rmq;
  int reconn_retries = 0;

  rmq = dbk_dummy_db_conn(id);
  if (rmq == NULL) {
    LM_ERR("Failed to create AMQP connection\n");
    return;
  }

  while (1) {
    if (!rmq->conn) {
      reconn_retries = 0;
      while (1) {
        reconn_retries++;
        LM_DBG("Attempt %d to connect to AMQP\n", reconn_retries);
        sleep(1);

        if (rmqp_open_connection(rmq) < 0) {
          LM_DBG("Failed to open AMQP connection\n");
          continue;
        }

        amqp_queue_declare(rmq->conn, rmq->channel, blf_queue, 0, 0, 0, 1, amqp_empty_table);
        if (rmq_error("Declaring queue", amqp_get_rpc_reply(rmq->conn))) {
          LM_DBG("Failed to declare AMQP presence queue\n");
          continue;
        }
        LM_DBG("Create presence AMQP queue %.*s\n", (int) blf_queue.len, (char *) blf_queue.bytes);

        amqp_exchange_declare(rmq->conn, rmq->channel, amqp_cstring_bytes("dialoginfo"), amqp_cstring_bytes("direct"),
                              0, 0, amqp_empty_table);
        if (rmq_error("Declaring exchange", amqp_get_rpc_reply(rmq->conn))) {
          LM_ERR("Failed to declare AMQP dialoginfo exchange\n");
          continue;
        }

        static amqp_bytes_t exch = {10, "dialoginfo"};
        amqp_queue_bind(rmq->conn, rmq->channel, blf_queue, exch, blf_queue, amqp_empty_table);
        if (rmq_error("Binding queue", amqp_get_rpc_reply(rmq->conn))) {
          LM_DBG("Unable to bind presence AMQP queue\n");
          continue;
        }

        amqp_basic_consume(rmq->conn, rmq->channel, blf_queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        if (rmq_error("Consuming", amqp_get_rpc_reply(rmq->conn))) {
          LM_DBG("Failed to start consuming from queue\n");
          continue;
        }

        break;
      }

      LM_DBG("Connected to AMQP after %d attempts\n", reconn_retries);

      continue;
    }

    while (1) {
      LM_DBG("Wait for a packet\n");
      body_received = 0;
      amqp_maybe_release_buffers(rmq->conn);
      result = amqp_simple_wait_frame(rmq->conn, &frame);
      if (result < 0 ) {
        LM_ERR("Lost AMQP connection\n");
        rmq_close(rmq);
        break;
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

      result = amqp_simple_wait_frame(rmq->conn, &frame);
      if (result < 0) {
        LM_ERR("Lost AMQP connection\n");
        rmq_close(rmq);
        break;
      }

      if (frame.frame_type != AMQP_FRAME_HEADER) {
        LM_ERR("amqp: Expected header!");
        break;
      }

      p = (amqp_basic_properties_t *) frame.payload.properties.decoded;
      if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        LM_DBG("Content-type: %.*s\n",
               (int) p->content_type.len, (char *) p->content_type.bytes);
      }

      body_target = frame.payload.properties.body_size;

      while (body_received < body_target) {
        result = amqp_simple_wait_frame(rmq->conn, &frame);
        if (result < 0) {
          LM_ERR("Lost AMQP connection\n");
          rmq_close(rmq);
          body_received = 0;
          break;
        }

        if (frame.frame_type != AMQP_FRAME_BODY) {
          LM_ERR("amqp: Expected header!");
          body_received = 0;
          break;
        }

        memcpy(body + body_received, frame.payload.body_fragment.bytes,
               frame.payload.body_fragment.len);

        body_received += frame.payload.body_fragment.len;

        LM_INFO("%.*s\n", (int)frame.payload.body_fragment.len,
                (char*)frame.payload.body_fragment.bytes);
      }

      if (body_received != body_target) {
        LM_CRIT("Unexpected body size: recv %zu, expected %zu\n",
                body_received, body_target);
        continue;
      }

      /* everything was fine, we can quit now because we received the reply */
      break;
    }

    if (body_received) {
      body[body_received] = '\0';
      LM_DBG("Received update %s\n", body);

      if (rmqp_pres_update_handle(body) < 0) {
        LM_ERR("Failed to add to the update queue\n");
      }
    }
  }

  LM_ERR("Presence consumer loop terminated\n");
  rmq_close(rmq);
}

typedef struct dbk_pres_dialog {
  int version;
  str callid;
  str localtag;
  str remotetag;
  int body_size_alloc;
  str body;
  struct dbk_pres_dialog* next;
} dbk_pres_dialog_t;

typedef struct dbk_pres_user {
  str user;
  dbk_pres_dialog_t * pd;
  struct dbk_pres_user* next;
} dbk_pres_user_t;

typedef struct {
  gen_lock_t lock;
  dbk_pres_user_t* pu;
}dbk_pres_htable_t;

static dbk_pres_htable_t* dbk_phtable = NULL;
unsigned int dbk_phtable_size = 256;

int dbk_initialize_pres_htable(void)
{
  int i;

  dbk_phtable = (dbk_pres_htable_t*)shm_malloc(dbk_phtable_size*sizeof(dbk_pres_htable_t));
  if ( dbk_phtable == NULL ) {
    LM_ERR("No more shared memory\n");
    return -1;
  }
  memset(dbk_phtable, 0, dbk_phtable_size*sizeof(dbk_pres_htable_t));

  for ( i = 0; i < dbk_phtable_size; i++ ) {
    lock_init(&dbk_phtable[i].lock);
  }

  return 0;
}

void dbk_free_pd(dbk_pres_dialog_t* pd)
{
  shm_free(pd->body.s);
  shm_free(pd);
}

void dbk_free_pu(dbk_pres_user_t* pu)
{
  dbk_pres_dialog_t *pd_next;
  dbk_pres_dialog_t *pd = pu->pd;
  while ( pd ) {
    pd_next = pd->next;
    dbk_free_pd(pd);
    pd = pd_next;
  }
  shm_free(pu);
}

void dbk_destroy_presence(void)
{
  dbk_pres_user_t *pu, *pu_next;
  int i;

  if ( dbk_phtable == NULL ) {
    return;
  }

  for ( i = 0; i < dbk_phtable_size; i++ ) {
    lock_destroy(&dbk_phtable[i].lock);
    pu = dbk_phtable[i].pu;
    while ( pu ) {
      pu_next = pu->next;
      dbk_free_pu(pu);
      pu = pu_next;
    }

  }
  shm_free(dbk_phtable);
}

dbk_pres_user_t* dbk_pres_search_pu(unsigned int hash_code, str* user,
                                    dbk_pres_user_t** pu_prev_ret, dbk_pres_user_t *prev_pu_addr)
{
  dbk_pres_user_t *pu, *pu_prev = NULL;

  for (pu = dbk_phtable[hash_code].pu; pu; pu = pu->next) {
    if (pu->user.len == user->len &&
        memcmp(pu->user.s, user->s, user->len)==0) {
      break;
    }
    pu_prev = pu;
  }
  if (pu_prev_ret) {
    *pu_prev_ret = pu_prev;
  }

  return pu;
}

dbk_pres_dialog_t* dbk_new_pres_dialog(str* callid, str* localtag, str* remotetag, str* body)
{
  dbk_pres_dialog_t* pd = (dbk_pres_dialog_t*)shm_malloc(sizeof(dbk_pres_dialog_t) + callid->len+
                                                         localtag->len + remotetag->len);

  if ( pd == NULL) {
    LM_ERR("No more shared memory\n");
    return NULL;
  }
  memset(pd, 0, sizeof(dbk_pres_dialog_t));
  char* p = (char*)pd + sizeof(dbk_pres_dialog_t);
  pd->callid.s = p;
  memcpy(pd->callid.s, callid->s, callid->len);
  pd->callid.len = callid->len;
  p += callid->len;

  if (localtag->len) {
    pd->localtag.s = p;
    memcpy(pd->localtag.s, localtag->s, localtag->len);
    pd->localtag.len = localtag->len;
    p += localtag->len;
  }
  if (remotetag->len) {
    pd->remotetag.s = p;
    memcpy(pd->remotetag.s, remotetag->s, remotetag->len);
    pd->remotetag.len = remotetag->len;
    p += remotetag->len;
  }

  pd->body_size_alloc = body->len;
  pd->body.s = (char*)shm_malloc(pd->body_size_alloc);
  if (pd->body.s == NULL) {
    LM_ERR("No more shared memory\n");
    shm_free(pd);
    return NULL;
  }
  memcpy(pd->body.s, body->s, body->len);
  pd->body.len = body->len;

  return pd;
}

dbk_pres_user_t* dbk_new_pres_user(str* user, str* callid, str* localtag, str* remotetag, str* body)
{
  dbk_pres_user_t* pu = (dbk_pres_user_t*)shm_malloc(sizeof(dbk_pres_user_t) + user->len);

  if ( pu == NULL) {
    LM_ERR("No more shared memory\n");
    return NULL;
  }
  memset(pu, 0, sizeof(dbk_pres_user_t));
  pu->user.s = (char*)pu + sizeof(dbk_pres_user_t);
  memcpy(pu->user.s, user->s, user->len);
  pu->user.len = user->len;
  pu->pd = dbk_new_pres_dialog(callid, localtag, remotetag, body);
  if (pu->pd == NULL) {
    LM_ERR("Failed to construct presence dialog\n");
    shm_free(pu);
    return NULL;
  }
  return pu;
}

int dbk_pres_insert_pu(str* user, str* callid, str* localtag, str* remotetag, str* body,
                       unsigned int hash_code)
{
  dbk_pres_user_t* pu = dbk_new_pres_user(user, callid, localtag, remotetag, body);

  if (pu == NULL) {
    LM_ERR("Failed to create new pres user\n");
    return -1;
  }

  lock_get(&dbk_phtable[hash_code].lock);
  pu->next = dbk_phtable[hash_code].pu;
  dbk_phtable[hash_code].pu = pu;
  lock_release(&dbk_phtable[hash_code].lock);
  return 0;
}

dbk_pres_dialog_t* dbk_pres_pu_search_pd(dbk_pres_user_t* pu, str* callid, str* localtag,
                                         str* remotetag, dbk_pres_dialog_t** pd_prev_ret, dbk_pres_dialog_t* prev_pd_addr)
{
  dbk_pres_dialog_t *pd, *pd_prev = NULL;

  for (pd = pu->pd; pd; pd = pd->next) {
    if (prev_pd_addr && pd != prev_pd_addr) {
      pd_prev = pd;
      continue;
    }

    if (pd->callid.len==callid->len && memcmp(pd->callid.s, callid->s, callid->len)==0 &&
        (!localtag->len || !pd->localtag.len || (localtag->len==pd->localtag.len &&
                                                 memcmp(localtag->s, pd->localtag.s, localtag->len)==0)) &&
        (!remotetag->len || !pd->remotetag.len || (remotetag->len==pd->remotetag.len &&
                                                   memcmp(remotetag->s, pd->remotetag.s, remotetag->len)==0)) ) {
      break;
    }

    pd_prev = pd;
  }

  if (pd_prev_ret) *pd_prev_ret = pd_prev;

  return pd;
}


void dbk_free_xml(str* body) {
  if (body) {
    if (body->s)
      xmlFree(body->s);
    pkg_free(body);
  }
}

int dbk_blf_copy_bodies(dbk_pres_user_t* pu, dbk_pres_dialog_t* curr_pd,
                        str* dlg_bodies, int* dlg_no)
{
  dbk_pres_dialog_t* pd;
  int i;

  for (pd = pu->pd, i= *dlg_no; pd && i< BLF_MAX_DIALOGS; pd = pd->next) {
    if ( pd == curr_pd) {
      continue;
    }
    dlg_bodies[i].s = (char*) pkg_malloc(pd->body.len);
    if (dlg_bodies[i].s == NULL) {
      LM_ERR("No more memory\n");
      goto error;
    }
    memcpy(dlg_bodies[i].s, pd->body.s, pd->body.len);
    dlg_bodies[i].len = pd->body.len;
    i++;
  }
  if (pd) {
    LM_WARN("More dialogs than BLF_MAX_DIALOGS=%d for user [%.*s]\n",
            BLF_MAX_DIALOGS, pu->user.len, pu->user.s);
  }
  *dlg_no = i;
  return 0;

 error:
  while(--i>=*dlg_no) {
    pkg_free(dlg_bodies[i].s);
  }
  return -1;
}

str* dbk_blf_aggregate_body(str* user, str* dlg_bodies, int dlg_no)
{
  str* dlg_bodies_p[BLF_MAX_DIALOGS];
  struct sip_uri uri;
  int i;

  LM_DBG("Aggregate %d bodies\n", dlg_no);

  if (parse_uri(user->s, user->len, &uri) < 0) {
    LM_ERR("Failed to parse uri\n");
    return NULL;
  }

  for (i = 0; i < dlg_no; i++) {
    dlg_bodies_p[i] = &dlg_bodies[i];
  }

  return pres_dialoginfo_api.agg_dialoginfo(&uri.user, &uri.host, dlg_bodies_p, dlg_no);
}

str* dbk_phtable_update(str* local_user, str* remote_user, str* callid,
                        str* localtag, str* remotetag, str* state, int initiator)
{
  dbk_pres_user_t* pu, *pu_prev = NULL;
  dbk_pres_dialog_t* pd, *pd_prev = NULL;
  unsigned int hash_code = core_hash(local_user, 0, dbk_phtable_size);
  str* body = NULL ;
  int terminated = strcmp(state->s, "terminated")==0?1:0;
  str dlg_bodies[BLF_MAX_DIALOGS];
  int dlg_no = 1;
  int i;

  LM_DBG("Update dbk phtable: %.*s %.*s %.*s %.*s"
         ,local_user->len, local_user->s
         ,remote_user->len, remote_user->s
         ,callid->len, callid->s
         ,state->len, state->s);

  if (terminated) {
    LM_DBG("Updated to terminated state\n");
  }

  lock_get(&dbk_phtable[hash_code].lock);
  pu = dbk_pres_search_pu(hash_code, local_user, &pu_prev, 0);

  /**  1. No record for user */
  if (pu == NULL) {
    lock_release(&dbk_phtable[hash_code].lock);
    if (!terminated) {
      LM_INFO("No record found for %.*s/%.*s, add with initial state %.*s\n"
              ,local_user->len, local_user->s
              ,remote_user->len, remote_user->s
              ,state->len, state->s);
      if ( (body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user,
                                                       callid, initiator, localtag, remotetag, 0 , 0, 0))== NULL) {
        LM_ERR("Failed to construct BLF XML doc\n");
        return NULL;
      }
      LM_DBG("dialoginfo body [%.*s]\n", body->len, body->s);

      if ( dbk_pres_insert_pu(local_user, callid, localtag, remotetag, body, hash_code) < 0) {
        LM_ERR("Failed to insert new pres_user in htable\n");
      }
      return body;
    }
    LM_INFO("No record found for %.*s/%.*s and state terminated, do nothing\n"
            ,local_user->len, local_user->s
            ,remote_user->len, remote_user->s);
    return pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user, callid,
                                               initiator, localtag, remotetag, 0 , 0, 0);
  }

  pd = dbk_pres_pu_search_pd(pu, callid, localtag, remotetag, &pd_prev, 0);

  /**  2. Existing record for user, no record for dialog */
  if (pd == NULL) {

    /* copy the other bodies to create the aggregated body */
    if (pu->pd != NULL) {
      dbk_blf_copy_bodies(pu, NULL, dlg_bodies, &dlg_no);
    }
    lock_release(&dbk_phtable[hash_code].lock);
    if (!terminated) {
      LM_INFO("Dialog %.*s not found for %.*s/%.*s, add with initial state %.*s\n"
              ,callid->len, callid->s
              ,local_user->len, local_user->s
              ,remote_user->len, remote_user->s
              ,state->len, state->s);
      LM_DBG("Dialog record not found, insert a new one\n");
      if ( (body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user,
                                                       callid, initiator, localtag, remotetag, 0 , 0, 0))== NULL) {
        LM_ERR("Failed to construct BLF XML doc\n");
        goto ret_current_dialog;
      }
      LM_DBG("dialoginfo body [%.*s]\n", body->len, body->s);

      pd = dbk_new_pres_dialog(callid, localtag, remotetag, body);
      if (pd == NULL) {
        LM_ERR("Failed to create new pres dialog\n");
        goto ret_agg_dialogs;
      }
      lock_get(&dbk_phtable[hash_code].lock);
      /* search again the corresponding pu
       * give it the prev addr to speed up the search */
      pu = dbk_pres_search_pu(hash_code, local_user, 0, pu);
      if (pu == NULL) { /* pu deleted in the meantime (another dialog ended) */
        lock_release(&dbk_phtable[hash_code].lock);
        shm_free(pd);
        dbk_pres_insert_pu(local_user, callid, localtag, remotetag, body, hash_code);
        goto ret_current_dialog;
      }
      pd->next = pu->pd;
      pu->pd = pd;
      lock_release(&dbk_phtable[hash_code].lock);
    } else { /* terminated */
      LM_INFO("Dialog %.*s not found for %.*s/%.*s, building terminated body\n"
              ,callid->len, callid->s
              ,local_user->len, local_user->s
              ,remote_user->len, remote_user->s);
      body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user, callid,
                                                 initiator, localtag, remotetag, 0 , 0, 0);
    }
    goto ret_agg_dialogs;
  }

  /**  3. Existing record for user and existing record for dialog */

  /* If there are other dialogs, make a copy of the bodies */
  if (pu->pd != pd || pd->next != NULL) {
    dbk_blf_copy_bodies(pu, pd, dlg_bodies, &dlg_no);
  }

  if (!terminated) {
    LM_INFO("Found record %.*s/%.*s and matching dialog %.*s, update the state %.*s\n"
            ,local_user->len, local_user->s
            ,remote_user->len, remote_user->s
            ,callid->len, callid->s
            ,state->len, state->s);
    /* update only the body */
    lock_release(&dbk_phtable[hash_code].lock);

    if ( (body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user,
                                                     callid, initiator, localtag, remotetag, 0 , 0, 0))== NULL) {
      LM_ERR("Failed to construct BLF XML doc\n");
      goto ret_current_dialog;
    }
    LM_DBG("dialoginfo body [%.*s]\n", body->len, body->s);

    lock_get(&dbk_phtable[hash_code].lock);
    /* search again the corresponding pu;
     * give it the prev addr to speed up the search */
    pu = dbk_pres_search_pu(hash_code, local_user, 0, pu);
    if (pu == NULL) { /* pres user deleted in the meantime (my dialog ended) */
      lock_release(&dbk_phtable[hash_code].lock);
      LM_INFO("Dialog %.*s deleted in the meantime (my dialog must have ended)\n"
              ,callid->len, callid->s);
      dbk_free_xml(body);
      body = NULL;
      goto ret_current_dialog; /* to free the dlg_bodies array if there is anything there */
    }

    pd = dbk_pres_pu_search_pd(pu, callid, localtag, remotetag, &pd_prev, pd);
    if (pd == NULL) {
      lock_release(&dbk_phtable[hash_code].lock);
      LM_INFO("Dialog %.*s deleted in the meantime (my dialog must have ended)\n"
              ,callid->len, callid->s);
      dbk_free_xml(body);
      body = NULL;
      goto ret_current_dialog; /* to free the dlg_bodies array if there is anything there */
    }

    if (pd->body_size_alloc < body->len) {
      pd->body_size_alloc = body->len * 2;
      pd->body.s = (char*) shm_realloc(pd->body.s, pd->body_size_alloc);
      if (pd->body.s == NULL) {
        LM_ERR("No more shared memory\n");
        lock_release(&dbk_phtable[hash_code].lock);
        goto ret_agg_dialogs;
      }
    }
    memcpy(pd->body.s, body->s, body->len);
    pd->body.len = body->len;
    if ( pu->pd != pd || pd->next!= NULL) {
      lock_release(&dbk_phtable[hash_code].lock);
      goto ret_current_dialog;
    }
    lock_release(&dbk_phtable[hash_code].lock);
  } else {
    LM_INFO("pd_prev: %p  pd->next: %p\n", pd_prev, pd->next);
    if (pd_prev == NULL && pd->next == NULL) { // there is no other dialog record
      LM_INFO("Found record %.*s/%.*s and matching dialog %.*s, no remaining dialogs removing pu record\n"
              ,local_user->len, local_user->s
              ,remote_user->len, remote_user->s
              ,callid->len, callid->s);
      /* delete pu */
      if (pu_prev) {
        pu_prev->next = pu->next;
      } else {
        dbk_phtable[hash_code].pu = pu->next;
      }
      lock_release(&dbk_phtable[hash_code].lock);
      dbk_free_pu(pu);

      body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user, callid,
                                                 initiator, localtag, remotetag, 0 , 0, 0);
      goto ret_current_dialog;
    } else {
      LM_INFO("Found record %.*s/%.*s and matching dialog %.*s, other dialogs remain\n"
              ,local_user->len, local_user->s
              ,remote_user->len, remote_user->s
              ,callid->len, callid->s);
      /* delete pd */
      if (pd_prev) {
        pd_prev->next = pd->next;
      } else {
        pu->pd = pd->next;
      }
      lock_release(&dbk_phtable[hash_code].lock);
      dbk_free_pd(pd);
      body = pua_dialoginfo_api.build_dialoginfo(state->s, local_user, remote_user, callid,
                                                 initiator, localtag, remotetag, 0 , 0, 0);
    }
  }

 ret_agg_dialogs:
  if (dlg_no > 1) {
    str* body_copy = body;
    dlg_bodies[0] = *body;
    body = dbk_blf_aggregate_body(local_user, dlg_bodies, dlg_no);
    if (body == NULL) {
      LM_ERR("Failed to aggregate body\n");
      body = body_copy;
    } else {
      dbk_free_xml(body_copy);
    }
  }

 ret_current_dialog:
  /* free the copied dialog bodies */
  for(i =1; i < dlg_no; i++) {
    pkg_free(dlg_bodies[i].s);
  }

  return body;
}

int dbk_pres_update_and_notify(str* local_user, str* remote_user, str* callid,
                               str* localtag, str* remotetag, str* state, int initiator) {

  str event = str_init("dialog");
  str* body;

  /* update in htable */
  if ((body = dbk_phtable_update(local_user, remote_user, callid, localtag,
                                 remotetag, state, initiator)) == 0) {
    LM_ERR("Failed to update in htable\n");
    return -1;
  }

  /* call refresh_watchers from presence */
  if (presence_api.notify_watchers(local_user, &event, body) < 0) {
    LM_ERR("Failed to notify watchers\n");
    goto error;
  }
  dbk_free_xml(body);
  return 0;
 error:
  dbk_free_xml(body);
  return -1;
}


void free_mwi_body(str* body)
{
	if (body) {
		if (body->s)
			shm_free(body->s);
		pkg_free(body);
	}
}

str* mwi_body(str* mwi_account, str* mwi_waiting, str* mwi_new, str* mwi_saved, str* mwi_urgent, str* mwi_urgent_saved )
{

       char mwi_body_buffer[1024];
	sprintf(mwi_body_buffer,MWI_BODY
                 ,mwi_waiting->len, mwi_waiting->s
                 ,mwi_account->len, mwi_account->s
                 ,mwi_new->len, mwi_new->s
                 ,mwi_saved->len, mwi_saved->s
                 ,mwi_urgent->len, mwi_urgent->s
                 ,mwi_urgent_saved->len, mwi_urgent_saved->s
              );
       int x = strlen(mwi_body_buffer);

       LM_INFO("message-summary body = %s",mwi_body_buffer);

       str* body = (str*)pkg_malloc(sizeof(str));

	body->s = (char*)shm_malloc(x * sizeof(char));
	if (body->s == NULL) {
		LM_ERR("No more shared memory\n");
		return NULL;
	}
	memcpy(body->s, mwi_body_buffer, x * sizeof(char));
	body->len = x;

       LM_INFO("message-summary body-> = %.*s",body->len, body->s);

       return body;
}

str* dbk_phtable_update_mwi(str* local_user, str* remote_user, str* callid,
	 str* localtag, str* remotetag, str* mwi_account, str* mwi_waiting, str* mwi_new, str* mwi_saved, str* mwi_urgent, str* mwi_urgent_saved)
{
	dbk_pres_user_t* pu, *pu_prev = NULL;
	dbk_pres_dialog_t* pd, *pd_prev = NULL;
	unsigned int hash_code = core_hash(local_user, 0, dbk_phtable_size);
	str* body = NULL ;
	int terminated = 0;
	str dlg_bodies[BLF_MAX_DIALOGS];
	int dlg_no = 1;
	int i;

	if (terminated) {
		LM_INFO("Update for terminated state\n");
	}

	lock_get(&dbk_phtable[hash_code].lock);
	pu = dbk_pres_search_pu(hash_code, local_user, &pu_prev, 0);

	/**  1. No record for user */
	if (pu == NULL) {
		lock_release(&dbk_phtable[hash_code].lock);
		if (!terminated) {
			LM_INFO("No record found for %s, created a new one\n", local_user->s);
			if ( (body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved))== NULL) {
				LM_ERR("Failed to construct MWI doc\n");
				return NULL;
			}
			LM_INFO("mwi body [%.*s]\n", body->len, body->s);

			if ( dbk_pres_insert_pu(local_user, callid, localtag, remotetag, body, hash_code) < 0) {
				LM_INFO("Failed to insert new pres_user mwi in htable\n");
			}
			return body;
		}
		LM_INFO("No record found and state terminated, do nothing\n");
		return mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved);
	}

	pd = dbk_pres_pu_search_pd(pu, callid, localtag, remotetag, &pd_prev, 0);

	/**  2. Existing record for user, no record for dialog */
	if (pd == NULL) {

		/* copy the other bodies to create the aggregated body */
		if (pu->pd != NULL) {
			dbk_blf_copy_bodies(pu, NULL, dlg_bodies, &dlg_no);
		}
		lock_release(&dbk_phtable[hash_code].lock);
		if (!terminated) {
			LM_INFO("Dialog record not found, insert a new one\n");
			if ( (body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved))== NULL) {
				LM_ERR("Failed to construct BLF XML doc\n");
				goto ret_current_dialog;
			}
			LM_INFO("mwi body [%.*s]\n", body->len, body->s);

			pd = dbk_new_pres_dialog(callid, localtag, remotetag, body);
			if (pd == NULL) {
				LM_ERR("Failed to create new pres dialog\n");
				goto ret_agg_dialogs;
			}
			lock_get(&dbk_phtable[hash_code].lock);
			/* search again the corresponding pu
 			 * give it the prev addr to speed up the search */
			pu = dbk_pres_search_pu(hash_code, local_user, 0, pu);
			if (pu == NULL) { /* pu deleted in the meantime (another dialog ended) */
				lock_release(&dbk_phtable[hash_code].lock);
				shm_free(pd);
				dbk_pres_insert_pu(local_user, callid, localtag, remotetag, body, hash_code);
				goto ret_current_dialog;
			}
			pd->next = pu->pd;
			pu->pd = pd;
//			pu->dlg_no++;
			lock_release(&dbk_phtable[hash_code].lock);
		} else { /* terminated */
			body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved);
		}
		goto ret_agg_dialogs;
	}

	/**  3. Existing record for user and existing record for dialog */

	/* If there are other dialogs, make a copy of the bodies */
//	if (pu->pd != pd || pd->next != NULL) {
//		dbk_blf_copy_bodies(pu, pd, dlg_bodies, &dlg_no);
//	}

	if (!terminated) {
		LM_INFO("Found record and matching dialog, update the body\n");
		/* update only the body */
		lock_release(&dbk_phtable[hash_code].lock);

		if ( (body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved))== NULL) {
			LM_ERR("Failed to construct BLF XML doc\n");
			goto ret_current_dialog;
		}
		LM_INFO("mwi body [%.*s]\n", body->len, body->s);

		lock_get(&dbk_phtable[hash_code].lock);
		/* search again the corresponding pu;
		 * give it the prev addr to speed up the search */
		pu = dbk_pres_search_pu(hash_code, local_user, 0, pu);
		if (pu == NULL) { /* pres user deleted in the meantime (my dialog ended) */
			lock_release(&dbk_phtable[hash_code].lock);
			LM_INFO("pu deleted in the meantime (my dialog must have ended)\n");
			free_mwi_body(body);
			body = NULL;
			goto ret_current_dialog; /* to free the dlg_bodies array if there is anything there */
		}

		pd = dbk_pres_pu_search_pd(pu, callid, localtag, remotetag, &pd_prev, pd);
		if (pd == NULL) {
			lock_release(&dbk_phtable[hash_code].lock);
			LM_INFO("pd deleted in the meantime (my dialog must have ended)\n");
			free_mwi_body(body);
			body = NULL;
			goto ret_current_dialog; /* to free the dlg_bodies array if there is anything there */
		}

		if (pd->body_size_alloc < body->len) {
			pd->body_size_alloc = body->len * 2;
			pd->body.s = (char*) shm_realloc(pd->body.s, pd->body_size_alloc);
			if (pd->body.s == NULL) {
				LM_ERR("No more shared memory\n");
				lock_release(&dbk_phtable[hash_code].lock);
				goto ret_agg_dialogs;
			}
		}
		memcpy(pd->body.s, body->s, body->len);
		pd->body.len = body->len;
		if ( pu->pd != pd || pd->next!= NULL) {
			lock_release(&dbk_phtable[hash_code].lock);
			goto ret_current_dialog;
		}
		lock_release(&dbk_phtable[hash_code].lock);
	} else {
		LM_INFO("Found record and matching dialog, state terminated, delete\n");
		if (pd_prev == NULL && pd->next == NULL) { // there is no other dialog record
			LM_INFO("No other dialog - delete pu record\n");
			/* delete pu */
			if (pu_prev) {
				pu_prev->next = pu->next;
			} else {
				dbk_phtable[hash_code].pu = pu->next;
			}
			lock_release(&dbk_phtable[hash_code].lock);
			dbk_free_pu(pu);

			body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved);
			goto ret_current_dialog;
		} else {
			LM_INFO("Other dialog - delete pd record\n");
			/* delete pd */
			if (pd_prev) {
				pd_prev->next = pd->next;
			} else {
				pu->pd = pd->next;
			}
//			pu->dlg_no--;
			lock_release(&dbk_phtable[hash_code].lock);
			dbk_free_pd(pd);
			body = mwi_body(mwi_account, mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent, mwi_urgent_saved);
		}
	}

ret_agg_dialogs:
/*
	if (dlg_no > 1) {
		str* body_copy = body;
		dlg_bodies[0] = *body;
		body = dbk_blf_aggregate_body(local_user, dlg_bodies, dlg_no);
		if (body == NULL) {
			LM_ERR("Failed to aggregate body\n");
			body = body_copy;
		} else {
			free_mwi_body(body_copy);
		}
	}
*/

ret_current_dialog:
	/* free the copied dialog bodies */
	for(i =1; i < dlg_no; i++) {
		pkg_free(dlg_bodies[i].s);
	}

	return body;
}


int dbk_pres_update_and_notify_mwi(str* local_user, str* remote_user, str* callid,
	str* localtag, str* remotetag, str* mwi_account, str* mwi_waiting, str* mwi_new, str* mwi_saved, str* mwi_urgent, str* mwi_urgent_saved) {

	str event = str_init("message-summary");
	str* body;

	/* update in htable */
	if ((body = dbk_phtable_update_mwi(local_user, remote_user, callid, localtag,
					remotetag,  mwi_account,  mwi_waiting,  mwi_new,  mwi_saved,  mwi_urgent,  mwi_urgent_saved)) == 0) {
		LM_ERR("Failed to update in htable\n");
		return -1;
	}

	/* call refresh_watchers from presence */

       if (presence_api.notify_watchers(local_user, &event, body) < 0) {
		LM_ERR("Failed to notify watchers\n");
		goto error;
	}

      /* notify via publish */
      /*
       publ_info_t publ;
       memset(&publ, 0, sizeof(publ_info_t));
	publ.pres_uri= remote_user;
	publ.body= body;
	publ.event= MSGSUM_EVENT;
       publ.expires = 180;
	pua_api.send_publish(&publ);
      */


	free_mwi_body(body);
	return 0;
error:
	free_mwi_body(body);
	return -1;
}



#define json_extract_field(json_name, field)  do {                      \
    struct json_object* obj = json_object_object_get(json_obj, json_name); \
    field.s = (char*)json_object_get_string(obj);                       \
    if (field.s == NULL) {                                              \
      LM_DBG("Json-c error - failed to extract field [%s]\n", json_name); \
      field.s = "";                                                     \
    } else {                                                            \
      field.len = strlen(field.s);                                      \
    }                                                                   \
    LM_DBG("%s: [%s]\n", json_name, field.s?field.s:"Empty");           \
  } while (0);


int dbk_phtable_flush(int flush_all, str* user)
{
  dbk_pres_user_t* pu;

  if (flush_all) {
    int i;
    dbk_pres_user_t* pu_next;
    for (i = 0; i< dbk_phtable_size; i++) {
      lock_get(&dbk_phtable[i].lock);
      pu = dbk_phtable[i].pu;
      dbk_phtable[i].pu = NULL;
      lock_release(&dbk_phtable[i].lock);

      for (; pu; pu = pu_next) {
        pu_next = pu->next;
        dbk_free_pu(pu);
      }
    }
  } else {
    int hash_code;
    dbk_pres_user_t* pu_prev = NULL;
    str* body;
    static str event = str_init("dialog");

    hash_code = core_hash(user, NULL, dbk_phtable_size);
    lock_get(&dbk_phtable[hash_code].lock);
    pu = dbk_pres_search_pu(hash_code, user, &pu_prev, 0);
    if (pu == NULL) {
      LM_INFO("FLUSH: No record found for user %.*s\n", user->len, user->s);
      lock_release(&dbk_phtable[hash_code].lock);
    } else {
      LM_INFO("FLUSH: Delete record for user %.*s\n", user->len, user->s);
      if (pu_prev) {
        pu_prev->next = pu->next;
      } else {
        dbk_phtable[hash_code].pu = pu->next;
      }
      lock_release(&dbk_phtable[hash_code].lock);
      dbk_free_pu(pu);
    }


    /* send a notify with no dialog to clear dialog state */
    if ( (body = pua_dialoginfo_api.build_dialoginfo(0, user,
                                                     0, 0, 0, 0, 0, 0, 0, 0)) == NULL ) {
      LM_ERR("Failed to construct BLF XML doc\n");
      return -1;
    }
    if (presence_api.notify_watchers(user, &event, body) < 0) {
      LM_ERR("Failed to notify watchers\n");
      dbk_free_xml(body);
      return -1;
    }
    dbk_free_xml(body);
  }
  return 0;
}

int rmqp_pres_flush_handle(struct json_object* json_obj)
{
  str type={0, 0};
  str user= {0, 0};
  int flush_all = 0;

  json_extract_field(BLF_JSON_TYPE, type);
  if (type.len == 3 && strncmp(type.s, "all", 3) == 0) {
    flush_all = 1;
  } else {
    json_extract_field(BLF_JSON_USER, user);
  }

  return dbk_phtable_flush(flush_all, &user);
}

struct mi_root * mi_dbk_phtable_flush(struct mi_root *cmd, void *param)
{
  struct mi_node* node= NULL;
  str type;
  str user;
  int flush_all = 0;

  node = cmd->node.kids;
  if(node == NULL) {
    LM_ERR("Null command- missing parameters\n");
    return 0;
  }

  LM_DBG("Get type\n");

  /* Get type */
  type = node->value;
  if(type.s == NULL || type.len== 0) {
    LM_ERR( "first parameter empty\n");
    return init_mi_tree(404, "Missing parameter ('all' or 'user')", 35);
  }

  LM_DBG("Flush type=[%.*s]\n", type.len, type.s);

  if (type.len == 3 && strncmp(type.s, "all", 3) == 0) {
    LM_INFO("Flush all\n");
    flush_all = 1;
  } else {
    node = node->next;
    if(node == NULL)
      return 0;
    user = node->value;
    if(user.s == NULL || user.len== 0) {
      LM_ERR( "No user uri provided\n");
      return init_mi_tree(404, "No user uri provided", 20);
    }
    LM_INFO("Flush user [%.*s]\n", user.len, user.s);
  }

  if (dbk_phtable_flush(flush_all, &user) < 0 ) {
    LM_ERR("Presence htable flushing failed\n");
    return init_mi_tree( 500, MI_SSTR(MI_INTERNAL_ERR));
  }
  return init_mi_tree( 200, MI_SSTR(MI_OK));

}




/*
 * presence update: json format
 * {"From": "uri", "To": "uri", "State": "state", "Callid": "callid", "From-Tag": "tag", "To-Tag": "tag"}
 * {"Replaces": "", "Refered-By": ""}
 * */

int mwi_pres_update_handler(char *req, struct json_object *json_obj)
{
    int ret = 0;
	str from_user={0, 0}, to_user= {0, 0};
	str callid= {0, 0}, fromtag= {0, 0}, totag= {0, 0};
	str mwi_user={0, 0}, mwi_waiting= {0, 0}, mwi_new= {0, 0}, mwi_saved= {0, 0}, mwi_urgent= {0, 0}, mwi_urgent_saved= {0, 0}, mwi_account= {0, 0};

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


//     ret = dbk_pres_update_and_notify_mwi(&from_user, &to_user, &callid, &fromtag, &totag, &mwi_account, &mwi_waiting, &mwi_new, &mwi_saved, &mwi_urgent, &mwi_urgent_saved);
    str* mwibody = dbk_phtable_update_mwi(&from_user, &to_user, &callid, &fromtag, &totag, &mwi_account, &mwi_waiting, &mwi_new, &mwi_saved, &mwi_urgent, &mwi_urgent_saved);
//     str* mwibody = mwi_body(&mwi_account, &mwi_waiting, &mwi_new, &mwi_saved, &mwi_urgent, &mwi_urgent_saved );


    /* send_publish */

    publ_info_t publ;
    memset(&publ, 0, sizeof(publ_info_t));
    publ.pres_uri= &to_user;
    publ.body= mwibody;
    publ.event= MSGSUM_EVENT;
    publ.expires = 180;
    ret = pua_api.send_publish(&publ);

    free_mwi_body(mwibody);

    return ret;


}

int blf_pres_update_handler(char *req, struct json_object *json_obj)
{
    int ret = 0;
	str from_user={0, 0}, to_user= {0, 0};
	str callid= {0, 0}, fromtag= {0, 0}, totag= {0, 0};
	str state= {0, 0};
	str* body = NULL;
	str direction={0, 0};

	json_extract_field(BLF_JSON_FROM, from_user);
    json_extract_field(BLF_JSON_TO, to_user);
    json_extract_field(BLF_JSON_CALLID, callid);
    json_extract_field(BLF_JSON_FROMTAG, fromtag);
    json_extract_field(BLF_JSON_TOTAG, totag);
    json_extract_field(BLF_JSON_DIRECTION, direction);
    json_extract_field(BLF_JSON_STATE, state);

    if ( !from_user.len || !to_user.len || !callid.len || !state.len) {
      LM_ERR("Wrong formated json %s\n", req);
      goto error;
    }


    if (!strcmp(direction.s, "inbound"))
        body = dbk_phtable_update(&from_user, &to_user, &callid, &fromtag, &totag, &state, 1);
    else
    	body = dbk_phtable_update(&to_user, &from_user, &callid, &totag, &fromtag, &state, 0);


	if ( body == NULL) {
			LM_ERR("Failed to construct BLF XML doc\n");
			goto error;
		}

    LM_INFO("\n\n XML ==>> %.*s\n\n", body->len, body->s);
    publ_info_t publ1;
    memset(&publ1, 0, sizeof(publ_info_t));
    if (!strcmp(direction.s, "inbound"))
        publ1.pres_uri= &from_user;
    else
        publ1.pres_uri= &to_user;

   	publ1.body= body;
    publ1.event= DIALOG_EVENT;
    publ1.expires = 3600;
    ret = pua_api.send_publish(&publ1);
    dbk_free_xml(body);



    /* OLD WAY */
    /*
    if (!strcmp(direction.s, "inbound")) {
      LM_DBG("channel is the initiator\n");
      ret = dbk_pres_update_and_notify(&from_user, &to_user, &callid, &fromtag, &totag, &state, 1);
    } else {
      LM_DBG("channel is the recipient\n");
      ret = dbk_pres_update_and_notify(&to_user, &from_user, &callid, &totag, &fromtag, &state, 0);
    }
    */

    LM_INFO("Received update: %.*s/%.*s %.*s %.*s"
            ,from_user.len, from_user.s
            ,to_user.len, to_user.s
            ,callid.len, callid.s
            ,state.len, state.s);

    if (ret < 0) {
      LM_ERR("Failed to process dialoginfo update command\n");
      ret = -1;
    }

   return ret;

error:
    return -1;

}


int rmqp_pres_update_handle(char* req)
{
  str event_name={0, 0}, event_package = {0, 0};
  struct json_object *json_obj;
  int ret = 0;

  /* extract info from json and construct xml */
  json_obj = json_tokener_parse(req);
  if (is_error(json_obj)) {
    LM_ERR("Error parsing json: %s\n", json_tokener_errors[-(unsigned long)json_obj]);
    LM_ERR("%s\n", req);
    goto error;
  }

  json_extract_field(BLF_JSON_EVENT_NAME, event_name);

  if ( event_name.len == 5 && strncmp(event_name.s, "flush", 5) == 0 ) {
	  ret = rmqp_pres_flush_handle(json_obj);
  } else if ( event_name.len == 6 && strncmp(event_name.s, "update", 6) == 0 ) {
	  json_extract_field(BLF_JSON_EVENT_PKG, event_package);
	  if ( event_package.len == str_event_dialog.len
			  && strncmp(event_package.s, str_event_dialog.s, event_package.len) == 0 ) {
		  ret = blf_pres_update_handler(req, json_obj);
	  }
      else if ( event_package.len == str_event_message_summary.len
    		  && strncmp(event_package.s, str_event_message_summary.s, event_package.len) == 0 ) {

		  ret = mwi_pres_update_handler(req, json_obj);
	      }
  }

  json_object_put(json_obj);
  return ret;
 error:
  return -1;
}


int dbk_presence_query_expired(db1_res_t** _r)
{
  /* TODO delete expired record */

  db1_res_t* db_res = db_new_result();
  if ( db_res == NULL ) {
    LM_ERR("no memory left\n");
    return -1;
  }
  RES_ROW_N(db_res) = 0;

  *_r = db_res;
  return 0;
}


int dbk_presence_query(const db1_con_t* _h, const db_key_t* _k,
                       const db_val_t* _v, const db_key_t* _c, int _n, int _nc, db1_res_t** _r)
{
  str username = {0, 0};
  str domain = {0, 0};
  int i;
  int uri_size;
  unsigned int hash_code;
  str pres_uri;
  dbk_pres_user_t* pu;
  dbk_pres_dialog_t* pd;
  db1_res_t* db_res = NULL;
  int col;
  str body[BLF_MAX_DIALOGS];
  int row_cnt = 0;

  if (_n == 2 && _k[0]->len == str_expires_col.len &&
      strncmp(_k[0]->s, str_expires_col.s, str_expires_col.len) == 0 ) {
    LM_DBG("dbk_presence_query goes to dbk_presence_query_expired\n");
    return dbk_presence_query_expired(_r);
  }

  for ( i = 0; i< _n; i++ ) {
    if (username.len && domain.len)
      break;
    if (_k[i]->len == str_username_col.len &&
        strncmp(_k[i]->s, str_username_col.s, str_username_col.len) == 0) {
      username = _v[i].val.str_val;
    } else
      if (_k[i]->len == str_domain_col.len &&
          strncmp(_k[i]->s, str_domain_col.s, str_domain_col.len) == 0) {
        domain = _v[i].val.str_val;
      }
  }
  if (!username.len || !domain.len) {
    LM_ERR("Unsupported query - expected a query after username and domain\n");
    return -1;
  }

  uri_size = username.len + domain.len + 4;
  if ( pres_uri_size < uri_size ) {
    pres_uri_size = uri_size*2;
    pres_uri_buf = (char*)pkg_realloc(pres_uri_buf, pres_uri_size);
    if (pres_uri_buf == NULL) {
      LM_ERR("No more private memory\n");
      return 0;
    }
  }
  pres_uri.s = pres_uri_buf;
  pres_uri.len = sprintf(pres_uri_buf, "sip:%.*s@%.*s", username.len, username.s, domain.len, domain.s);

  LM_DBG("dbk presence query %s\n", pres_uri.s);

  db_res = db_new_result();
  if ( db_res == NULL ) {
    LM_ERR("no memory left\n");
    return -1;
  }
  RES_ROW_N(db_res) = 0;

  memset(body, 0, BLF_MAX_DIALOGS*sizeof(str));

  /* search in hash_table*/
  hash_code = core_hash(&pres_uri, NULL, dbk_phtable_size);

  lock_get(&dbk_phtable[hash_code].lock);
  pu = dbk_pres_search_pu(hash_code, &pres_uri, 0, 0);

  if ( pu == NULL ) {
    LM_INFO("No dialog info found for user [%.*s]\n", pres_uri.len, pres_uri.s);
    lock_release(&dbk_phtable[hash_code].lock);
    *_r = db_res;
    return 0;
  }
  LM_DBG("Found presence user record\n");
  if ( pu->pd == NULL ) {
    LM_ERR("Critical bad data in phtable: pu->pd == NULL\n");
    goto error1;
  }
  pd = pu->pd;
  row_cnt = 0;
  while ( pd != NULL && row_cnt < BLF_MAX_DIALOGS ) {
    body[row_cnt].s = (char*)pkg_malloc(pu->pd->body.len + 1);
    if (body[row_cnt].s == NULL) {
      LM_ERR("No more shared memory\n");
      goto error1;
    }
    memcpy(body[row_cnt].s, pu->pd->body.s, pu->pd->body.len);
    body[row_cnt].len = pu->pd->body.len;
    body[row_cnt].s[body[row_cnt].len] = '\0';
    pd = pd->next;
    row_cnt++;
  }

  lock_release(&dbk_phtable[hash_code].lock);

  LM_INFO("The user %.*s has %d active dialogs\n", pres_uri.len, pres_uri.s, row_cnt);
  RES_COL_N(db_res) = _nc;
  RES_ROW_N(db_res) = row_cnt;
  if (db_allocate_rows(db_res) < 0) {
    LM_ERR("Could not allocate rows.\n");
    goto error2;
  }

  if (db_allocate_columns(db_res, RES_COL_N(db_res)) != 0) {
    LM_ERR("Could not allocate columns\n");
    goto error2;
  }
  for(col = 0; col < RES_COL_N(db_res); col++) {
    RES_NAMES(db_res)[col] = (str*)pkg_malloc(sizeof(str));
    if (! RES_NAMES(db_res)[col]) {
      LM_ERR("no private memory left\n");
      RES_COL_N(db_res) = col;
      db_free_columns(db_res);
      goto error2;
    }
    LM_DBG("Allocated %lu bytes for RES_NAMES[%d] at %p\n",
           (unsigned long)sizeof(str), col, RES_NAMES(db_res)[col]);

    RES_NAMES(db_res)[col]->s = _c[col]->s;
    RES_NAMES(db_res)[col]->len = _c[col]->len;
    RES_TYPES(db_res)[col] = DB1_STR;

    LM_DBG("RES_NAMES(%p)[%d]=[%.*s]\n", RES_NAMES(db_res)[col], col,
           RES_NAMES(db_res)[col]->len, RES_NAMES(db_res)[col]->s);
  }

  for (i = 0; i< row_cnt; i++ ) {
    if (db_allocate_row(db_res, &(RES_ROWS(db_res)[i])) != 0) {
      LM_ERR("Could not allocate row.\n");
      RES_ROW_N(db_res) = i;
      while(--i >= 0) {
        RES_ROWS(db_res)[i].values[col].free  = 0;
      }
      goto error2;
    }

    /* complete the row with the columns */
    for(col = 0; col< _nc; col++) {
      LM_DBG("Col[%d]: %.*s\n", col, _c[col]->len, _c[col]->s);
      RES_ROWS(db_res)[0].values[col].type = DB1_STR;
      if (strncmp(_c[col]->s, "body", _c[col]->len) == 0 ) {
        RES_ROWS(db_res)[i].values[col].val.str_val = body[i];
        RES_ROWS(db_res)[i].values[col].free  = 1;
        RES_ROWS(db_res)[i].values[col].nul  = 0;
        LM_INFO("Body in result: [%s]\n", RES_ROWS(db_res)[i].values[col].val.string_val);
      } else {
        RES_ROWS(db_res)[i].values[col].val.str_val.s = "";
        RES_ROWS(db_res)[i].values[col].val.str_val.len = 0;
        RES_ROWS(db_res)[i].values[col].free  = 0;
        RES_ROWS(db_res)[i].values[col].nul  = 1;
      }
    }
  }

  LM_DBG("Returned [%d] rows\n", row_cnt);
  *_r = db_res;
  return 0;

 error1:
  lock_release(&dbk_phtable[hash_code].lock);
 error2:
  for (i = 0; i< row_cnt; i++) {
    if (!body[i].s)
      break;
    pkg_free(body[i].s);
  }
  db_free_result(db_res);
  return -1;
}

int dbk_dialoginfo_update(const db1_con_t* _h, const db_key_t* db_col,
                          const db_val_t* db_val, const int _n)
{
  json_object *json_object = NULL;
  rmq_conn_t* rmq = (rmq_conn_t*)_h->tail;
  amqp_bytes_t amqp_mb;
  static amqp_bytes_t routing_key= {3, "BLF"};
  str from_user = db_val[0].val.str_val;
  str to_user   = db_val[1].val.str_val;
  str state     = db_val[2].val.str_val;
  str callid    = db_val[3].val.str_val;
  str from_tag  = db_val[4].val.str_val;
  str to_tag    = db_val[5].val.str_val;
  str unique_string;

  if (!rmq->conn && rmqp_open_connection(rmq) < 0) {
    LM_WARN("disconnected from AMQP, dropping presence update\n");
    goto error;
  }

  if (_n != 6) {
    LM_ERR("Inconsistency, expected 6 columns\n");
    goto error;
  }

  LM_INFO("Dialog info update %.*s/%.*s %.*s %.*s\n",
          from_user.len, from_user.s
          ,to_user.len, to_user.s
          ,callid.len, callid.s
          ,state.len, state.s);

  /* construct json */

  json_object = json_object_new_object();
  if ( is_error(json_object) ) {
    LM_ERR("Error constructing new json object: %s\n",
           json_tokener_errors[-(unsigned long)json_object]);
    goto error;
  }

  json_object_object_add(json_object, BLF_JSON_FROM, json_object_new_string_len(from_user.s, from_user.len));
  json_object_object_add(json_object, BLF_JSON_TO, json_object_new_string_len(to_user.s, to_user.len));
  json_object_object_add(json_object, BLF_JSON_STATE, json_object_new_string_len(state.s, state.len));
  json_object_object_add(json_object, BLF_JSON_CALLID, json_object_new_string_len(callid.s, callid.len));
  json_object_object_add(json_object, BLF_JSON_FROMTAG, json_object_new_string_len(from_tag.s, from_tag.len));
  json_object_object_add(json_object, BLF_JSON_TOTAG, json_object_new_string_len(to_tag.s, to_tag.len));
  json_object_object_add(json_object, BLF_JSON_EVENT_CATEGORY, json_object_new_string("presence"));
  json_object_object_add(json_object, BLF_JSON_EVENT_NAME, json_object_new_string("update"));
  tmb.generate_callid(&unique_string);
  json_object_object_add(json_object, BLF_JSON_MSG_ID, json_object_new_string_len(unique_string.s, unique_string.len));

  json_object_object_add(json_object, BLF_JSON_EVENT_PKG, json_object_new_string("dialog"));

  amqp_mb.bytes = (char*)json_object_to_json_string(json_object);
  if (amqp_mb.bytes == NULL) {
    LM_ERR("Failed to get json string\n");
    goto error;
  }
  amqp_mb.len   = strlen(amqp_mb.bytes);
  LM_DBG("AMQP: body: %s\n", (char*)amqp_mb.bytes);

  /* send to rabbitmq */

  LM_DBG("AMQP: exchange [%.*s]\n", (int)rmq->exchange.len, (char*)rmq->exchange.bytes);
  LM_DBG("AMQP: channel %d\n", rmq->channel);
  LM_DBG("AMQP: routing key [%s]\n", (char*)blf_queue.bytes);
  LM_DBG("AMQP: body: %s\n", (char*)amqp_mb.bytes);

  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(amqp_basic_properties_t));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");

  if (amqp_basic_publish(rmq->conn,
                         rmq->channel,
                         rmq->exchange,
                         routing_key,
                         0,
                         0,
                         &props,
                         amqp_mb) < 0) {
    LM_WARN("failed to publish presence update\n");
    rmq_close(rmq);
    goto error;
  }

  json_object_put(json_object);

  return 0;
 error:
  if ( json_object )
    json_object_put(json_object);
  return -1;
}

int dbk_mi_print_pu(struct mi_node* rpl, dbk_pres_user_t* pu, int hash_code)
{
  dbk_pres_dialog_t* pd;
  struct mi_node* node= NULL;
  struct mi_node* node1 = NULL;
  int len;
  char* p;
  int dlg_count = 0;

  node = add_mi_node_child(rpl, 0, "User", 4, pu->user.s, pu->user.len);
  if (node==0) {
    LM_ERR("Failed to add User node\n");
    goto error;
  }

  if (addf_mi_attr( node, 0, "hash", 4, "%u", hash_code) == 0) {
    LM_ERR("Failed to add hash attribute\n");
    goto error;
  }

  for (pd = pu->pd; pd; pd = pd->next, dlg_count++) {

    p= int2str(dlg_count, &len);
    if ( (node1= add_mi_node_child(node, MI_DUP_VALUE, "Dialog", 6, p, len)) == 0) {
      LM_ERR("Failed to add Dialog node\n");
      goto error;
    }

    if (add_mi_node_child(node1, MI_DUP_VALUE, "callid", 6,
                          pd->callid.s, pd->callid.len) == 0) {
      LM_ERR("Failed to add callid\n");
      goto error;
    }
    if (add_mi_node_child(node1, MI_DUP_VALUE, "local-tag", 9,
                          pd->localtag.s, pd->localtag.len) == 0) {
      LM_ERR("Failed to add from-tag\n");
      goto error;
    }
    if (add_mi_node_child(node1, MI_DUP_VALUE, "remote-tag", 10,
                          pd->remotetag.s, pd->remotetag.len) == 0) {
      LM_ERR("Failed to add to-tag\n");
      goto error;
    }
    if (add_mi_node_child(node1, MI_DUP_VALUE, "body", 4,
                          pd->body.s, pd->body.len) == 0) {
      LM_ERR("Failed to add body\n");
      goto error;
    }
  }

  return 0;
 error:
  return -1;

}

struct mi_root * mi_dbk_phtable_dump(struct mi_root *cmd_tree, void *param)
{
  int i;
  dbk_pres_user_t* pu;
  struct mi_root* rpl_tree= NULL;
  struct mi_node* rpl = NULL;

  rpl_tree = init_mi_tree( 200, MI_SSTR(MI_OK));
  if (rpl_tree==0)
    return 0;
  rpl = &rpl_tree->node;

  for (i = 0; i< dbk_phtable_size; i++) {
    lock_get(&dbk_phtable[i].lock);
    for (pu = dbk_phtable[i].pu; pu; pu = pu->next) {
      if (dbk_mi_print_pu(rpl, pu, i)!=0)
        goto error;
    }
    lock_release(&dbk_phtable[i].lock);
  }

  return rpl_tree;

 error:
  lock_release(&dbk_phtable[i].lock);
  LM_ERR("Failed to print pres htable\n");
  return 0;
}

int dbk_presence_subscribe_alert_kazoo(rmq_conn_t* rmq, str* user,unsigned int expires, str* from_user, str* event, str* contact, str* callid, str* from_tag, str* to_tag)
{
  static amqp_bytes_t exchange = {15, "dialoginfo_subs"};
  static amqp_bytes_t routing_key = {15, "dialoginfo_subs"};
  static amqp_bytes_t amqp_mb;
  json_object *json_object = NULL;
  str unique_string;

  if (!rmq->conn && rmqp_open_connection(rmq) < 0) {
    LM_WARN("disconnected from AMQP, dropping presence subsciption\n");
    goto error;
  }

  json_object = json_object_new_object();
  if ( is_error(json_object) ) {
    LM_ERR("Error constructing new json object: %s\n",
           json_tokener_errors[-(unsigned long)json_object]);
    goto error;
  }

  LM_INFO("Subscription %.*s/%.*s expires in %d\n"
          ,from_user->len, from_user->s
          ,user->len, user->s
          ,expires-(int)time(NULL));

  json_object_object_add(json_object, BLF_JSON_USER, json_object_new_string_len(user->s, user->len));
  json_object_object_add(json_object, BLF_JSON_FROM, json_object_new_string_len(from_user->s, from_user->len));
  json_object_object_add(json_object, BLF_JSON_QUEUE, json_object_new_string_len(blf_queue.bytes, blf_queue.len));
  json_object_object_add(json_object, BLF_JSON_EXPIRES, json_object_new_int(expires-(int)time(NULL)));
  json_object_object_add(json_object, BLF_JSON_APP_NAME, json_object_new_string(NAME));
  json_object_object_add(json_object, BLF_JSON_APP_VERSION, json_object_new_string(VERSION));
  json_object_object_add(json_object, BLF_JSON_NODE, json_object_new_string(node_name));
  json_object_object_add(json_object, BLF_JSON_SERVERID, json_object_new_string_len(blf_queue.bytes, blf_queue.len));
  json_object_object_add(json_object, BLF_JSON_EVENT_CATEGORY, json_object_new_string("presence"));
  json_object_object_add(json_object, BLF_JSON_EVENT_NAME, json_object_new_string("subscription"));
  tmb.generate_callid(&unique_string);
  json_object_object_add(json_object, BLF_JSON_MSG_ID, json_object_new_string_len(unique_string.s, unique_string.len));

  json_object_object_add(json_object, BLF_JSON_EVENT_PKG, json_object_new_string_len(event->s, event->len));
  json_object_object_add(json_object, BLF_JSON_CALLID, json_object_new_string_len(callid->s, callid->len));
  json_object_object_add(json_object, BLF_JSON_FROMTAG, json_object_new_string_len(from_tag->s, from_tag->len));
  json_object_object_add(json_object, BLF_JSON_TOTAG, json_object_new_string_len(to_tag->s, to_tag->len));
  json_object_object_add(json_object, BLF_JSON_CONTACT, json_object_new_string_len(contact->s, contact->len));

  amqp_mb.bytes = (char*)json_object_to_json_string(json_object);
  if (amqp_mb.bytes == NULL) {
    LM_ERR("Failed to get json string\n");
    goto error;
  }
  amqp_mb.len   = strlen(amqp_mb.bytes);
  LM_DBG("AMQP: body: %s\n", (char*)amqp_mb.bytes);

  /* send to rabbitmq */

  //LM_DBG("AMQP: exchange [%.*s]\n", (int)rmq->exchange.len, (char*)rmq->exchange.bytes);
  LM_DBG("AMQP: exchange [%.*s]\n", (int)exchange.len, (char*)exchange.bytes);
  LM_DBG("AMQP: channel %d\n", rmq->channel);
  LM_DBG("AMQP: routing key [%s]\n", (char*)routing_key.bytes);
  LM_DBG("AMQP: body: %s\n", (char*)amqp_mb.bytes);

  amqp_basic_properties_t props;
  memset(&props, 0, sizeof(amqp_basic_properties_t));
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
  props.content_type = amqp_cstring_bytes("application/json");

  if (amqp_basic_publish(rmq->conn,
                         rmq->channel,
                         exchange,
                         routing_key,
                         0,
                         0,
                         &props,
                         amqp_mb) < 0) {
    LM_WARN("failed to publish presence subsciption\n");
    rmq_close(rmq);
    goto error;
  }

  json_object_put(json_object);

  return 0;
 error:
  if ( json_object )
    json_object_put(json_object);
  return -1;
}

int dbk_presence_subscribe_new(const db1_con_t* _h, const db_key_t* db_col,
                               const db_val_t* db_val, const int _n)
{
  unsigned int expires = 0;
  str user= {0, 0};
  int i;
  struct cell* t;
  pv_value_t value;
  str contact = {0, 0}, callid = {0, 0};
  str event = str_init("presence");
  str from_tag = {0, 0}, to_tag = {0, 0};


	for (i = 0; i< _n; i++) {
		if (db_col[i]->len == str_presentity_uri_col.len &&
				strncmp(db_col[i]->s, str_presentity_uri_col.s, str_presentity_uri_col.len) == 0) {
			user = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_expires_col.len &&
				strncmp(db_col[i]->s, str_expires_col.s, str_expires_col.len) == 0) {
			expires = db_val[i].val.int_val;
		} else if (db_col[i]->len == str_contact_col.len &&
				strncmp(db_col[i]->s, str_contact_col.s, str_contact_col.len) == 0) {
			contact = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_event_col.len &&
				strncmp(db_col[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_callid_col.len &&
				strncmp(db_col[i]->s, str_callid_col.s, str_callid_col.len) == 0) {
			callid = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_from_tag_col.len &&
				strncmp(db_col[i]->s, str_from_tag_col.s, str_from_tag_col.len) == 0) {
			from_tag = db_val[i].val.str_val;
		} else if (db_col[i]->len == str_to_tag_col.len &&
				strncmp(db_col[i]->s, str_to_tag_col.s, str_to_tag_col.len) == 0) {
			to_tag = db_val[i].val.str_val;
		}

            if(db_val[i].type == DB1_STR)
               LM_INFO("subscribe field %s = %.*s ", db_col[i]->s, db_val[i].val.str_val.len, db_val[i].val.str_val.s);
            if(db_val[i].type == DB1_INT)
               LM_INFO("subscribe field %s = %i ", db_col[i]->s, db_val[i].val.int_val);
	}

  LM_DBG("i=%d expires=%u\n", i, expires);
//  if (i == _n) {
//    LM_ERR("Wrong formated sql insert\n");
//    return -1;
//  }

  /* save in hash table the presentity_uri for this callid */
  t = tmb.t_gett();
  if (t == NULL || t->uas.request == NULL) {
    LM_ERR("No tm transaction or no sip msg found\n");
    return -1;
  }
  value.flags = PV_VAL_STR;
  value.rs = user;
  if (pv_set_spec_value(t->uas.request, &sht_spec, 0, &value) < 0) {
    LM_ERR("Failed to add sht value\n");
  }
  LM_DBG("Stored $sht(dbk=>%.*s)=[%.*s]\n", t->callid.len,  t->callid.s, value.rs.len, value.rs.s);

  if(parse_contact(t->uas.request->contact) == 0)
    contact = ((contact_body_t*)t->uas.request->contact->parsed)->contacts->uri;

  if(parse_event(t->uas.request->event) == 0)
    event = ((event_t*)t->uas.request->event->parsed)->name;

  user = ((to_body_t*)t->uas.request->to->parsed)->uri;

  return dbk_presence_subscribe_alert_kazoo((rmq_conn_t*)_h->tail, &user,
			expires, &((to_body_t*)t->uas.request->from->parsed)->uri, &event, &contact, &callid, &from_tag, &to_tag);

}


int dbk_presence_subscribe_update(const db1_con_t* _h, const db_key_t* _k,
                                  const db_val_t* _v, const db_key_t* _uk, const db_val_t* _uv,
                                  const int _n, const int _un)
{

  str user= {0, 0}, contact = {0, 0};
  str callid = {0, 0}, from_tag = {0, 0}, to_tag = {0, 0};
  str event = str_init("presence");

  unsigned int expires = 0;
  int i;
  struct cell* t;
  pv_value_t value;
  value.rs.len = 0;

  if(_un == 4)
     return 0;

  for (i = 0; i< _n; i++) {
		if (_k[i]->len == str_presentity_uri_col.len &&
			strncmp(_k[i]->s, str_presentity_uri_col.s, str_presentity_uri_col.len) == 0) {
			user = _v[i].val.str_val;
		} else if (_k[i]->len == str_contact_col.len &&
				strncmp(_k[i]->s, str_contact_col.s, str_contact_col.len) == 0) {
			contact = _v[i].val.str_val;
		} else if (_k[i]->len == str_event_col.len &&
				strncmp(_k[i]->s, str_event_col.s, str_event_col.len) == 0) {
			event = _v[i].val.str_val;
		} else if (_k[i]->len == str_callid_col.len &&
				strncmp(_k[i]->s, str_callid_col.s, str_callid_col.len) == 0) {
			callid = _v[i].val.str_val;
		} else if (_k[i]->len == str_from_tag_col.len &&
				strncmp(_k[i]->s, str_from_tag_col.s, str_from_tag_col.len) == 0) {
			from_tag = _v[i].val.str_val;
		} else if (_k[i]->len == str_to_tag_col.len &&
				strncmp(_k[i]->s, str_to_tag_col.s, str_to_tag_col.len) == 0) {
			to_tag = _v[i].val.str_val;
		}

        if(_v[i].type == DB1_STR)
           LM_INFO("subscribe field %s = %.*s ", _k[i]->s, _v[i].val.str_val.len, _v[i].val.str_val.s);
        if(_v[i].type == DB1_INT)
           LM_INFO("subscribe field %s = %i ", _k[i]->s, _v[i].val.int_val);

  }

  for (i = 0; i< _un; i++) {
    if (_uk[i]->len == str_expires_col.len &&
        strncmp(_uk[i]->s, str_expires_col.s, str_expires_col.len) == 0) {
      expires = _uv[i].val.int_val;
	} else if (_uk[i]->len == str_from_tag_col.len &&
			strncmp(_uk[i]->s, str_from_tag_col.s, str_from_tag_col.len) == 0) {
		from_tag = _uv[i].val.str_val;
	} else if (_uk[i]->len == str_to_tag_col.len &&
			strncmp(_uk[i]->s, str_to_tag_col.s, str_to_tag_col.len) == 0) {
		to_tag = _uv[i].val.str_val;
    }

    if(_uv[i].type == DB1_STR)
       LM_INFO("subscribe field %s = %.*s ", _uk[i]->s, _uv[i].val.str_val.len, _uv[i].val.str_val.s);
    if(_uv[i].type == DB1_INT)
       LM_INFO("subscribe field %s = %i ", _uk[i]->s, _uv[i].val.int_val);

  }

//  if (i == _n) {
//    LM_DBG("Not an expires update\n");
//    return 0;
//  }

  /* get user from sht */
  t = tmb.t_gett();
  if (t == NULL || t->uas.request == NULL) {
    LM_ERR("No tm transaction or no sip msg found\n");
    return -1;
  }
  if (pv_get_spec_value(t->uas.request, &sht_spec, &value) < 0) {
    LM_ERR("Failed to get sht value\n");
    return -1;
  }

  /* set it again to reset expires */
  if (pv_set_spec_value(t->uas.request, &sht_spec, 0, &value) < 0) {
    LM_ERR("Failed to add sht value\n");
  }
  if (value.rs.len == 0) {
    LM_DBG("Failed to get the presentity uri from $sht, take it from request To header\n");
    value.rs = ((to_body_t*)t->uas.request->to->parsed)->uri;
    LM_DBG("Took presentity_uri from To header=[%.*s]\n", value.rs.len, value.rs.s);
  } else {
    LM_DBG("Found presentity_uri $sht(dbk=>%.*s)=[%.*s]\n", t->callid.len,  t->callid.s, value.rs.len, value.rs.s);
  }

  if(parse_contact(t->uas.request->contact) == 0)
    contact = ((contact_body_t*)t->uas.request->contact->parsed)->contacts->uri;

  if(parse_event(t->uas.request->event) == 0)
    event = ((event_t*)t->uas.request->event->parsed)->name;

  user = ((to_body_t*)t->uas.request->to->parsed)->uri;


  return dbk_presence_subscribe_alert_kazoo((rmq_conn_t*)_h->tail, &user,
			expires, &((to_body_t*)t->uas.request->from->parsed)->uri, &event, &contact, &callid, &from_tag, &to_tag);

}
