/*
 * kz_json.h
 *
 *  Created on: Aug 2, 2014
 *      Author: root
 */

#ifndef KZ_JSON_H_
#define KZ_JSON_H_

#include "../../parser/msg_parser.h"


int kz_json_get_field(struct sip_msg* msg, char* json, char* field, char* dst);



#endif /* KZ_JSON_H_ */
