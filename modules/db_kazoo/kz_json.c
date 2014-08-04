/**
 * $Id$
 *
 * Copyright (C) 2011 Flowroute LLC (flowroute.com)
 *
 * This file is part of Kamailio, a free SIP server.
 *
 * This file is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#include <stdio.h>
#include <string.h>
#include <json/json.h>

#include "../../mod_fix.h"
#include "../../lvalue.h"

#include "kz_json.h"


char** str_split(char* a_str, const char a_delim)
{
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}


int kz_json_get_field(struct sip_msg* msg, char* json, char* field, char* dst)
{
  str json_s;
  str field_s;
  pv_spec_t *dst_pv;
  pv_value_t dst_val;
  char** tokens;
  char* dup;
  char f1[25], f2[25], f3[25];
  int i;

	if (fixup_get_svalue(msg, (gparam_p)json, &json_s) != 0) {
		LM_ERR("cannot get json string value\n");
		return -1;
	}

	if (fixup_get_svalue(msg, (gparam_p)field, &field_s) != 0) {
		LM_ERR("cannot get field string value\n");
		return -1;
	}
	
	dst_pv = (pv_spec_t *)dst;
	
	struct json_object *j = json_tokener_parse(json_s.s);

	if (is_error(j)) {
		LM_ERR("empty or invalid JSON\n");
		return -1;
	}

	struct json_object *jtree = NULL;

	dup = strdup(field_s.s);
    tokens = str_split(dup, '.');
    free(dup);

    if (tokens)
    {
    	jtree = j;
        for (i = 0; *(tokens + i); i++)
        {
        	if(jtree != NULL) {
				str field = str_init(*(tokens + i));
				// check for idx []
				int sresult = sscanf(field.s, "%[^[][%[^]]]", f1, f2, f3);
				LM_DBG("CHECK IDX %d - %s , %s, %s\n", sresult, field.s, f1, f2);

				jtree = json_object_object_get(jtree, f1);
				if(jtree != NULL) {
					char *value = (char*)json_object_get_string(jtree);
					LM_DBG("JTREE OK %s\n", value);
				}
				if(jtree != NULL && sresult > 1 && json_object_is_type(jtree, json_type_array)) {
					int idx = atoi(f2);
					jtree = json_object_array_get_idx(jtree, idx);
					if(jtree != NULL) {
						char *value = (char*)json_object_get_string(jtree);
						LM_DBG("JTREE IDX OK %s\n", value);
					}
				}
        	}
            free(*(tokens + i));
        }
        free(tokens);
    }

	if(jtree != NULL) {
		char *value = (char*)json_object_get_string(jtree);
		dst_val.rs.s = value;
		dst_val.rs.len = strlen(value);
		dst_val.flags = PV_VAL_STR;
	} else {
		dst_val.flags = PV_VAL_NULL;
	}

	dst_pv->setf(msg, &dst_pv->pvp, (int)EQ_T, &dst_val);

	json_object_put(j);



	return 1;
}
