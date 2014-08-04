/*
 * kz_fixup.c
 *
 *  Created on: Aug 2, 2014
 *      Author: root
 */


#include "../../mod_fix.h"
#include "../../lvalue.h"

#include "kz_fixup.h"

int fixup_kz_json(void** param, int param_no)
{
  if (param_no == 1 || param_no == 2) {
		return fixup_spve_null(param, 1);
	}

	if (param_no == 3) {
		if (fixup_pvar_null(param, 1) != 0) {
		    LM_ERR("failed to fixup result pvar\n");
		    return -1;
		}
		if (((pv_spec_t *)(*param))->setf == NULL) {
		    LM_ERR("result pvar is not writeble\n");
		    return -1;
		}
		return 0;
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}

int fixup_kz_json_free(void** param, int param_no)
{
	if (param_no == 1 || param_no == 2) {
		LM_WARN("free function has not been defined for spve\n");
		return 0;
	}

	if (param_no == 3) {
		return fixup_free_pvar_null(param, 3);
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}


int fixup_kz_amqp_encode(void** param, int param_no)
{
  if (param_no == 1 ) {
		return fixup_spve_null(param, 1);
	}

	if (param_no == 2) {
		if (fixup_pvar_null(param, 1) != 0) {
		    LM_ERR("failed to fixup result pvar\n");
		    return -1;
		}
		if (((pv_spec_t *)(*param))->setf == NULL) {
		    LM_ERR("result pvar is not writeble\n");
		    return -1;
		}
		return 0;
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}

int fixup_kz_amqp_encode_free(void** param, int param_no)
{
	if (param_no == 1 ) {
		LM_WARN("free function has not been defined for spve\n");
		return 0;
	}

	if (param_no == 2) {
		return fixup_free_pvar_null(param, 2);
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}

typedef struct _xl_msg
{
	pv_elem_t *m;
	struct action *a;
} xl_msg_t;


int kz_ampq_fixup_helper(void** param, int param_no)
{
	xl_msg_t *xm;
	str s;

	xm = (xl_msg_t*)pkg_malloc(sizeof(xl_msg_t));
	if(xm==NULL)
	{
		LM_ERR("no more pkg\n");
		return -1;
	}
	memset(xm, 0, sizeof(xl_msg_t));
	s.s = (char*)(*param); s.len = strlen(s.s);

	if(pv_parse_format(&s, &xm->m)<0)
	{
		LM_ERR("wrong format[%s]\n", (char*)(*param));
		return E_UNSPEC;
	}
	*param = (void*)xm;
	return 0;
}

int fixup_kz_amqp(void** param, int param_no)
{
  if (param_no == 1 || param_no == 2 || param_no == 3) {
		return fixup_spve_null(param, 1);
	}

	if (param_no == 4) {
		if (fixup_pvar_null(param, 1) != 0) {
		    LM_ERR("failed to fixup result pvar\n");
		    return -1;
		}
		if (((pv_spec_t *)(*param))->setf == NULL) {
		    LM_ERR("result pvar is not writeble\n");
		    return -1;
		}
		return 0;
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}

int fixup_kz_amqp_free(void** param, int param_no)
{
	if (param_no == 1 || param_no == 2 || param_no == 3) {
		return 0;
	}

	if (param_no == 4) {
		return fixup_free_pvar_null(param, 4);
	}

	LM_ERR("invalid parameter number <%d>\n", param_no);
	return -1;
}




