
#ifndef _BIND_PRES_DIALOGINFO_H_
#define _BIND_PRES_DIALOGINFO_H_

#include "../../sr_module.h"

typedef str* (*agg_dialoginfo_t)(str*, str*, str**, int);

typedef struct {
	agg_dialoginfo_t agg_dialoginfo;
} pres_dialoginfo_api_t;

int bind_pres_dialoginfo(pres_dialoginfo_api_t* api);

typedef int (*bind_pres_dialoginfo_t)(pres_dialoginfo_api_t* api);

inline static int pres_dialoginfo_load_api(pres_dialoginfo_api_t* api)
{
	bind_pres_dialoginfo_t bind_pres_dialoginfo_exports;
	if (!(bind_pres_dialoginfo_exports = (bind_pres_dialoginfo_t)find_export("bind_pres_dialoginfo", 1, 0)))
	{
		LM_ERR("Failed to import bind_pres_dialoginfo\n");
		return -1;
	}
	return bind_pres_dialoginfo_exports(api);
}

#endif
