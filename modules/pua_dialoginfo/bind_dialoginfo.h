
#ifndef _BIND_DIALOGINFO_H_
#define _BIND_DIALOGINFO_H_

#include "../../sr_module.h"

typedef str* (*build_dialoginfo_t)(char*, str*, str*, str*, 
	unsigned int, str*, str*, str*, str*, int);

typedef struct {
	build_dialoginfo_t build_dialoginfo;
} pua_dialoginfo_api_t;

int bind_pua_dialoginfo(pua_dialoginfo_api_t* api);

typedef int (*bind_pua_dialoginfo_t)(pua_dialoginfo_api_t* api);

inline static int pua_dialoginfo_load_api(pua_dialoginfo_api_t* api)
{
	bind_pua_dialoginfo_t bind_pua_dialoginfo_exports;
	if (!(bind_pua_dialoginfo_exports = (bind_pua_dialoginfo_t)find_export("bind_pua_dialoginfo", 1, 0)))
	{
		LM_ERR("Failed to import bind_pua_dialoginfo\n");
		return -1;
	}
	return bind_pua_dialoginfo_exports(api);
}

#endif
