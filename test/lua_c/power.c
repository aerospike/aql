#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

static int go(lua_State * L) {
  int rtrn = lua_tonumber(L, -1);   /* Get the single number arg */
  lua_pushnumber(L,rtrn*rtrn);      /* square it and push the return */

  return 1;
}

static const struct luaL_reg golib [] = {
  {"go", go},
  {NULL, NULL}
};

int luaopen_power(lua_State * L) {
  luaL_openlib(L, "go", golib, 0);
  return 1;
}


