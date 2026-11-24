.PHONY: all install clean

all:
	cd /home/cliondev/code/openresty12532/build/LuaJIT-2.1-20231117.1 && $(MAKE) TARGET_STRIP=@: CCDEBUG=-g Q= XCFLAGS='-DLUAJIT_ENABLE_LUA52COMPAT -DLUA_USE_APICHECK -DLUA_USE_ASSERT' CC=cc PREFIX=/usr/local/openresty/luajit
	cd /home/cliondev/code/openresty12532/build/lua-cjson-2.1.0.13 && $(MAKE) DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_CMODULE_DIR=/usr/local/openresty/lualib LUA_MODULE_DIR=/usr/local/openresty/lualib CJSON_CFLAGS="-g -O -fpic" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-resty-signal-0.03 && $(MAKE) DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall -fpic" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-redis-parser-0.13 && $(MAKE) DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-rds-parser-0.06 && $(MAKE) DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall" CC=cc
	cd /home/cliondev/code/openresty12532/build/nginx-1.25.3 && $(MAKE)

install: all
	mkdir -p $(DESTDIR)/usr/local/openresty/
	-cp /home/cliondev/code/openresty12532/COPYRIGHT $(DESTDIR)/usr/local/openresty/
	cd /home/cliondev/code/openresty12532/build/LuaJIT-2.1-20231117.1 && $(MAKE) install TARGET_STRIP=@: CCDEBUG=-g Q= XCFLAGS='-DLUAJIT_ENABLE_LUA52COMPAT -DLUA_USE_APICHECK -DLUA_USE_ASSERT' CC=cc PREFIX=/usr/local/openresty/luajit DESTDIR=$(DESTDIR)
	cd /home/cliondev/code/openresty12532/build/lua-cjson-2.1.0.13 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_CMODULE_DIR=/usr/local/openresty/lualib LUA_MODULE_DIR=/usr/local/openresty/lualib CJSON_CFLAGS="-g -O -fpic" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-resty-signal-0.03 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall -fpic" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-redis-parser-0.13 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-rds-parser-0.06 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_INCLUDE_DIR=/home/cliondev/code/openresty12532/build/luajit-root/usr/local/openresty/luajit/include/luajit-2.1 LUA_LIB_DIR=/usr/local/openresty/lualib CFLAGS="-g -O -Wall" CC=cc
	cd /home/cliondev/code/openresty12532/build/lua-resty-dns-0.23 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-memcached-0.17 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-redis-0.30 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-mysql-0.27 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-string-0.15 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-upload-0.11 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-websocket-0.11 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-lock-0.09 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-lrucache-0.13 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-core-0.1.28 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-upstream-healthcheck-0.08 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-limit-traffic-0.09 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-resty-shell-0.03 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/lua-tablepool-0.03 && $(MAKE) install DESTDIR=$(DESTDIR) LUA_LIB_DIR=/usr/local/openresty/lualib INSTALL=/home/cliondev/code/openresty12532/build/install
	cd /home/cliondev/code/openresty12532/build/opm-0.0.8 && /home/cliondev/code/openresty12532/build/install bin/* $(DESTDIR)/usr/local/openresty/bin/
	cd /home/cliondev/code/openresty12532/build/resty-cli-0.30 && /home/cliondev/code/openresty12532/build/install bin/* $(DESTDIR)/usr/local/openresty/bin/
	cp /home/cliondev/code/openresty12532/build/resty.index $(DESTDIR)/usr/local/openresty/
	cp -r /home/cliondev/code/openresty12532/build/pod $(DESTDIR)/usr/local/openresty/
	cd /home/cliondev/code/openresty12532/build/nginx-1.25.3 && $(MAKE) install DESTDIR=$(DESTDIR)
	mkdir -p $(DESTDIR)/usr/local/openresty/site/lualib $(DESTDIR)/usr/local/openresty/site/pod $(DESTDIR)/usr/local/openresty/site/manifest
	ln -sf /usr/local/openresty/nginx/sbin/nginx $(DESTDIR)/usr/local/openresty/bin/openresty

clean:
	rm -rf build *.exe *.dll openresty-*
