# $Id$

CC = gcc
CFLAGS = -g -O2 -Wall 
CPPFLAGS = -I/usr/local/ssl/include -g -O2 -Wall 
LDFLAGS = 
LIBS = 

SSL_CFLAGS = -DHAVE_SSL
SSL_LIBS = -L/usr/local/ssl/lib -lssl -lcrypto

ERLANG_CFLAGS = -I/usr/lib64/erlang/lib/erl_interface-3.7.1/include -I/usr/lib64/erlang/usr/include
ERLANG_LIBS = -L/usr/lib64/erlang/lib/erl_interface-3.7.1/lib -lerl_interface -lei

# Assume Linux-style dynamic library flags
DYNAMIC_LIB_CFLAGS = -fpic -shared
ifeq ($(shell uname),Darwin)
    DYNAMIC_LIB_CFLAGS = -fPIC -bundle -flat_namespace -undefined suppress
endif
ifeq ($(shell uname),SunOs)
    DYNAMIC_LIB_CFLAGS = -KPIC -G -z text
endif

Transform = '{parse_transform, lager_transform}'
EFLAGS += -DSSL40
EFLAGS += -I .. +$(Transform)
EFLAGS += -pz ..

# make debug=true to compile Erlang module with debug informations.
ifdef debug
	EFLAGS+=+debug_info +export_all
endif

ifeq (true, true)
  EFLAGS+=-DHAVE_MD2
  ERLANG_CFLAGS += -DHAVE_MD2
endif

ERLSHLIBS = ../tls_drv.so ../sha_drv.so
OUTDIR = ..
SOURCES = $(wildcard *.erl)
BEAMS = $(addprefix $(OUTDIR)/,$(SOURCES:.erl=.beam))


all:    $(BEAMS) $(ERLSHLIBS)

$(OUTDIR)/%.beam:       %.erl
	/usr/bin/erlc -W $(EFLAGS) -o $(OUTDIR) $<

#all:	$(ERLSHLIBS)
#	erl -s make all report "{outdir, \"..\"}" -noinput -s erlang halt

$(ERLSHLIBS):	../%.so:	%.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) \
		$(subst ../,,$(subst .so,.c,$@)) \
		$(LIBS) \
		$(SSL_LIBS) \
		$(SSL_CFLAGS) \
		$(ERLANG_LIBS) \
		$(ERLANG_CFLAGS) \
		-o $@ \
		$(DYNAMIC_LIB_CFLAGS)

clean:
	rm -f $(BEAMS) $(ERLSHLIBS)

distclean: clean
	rm -f Makefile

TAGS:
	etags *.erl
