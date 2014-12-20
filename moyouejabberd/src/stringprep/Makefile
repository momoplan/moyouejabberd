# $Id$

CC = gcc
CFLAGS = -g -O2 -Wall 
CPPFLAGS = -I/usr/local/ssl/include -g -O2 -Wall 
LDFLAGS = 
LIBS = 

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
EFLAGS += -I .. +$(Transform)
EFLAGS += -pz ..

# make debug=true to compile Erlang module with debug informations.
ifdef debug
	EFLAGS+=+debug_info +export_all
endif

ERLSHLIBS = ../stringprep_drv.so
OUTDIR = ..
SOURCES = $(wildcard *.erl)
BEAMS = $(addprefix $(OUTDIR)/,$(SOURCES:.erl=.beam))


all:    $(BEAMS) $(ERLSHLIBS)

$(OUTDIR)/%.beam:       %.erl
	/usr/bin/erlc -W $(EFLAGS) -o $(OUTDIR) $<

#all:	$(ERLSHLIBS)
#	erl -s make all report "{outdir, \"..\"}" -noinput -s erlang halt

$(ERLSHLIBS):	../%.so:	%.c uni_data.c uni_norm.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(INCLUDES) \
		$(subst ../,,$(subst .so,.c,$@)) $(LIBS) \
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

