NULL = 
ACLOCAL_AMFLAGS = -I m4

SUBDIRS = \
	neighborhood \
	identities \
	qspn \
	$(NULL)

noinst_LTLIBRARIES = \
	libmacgetter.la \
	$(NULL)

noinst_PROGRAMS =  \
	qspnclient \
	$(NULL)

libmacgetter_la_SOURCES = \
	mac-getter/hw.c \
	mac-getter/hw.h \
	$(NULL)

libmacgetter_la_CPPFLAGS = \
	$(NULL)

libmacgetter_la_LIBADD = \
	$(NULL)

qspnclient_SOURCES =  \
	proof.vala \
	naddr.vala \
	network_stack.vala \
	serialize_helpers.vala \
	mac-getter/hw.vapi \
	identities/identities.vapi \
	neighborhood/neighborhood.vapi \
	qspn/qspn.vapi \
	$(NULL)

qspnclient_VALAFLAGS = \
	--pkg gee-0.8 \
	--pkg ntkd-common \
	--pkg tasklet-system \
	--pkg pth-tasklet \
	--pkg ntkdrpc \
	$(NULL)

qspnclient_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(NTKDCOMMON_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(PTHTASKLET_CFLAGS) \
	$(NTKDRPC_CFLAGS) \
	-I$(srcdir)/identities \
	-I$(srcdir)/neighborhood \
	-I$(srcdir)/qspn \
	$(NULL)

qspnclient_LDADD = \
	-lm \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(NTKDCOMMON_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(PTHTASKLET_LIBS) \
	$(NTKDRPC_LIBS) \
	$(builddir)/libmacgetter.la \
	$(builddir)/neighborhood/libneighborhood.la \
	$(builddir)/identities/libidentities.la \
	$(builddir)/qspn/libqspn.la \
	$(NULL)

