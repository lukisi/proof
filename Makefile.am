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

TESTS = \
	test_ipv4compute \
	test_commander \
	$(NULL)

check_PROGRAMS = \
	test_ipv4compute \
	test_commander \
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
	ipv4_compute.vala \
	naddr.vala \
	network_stack.vala \
	commander.vala \
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

test_ipv4compute_SOURCES = \
	testsuites/ipv4compute/tester.vala \
	testsuites/ipv4compute/ipv4_compute.vala \
	$(NULL)

test_ipv4compute_VALAFLAGS = \
	--pkg gee-0.8 \
	$(NULL)

test_ipv4compute_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(NULL)

test_ipv4compute_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(NULL)

test_commander_SOURCES = \
	testsuites/commander/tester.vala \
	testsuites/commander/commander.vala \
	$(NULL)

test_commander_VALAFLAGS = \
	--pkg gee-0.8 \
	--pkg tasklet-system \
	--pkg pth-tasklet \
	$(NULL)

test_commander_CPPFLAGS = \
	-w \
	$(GOBJECT_CFLAGS) \
	$(GEE_CFLAGS) \
	$(TASKLETSYSTEM_CFLAGS) \
	$(PTHTASKLET_CFLAGS) \
	$(NULL)

test_commander_LDADD = \
	$(GOBJECT_LIBS) \
	$(GEE_LIBS) \
	$(TASKLETSYSTEM_LIBS) \
	$(PTHTASKLET_LIBS) \
	$(NULL)

